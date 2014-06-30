#!/usr/bin/env python
# #
#
# Copyright 2012-2014 Ghent University
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# All rights reserved.
#
"""
Script to check for quota transgressions and notify the offending users.

- relies on mmrepquota to get a quick estimate of user quota
- checks all storage systems that are listed in /etc/quota_check.conf
- writes quota information in gzipped json files in the target directory for the
  affected entity (user, project, vo)
- mails a user, vo or project moderator (TODO)

@author Andy Georges
"""
import jsonpickle
import os
import pwd
import re
import sys
import time
import urllib2

from string import Template

from vsc.administration.user import VscUser
from vsc.administration.vo import VscVo
from vsc.config.base import VscStorage
from vsc.filesystem.gpfs import GpfsOperations
from vsc.filesystem.quota.entities import QuotaUser, QuotaFileset
from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.utils import LdapQuery
from vsc.utils import fancylogger
from vsc.utils.cache import FileCache
from vsc.utils.mail import VscMail
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.rest_oauth import make_api_request
from vsc.utils.script_tools import ExtendedSimpleOption

# Constants
NAGIOS_CHECK_INTERVAL_THRESHOLD = 60 * 60 # one hour

GPFS_GRACE_REGEX = re.compile(r"(?P<days>\d+)\s*days?|(?P<hours>\d+)\s*hours?|(?P<minutes>\d+)\s*minutes?|(?P<expired>expired)")

GPFS_NOGRACE_REGEX = re.compile(r"none", re.I)

# log setup
logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()

QUOTA_USERS_WARNING = 20
QUOTA_USERS_CRITICAL = 40
QUOTA_FILESETS_CRITICAL = 1

QUOTA_NOTIFICATION_CACHE_THRESHOLD = 7 * 86400

QUOTA_EXCEEDED_MAIL_TEXT_TEMPLATE = Template("""
Dear $user_name


We have noticed that you have exceeded your quota on the VSC storage,
more in particular: $storage_name

As you may know, this may have a significant impact on the jobs you
can run on the various clusters.

Please clean up any files you no longer require.

Should you need more storage, you can use your VO storage.
If you are not a member of a VO, please consider joining one or request
a VO to be created for your research group. If your VO storage is full,
please ask its moderator ask to increase the quota.

Also, it is recommended to clear scratch storage and move data you wish
to keep to $$VSC_DATA or $$VSC_DATA_VO/$USER. It is paramount that scratch
space remains temporary storage for running (multi-node) jobs as it is
accessible faster than both $$VSC_HOME and $$VSC_DATA.

At this point on $time, your personal usage is the following:
$quota_info


Kind regards,
The UGent HPC team
""")


VO_QUOTA_EXCEEDED_MAIL_TEXT_TEMPLATE = Template("""
Dear $user_name


We have noticed that the VO ($vo_name) you moderate has exceeded its quota on the VSC storage,
more in particular: $$$storage_name

As you may know, this may have a significant impact on the jobs the VO members
can run on the various clusters.

Please clean up any files that are no longer required.

Should you need more storage, you can reply to this mail and ask for
the quota to be increased. Please motivate your request adequately.

Also, it is recommended to have your VO members clear scratch storage and move data they wish
to keep to $$VSC_DATA or $$VSC_DATA_VO/$USER. It is paramount that scratch
space remains temporary storage for running (multi-node) jobs as it is
accessible faster than both $$VSC_HOME and $$VSC_DATA.

At this point on $time, the VO  usage is the following:
$quota_info


Kind regards,
The UGent HPC team
""")


def get_mmrepquota_maps(quota_map, storage, filesystem, filesets):
    """Obtain the quota information.

    This function uses vsc.filesystem.gpfs.GpfsOperations to obtain
    quota information for all filesystems known to the storage.

    The returned dictionaries contain all information on a per user
    and per fileset basis for the given filesystem. Users with multiple
    quota settings across different filesets are processed correctly.

    Returns { "USR": user dictionary, "FILESET": fileset dictionary}.
    """
    user_map = {}
    fs_map = {}

    timestamp = int(time.time())

    logger.info("ordering USR quota for storage %s" % (storage))
    # Iterate over a list of named tuples -- GpfsQuota
    for (user, gpfs_quota) in quota_map['USR'].items():
        user_quota = user_map.get(user, QuotaUser(storage, filesystem, user))
        user_map[user] = _update_quota_entity(filesets,
                                              user_quota,
                                              filesystem,
                                              gpfs_quota,
                                              timestamp)

    logger.info("ordering FILESET quota for storage %s" % (storage))
    # Iterate over a list of named tuples -- GpfsQuota
    for (fileset, gpfs_quota) in quota_map['FILESET'].items():
        fileset_quota = fs_map.get(fileset, QuotaFileset(storage, filesystem, fileset))
        fs_map[fileset] = _update_quota_entity(filesets,
                                               fileset_quota,
                                               filesystem,
                                               gpfs_quota,
                                               timestamp)

    return {"USR": user_map, "FILESET": fs_map}


def _update_quota_entity(filesets, entity, filesystem, gpfs_quotas, timestamp):
    """
    Update the quota information for an entity (user or fileset).

    @type filesets: string
    @type entity: QuotaEntity instance
    @type filesystem: string
    @type gpfs_quota: list of GpfsQuota namedtuple instances
    @type timestamp: a timestamp, duh. an integer
    """

    for quota in gpfs_quotas:
        logger.debug("gpfs_quota = %s" % (str(quota)))
        grace = GPFS_GRACE_REGEX.search(quota.blockGrace)
        nograce = GPFS_NOGRACE_REGEX.search(quota.blockGrace)

        if nograce:
            expired = (False, None)
        elif grace:
            grace = grace.groupdict()
            if grace.get('days', None):
                expired = (True, int(grace['days']) * 86400)
            elif grace.get('hours', None):
                expired = (True, int(grace['hours']) * 3600)
            elif grace.get('minutes', None):
                expired = (True, int(grace['minutes']) * 60)
            elif grace.get('expired', None):
                expired = (True, 0)
            else:
                logger.raiseException("Unprocessed grace groupdict %s (from string %s)." %
                                        (grace, quota.blockGrace))
        else:
            logger.raiseException("Unknown grace string %s." % quota.blockGrace)

        if quota.filesetname:
            fileset_name = filesets[filesystem][quota.filesetname]['filesetName']
        else:
            fileset_name = None
        logger.debug("The fileset name is %s (filesystem %s); blockgrace %s to expired %s" %
                     (fileset_name, filesystem, quota.blockGrace, expired))
        entity.update(fileset_name,
                      int(quota.blockUsage),
                      int(quota.blockQuota),
                      int(quota.blockLimit),
                      int(quota.blockInDoubt),
                      expired,
                      timestamp)

    return entity


def process_fileset_quota(storage, gpfs, storage_name, filesystem, quota_map, opener, url, access_token, dry_run=False):
    """Store the quota information in the filesets.
    """

    filesets = gpfs.list_filesets()
    exceeding_filesets = []

    log_vo_quota_to_django(storage_name, quota_map, opener, url, access_token, dry_run)

    logger.info("filesets = %s" % (filesets))

    payload = []
    for (fileset, quota) in quota_map.items():
        fileset_name = filesets[filesystem][fileset]['filesetName']
        logger.debug("Fileset %s quota: %s" % (fileset_name, quota))

        path = filesets[filesystem][fileset]['path']
        filename = os.path.join(path, ".quota_fileset.json.gz")
        path_stat = os.stat(path)

        if dry_run:
            logger.info("Dry run: would update cache for %s at %s with %s" % (storage_name, path, "%s" % (quota,)))
            logger.info("Dry run: would chmod 640 %s" % (filename,))
            logger.info("Dry run: would chown %s to %s %s" % (filename, path_stat.st_uid, path_stat.st_gid))
        else:
            # TODO: This should somehow be some atomic operation.
            cache = FileCache(filename, False)
            cache.update(key="quota", data=quota, threshold=0)
            cache.update(key="storage_name", data=storage_name, threshold=0)
            cache.close()

            gpfs.chmod(0640, filename)
            gpfs.chown(path_stat.st_uid, path_stat.st_gid, filename)

        logger.info("Stored fileset %s [%s] quota for storage %s at %s" % (fileset, fileset_name, storage, filename))

        if quota.exceeds():
            exceeding_filesets.append((fileset_name, quota))

    return exceeding_filesets


def log_user_quota_to_django(user_map, storage_name, quota_map, opener, url, access_token, dry_run=False):
    """
    Upload the quota information to the django database, so it can be displayed for the users in the web application.
    """

    payload = []
    count = 0

    for (user_id, quota) in quota_map.items():

        user_name = user_map.get(int(user_id), None)
        if not user_name or not user_name.startswith('vsc4'):
            continue

        for (fileset, quota_) in quota.quota_map.items():

            params = {
                "fileset": fileset,
                "user": user_name,
                "used": quota_.used,
                "soft": quota_.soft,
                "hard": quota_.hard,
                "doubt" : quota_.doubt,
                "expired": quota_.expired[0],
                "remaining": quota_.expired[1] or 0,  # seconds
            }
            payload.append(params)
            count += 1

            if count > 100:
                log_quota_to_django(storage_name, "user", opener, url, payload, access_token, dry_run)
                count = 0
                payload = []

    if count:
        log_quota_to_django(storage_name, "user", opener, url, payload, access_token, dry_run)


def log_vo_quota_to_django(storage_name, quota_map, opener, url, payload, access_token, dry_run=False):
    pass

def log_quota_to_django(storage_name, kind, opener, url, payload, access_token, dry_run=False):

    payload = jsonpickle.encode(payload)

    if dry_run:
        logger.info("Would push payload to account web app: %s" % (payload,))
    else:
        try:
            path = "%s/api/usage/storage/%s/%s/size/" % (url, storage_name, kind)
            # result = make_api_request(opener, path, "PUT", payload, access_token)
        except Exception:
            logger.raiseException("Could not store quota info in account web app")


def sanitize_quota_information(fileset_name, quota):
    """Sanitize the information that is store at the user's side.

    There should be _no_ information regarding filesets besides:
        - vsc4xy
        - gvo*
    """
    for (fileset, value) in quota.quota_map.items():
        if not fileset.startswith('vsc4') and not fileset.startswith('gvo') and not fileset.startswith(fileset_name):
            quota.quota_map.pop(fileset)


def process_user_quota(storage, gpfs, storage_name, filesystem, quota_map, user_map, opener, url, access_token, dry_run=False):
    """Store the information in the user directories.
    """
    exceeding_users = []
    login_mount_point = storage[storage_name].login_mount_point
    gpfs_mount_point = storage[storage_name].gpfs_mount_point
    path_template = storage.path_templates[storage_name]

    # log_user_quota_to_django(user_map, storage_name, quota_map, opener, url, access_token, dry_run)

    for (user_id, quota) in quota_map.items():

        user_name = user_map.get(int(user_id), None)

        if user_name and user_name.startswith('vsc4'):
            user = VscUser(user_name)
            logger.debug("Checking quota for user %s with ID %s" % (user_name, user_id))
            logger.debug("User %s quota: %s" % (user, quota))

            path = user._get_path(storage_name)

            logger.debug("path for storing quota info would be %s" % (path,))

            # FIXME: We need some better way to address this
            # Right now, we replace the nfs mount prefix which the symlink points to
            # with the gpfs mount point. this is a workaround until we resolve the
            # symlink problem once we take new default scratch into production
            if gpfs.is_symlink(path):
                target = os.path.realpath(path)
                logger.debug("path is a symlink, target is %s" % (target,))
                logger.debug("login_mount_point for %s is %s" % (storage_name, login_mount_point))
                if target.startswith(login_mount_point):
                    new_path = target.replace(login_mount_point, gpfs_mount_point, 1)
                    logger.info("Found a symlinked path %s to the nfs mount point %s. Replaced with %s" %
                                (path, login_mount_point, gpfs_mount_point))
                else:
                    logger.warning("Unable to store quota information for %s on %s; symlink cannot be resolved properly"
                                   % (user_name, storage_name))
            else:
                new_path = path

            path_stat = os.stat(new_path)
            filename = os.path.join(new_path, ".quota_user.json.gz")

            sanitize_quota_information(path_template['user'][0], quota)

            if dry_run:
                logger.info("Dry run: would update cache for %s at %s with %s" % (storage_name, new_path, "%s" % (quota,)))
                logger.info("Dry run: would chmod 640 %s" % (filename,))
                logger.info("Dry run: would chown %s to %s %s" % (filename, path_stat.st_uid, path_stat.st_gid))
            else:
                cache = FileCache(filename, False)
                cache.update(key="quota", data=quota, threshold=0)
                cache.update(key="storage_name", data=storage_name, threshold=0)
                cache.close()

                gpfs.ignorerealpathmismatch = True
                gpfs.chmod(0640, filename)
                gpfs.chown(path_stat.st_uid, path_stat.st_uid, filename)
                gpfs.ignorerealpathmismatch = False

            logger.info("Stored user %s quota for storage %s at %s" % (user_name, storage_name, filename))

            if quota.exceeds():
                exceeding_users.append((user_name, quota))

    return exceeding_users


def notify(storage_name, item, quota, dry_run=False):
    """Send out the notification"""
    mail = VscMail(mail_host="smtp.ugent.be")
    if item.startswith("gvo"):  # VOs
        vo = VscVo(item)
        for user in [VscUser(m) for m in vo.moderator]:
            message = VO_QUOTA_EXCEEDED_MAIL_TEXT_TEMPLATE.safe_substitute(user_name=user.gecos,
                                                                           vo_name=item,
                                                                           storage_name=storage_name,
                                                                           quota_info="%s" % (quota,),
                                                                           time=time.ctime())
            if dry_run:
                logger.info("Dry-run, would send the following message: %s" % (message,))
            else:
                mail.sendTextMail(mail_to=user.mail,
                                  mail_from="hpc@ugent.be",
                                  reply_to="hpc@ugent.be",
                                  mail_subject="Quota on %s exceeded" % (storage_name,),
                                  message=message)
            logger.info("notification: recipient %s storage %s quota_string %s" %
                        (user.cn, storage_name, "%s" % (quota,)))

    elif item.startswith("gpr"):  # projects
        pass
    elif item.startswith("vsc"):  # users
        user = VscUser(item)

        exceeding_filesets = [fs for (fs, q) in quota.quota_map.items() if q.expired[0]]
        storage_names = []
        if [ef for ef in exceeding_filesets if not ef.startswith("gvo")]:
            storage_names.append(storage_name)
        if [ef for ef in exceeding_filesets if ef.startswith("gvo")]:
            storage_names.append(storage_name + "_VO")
        storage_names = ", ".join(["$" + sn for sn in storage_names])

        message = QUOTA_EXCEEDED_MAIL_TEXT_TEMPLATE.safe_substitute(user_name=user.gecos,
                                                                    storage_name=storage_names,
                                                                    quota_info="%s" % (quota,),
                                                                    time=time.ctime())
        if dry_run:
            logger.info("Dry-run, would send the following message: %s" % (message,))
        else:
            mail.sendTextMail(mail_to=user.mail,
                              mail_from="hpc@ugent.be",
                              reply_to="hpc@ugent.be",
                              mail_subject="Quota on %s exceeded" % (storage_name,),
                              message=message)
        logger.info("notification: recipient %s storage %s quota_string %s" %
                    (user.cn, storage_name, "%s" % (quota,)))
    else:
        logger.error("Should send a mail, but cannot process item %s" % (item,))


def notify_exceeding_items(gpfs, storage, filesystem, exceeding_items, target, dry_run=False):
    """Send out notification to the fileset owners.

    - if the fileset belongs to a VO: the VO moderator
    - if the fileset belongs to a project: the project moderator
    - if the fileset belongs to a user: the user

    The information is cached. The mail is sent in the following cases:
        - the excession is new
        - the excession occurred more than 7 days ago and stayed in the cache. In this case, the cache is updated as
          to avoid sending outdated mails repeatedly.
    """

    cache_path = os.path.join(gpfs.list_filesystems()[filesystem]['defaultMountPoint'], ".quota_%s_cache.json.gz" % (target))
    cache = FileCache(cache_path, True)  # we retain the old data

    logger.info("Processing %d exceeding items" % (len(exceeding_items)))

    for (item, quota) in exceeding_items:
        updated = cache.update(item, quota, QUOTA_NOTIFICATION_CACHE_THRESHOLD)
        logger.info("Storage %s: cache entry for %s was updated: %s" % (storage, item, updated))
        if updated:
            notify(storage, item, quota, dry_run)

    if not dry_run:
        cache.close()
    else:
        logger.info("Dry run: not saving the updated cache")


def notify_exceeding_filesets(**kwargs):
    """Notification for filesets that have exceeded their quota."""

    kwargs['target'] = 'filesets'
    notify_exceeding_items(**kwargs)


def notify_exceeding_users(**kwargs):
    """Notification for users who have exceeded their quota."""
    kwargs['target'] = 'users'
    notify_exceeding_items(**kwargs)


def map_uids_to_names():
    """Determine the mapping between user ids and user names."""
    ul = pwd.getpwall()
    d = {}
    for u in ul:
        d[u[2]] = u[0]
    return d


def main():
    """Main script"""

    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'storage': ('the VSC filesystems that are checked by this script', None, 'extend', []),
        'account_page_url': ('Base URL of the account page', None, 'store', 'https://account.vscentrum.be/django'),
        'access_token': ('OAuth2 token to access the account page REST API', None, 'store', None),
    }
    opts = ExtendedSimpleOption(options)

    try:
        opener = urllib2.build_opener(urllib2.HTTPHandler)
        access_token = opts.options.access_token

        user_id_map = map_uids_to_names()  # is this really necessary?
        LdapQuery(VscConfiguration())
        gpfs = GpfsOperations()
        storage = VscStorage()

        target_filesystems = [storage[s].filesystem for s in opts.options.storage]

        filesystems = gpfs.list_filesystems(target_filesystems).keys()
        logger.debug("Found the following GPFS filesystems: %s" % (filesystems))

        filesets = gpfs.list_filesets()
        logger.debug("Found the following GPFS filesets: %s" % (filesets))

        quota = gpfs.list_quota()
        exceeding_filesets = {}
        exceeding_users = {}
        stats = {}

        for storage_name in opts.options.storage:

            logger.info("Processing quota for storage_name %s" % (storage_name))
            filesystem = storage[storage_name].filesystem

            if filesystem not in filesystems:
                logger.error("Non-existant filesystem %s" % (filesystem))
                continue

            if filesystem not in quota.keys():
                logger.error("No quota defined for storage_name %s [%s]" % (storage_name, filesystem))
                continue

            quota_storage_map = get_mmrepquota_maps(quota[filesystem], storage_name, filesystem, filesets)

            exceeding_filesets[storage_name] = process_fileset_quota(storage,
                                                                     gpfs,
                                                                     storage_name,
                                                                     filesystem,
                                                                     quota_storage_map['FILESET'],
                                                                     opener,
                                                                     opts.options.account_page_url,
                                                                     access_token,
                                                                     opts.options.dry_run)
            exceeding_users[storage_name] = process_user_quota(storage,
                                                               gpfs,
                                                               storage_name,
                                                               filesystem,
                                                               quota_storage_map['USR'],
                                                               user_id_map,
                                                               opener,
                                                               opts.options.account_page_url,
                                                               access_token,
                                                               opts.options.dry_run)

            stats["%s_fileset_critical" % (storage_name,)] = QUOTA_FILESETS_CRITICAL
            if exceeding_filesets[storage_name]:
                stats["%s_fileset" % (storage_name,)] = 1
                logger.warning("storage_name %s found %d filesets that are exceeding their quota" % (storage_name,
                                                                                                len(exceeding_filesets)))
                for (e_fileset, e_quota) in exceeding_filesets[storage_name]:
                    logger.warning("%s has quota %s" % (e_fileset, str(e_quota)))
            else:
                stats["%s_fileset" % (storage_name,)] = 0
                logger.debug("storage_name %s found no filesets that are exceeding their quota" % storage_name)

            notify_exceeding_filesets(gpfs=gpfs,
                                      storage=storage_name,
                                      filesystem=filesystem,
                                      exceeding_items=exceeding_filesets[storage_name],
                                      dry_run=opts.options.dry_run)

            stats["%s_users_warning" % (storage_name,)] = QUOTA_USERS_WARNING
            stats["%s_users_critical" % (storage_name,)] = QUOTA_USERS_CRITICAL
            if exceeding_users[storage_name]:
                stats["%s_users" % (storage_name,)] = len(exceeding_users[storage_name])
                logger.warning("storage_name %s found %d users who are exceeding their quota" %
                               (storage_name, len(exceeding_users[storage_name])))
                for (e_user_id, e_quota) in exceeding_users[storage_name]:
                    logger.warning("%s has quota %s" % (e_user_id, str(e_quota)))
            else:
                stats["%s_users" % (storage_name,)] = 0
                logger.debug("storage_name %s found no users who are exceeding their quota" % storage_name)

            notify_exceeding_users(gpfs=gpfs,
                                   storage=storage_name,
                                   filesystem=filesystem,
                                   exceeding_items=exceeding_users[storage_name],
                                   dry_run=opts.options.dry_run)
    except Exception, err:
        logger.exception("critical exception caught: %s" % (err))
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("quota check completed", stats)

if __name__ == '__main__':
    main()
