#!/usr/bin/env python
##
#
# Copyright 2012-2013 Ghent University
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

import copy
import os
import pwd
import re
import sys
import time

from string import Template

from vsc.administration.user import VscUser
from vsc.config.base import VscStorage
from vsc.filesystem.gpfs import GpfsOperations
from vsc.filesystem.quota.entities import QuotaUser, QuotaFileset
from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.utils import LdapQuery
from vsc.utils import fancylogger
from vsc.utils.availability import proceed_on_ha_service
from vsc.utils.cache import FileCache
from vsc.utils.generaloption import simple_option
from vsc.utils.lock import lock_or_bork, release_or_bork
from vsc.utils.nagios import NagiosReporter, NagiosResult, NAGIOS_EXIT_OK, NAGIOS_EXIT_CRITICAL, NAGIOS_EXIT_WARNING
from vsc.utils.timestamp_pid_lockfile import TimestampedPidLockfile

## Constants
NAGIOS_CHECK_FILENAME = '/var/log/pickles/dquota.nagios.json.gz'
NAGIOS_HEADER = 'quota_check'
NAGIOS_CHECK_INTERVAL_THRESHOLD = 30 * 60  # 30 minutes

QUOTA_CHECK_LOG_FILE = '/var/log/gpfs_quota_checker.log'
QUOTA_CHECK_REMINDER_CACHE_FILENAME = '/var/log/quota/gpfs_quota_checker.report.reminderCache.pickle'
QUOTA_CHECK_LOCK_FILE = '/var/run/gpfs_quota_checker_tpid.lock'

GPFS_GRACE_REGEX = re.compile(r"(?P<days>\d+)days|(?P<hours>\d+hours)|(?P<expired>expired)")

# log setup
fancylogger.logToFile(QUOTA_CHECK_LOG_FILE)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()
logger = fancylogger.getLogger('gpfs_quota_checker')


QUOTA_EXCEEDED_MAIL_TEXT_TEMPLATE = Template('\n'.join([
    'Dear $user_name',
    '',
    '',
    'We have noticed that you have exceeded your quota on the VSC storage,',
    'more in particular: $storage'
    'As you may know, this may have a significant impact on the jobs you',
    'can run on the various clusters.',
    '',
    'Please run "show_quota.py" regularly to check your storage and clean',
    'up any files you no longer require.',
    '',
    'Should you need more storage, you can use your VO data storage.',
    'If you are not a member of a VO, please consider joining one or request',
    'a VO to be created for your research group.',
    ''
    'Also, it is recommended to clear scratch storage and move data you wish',
    'to keep to $$VSC_DATA. Scratch space should remain temporary storage for',
    'running jobs as it is accessible faster than both $$VSC_HOME and $$VSC_DATA.',
    '',
    'At this point on $time, your personal usage is the following:',
    '$quota_info',
    '',
    'Kind regards,',
    'The UGent HPC team',
    ]))


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

        if not grace:
            expired = (False, None)
        else:
            grace = grace.groupdict()
            if grace.get('days', None):
                expired = (True, int(grace['days']) * 86400)
            elif grace.get('hours', None):
                expired = (True, int(grace['hours']) * 3600)
            elif grace.get('expired', None):
                expired = (True, 0)
            else:
                expired = (False, None)
        if quota.filesetname:
            fileset_name = filesets[filesystem][quota.filesetname]['filesetName']
        else:
            fileset_name = None
        logger.debug("The fileset name is %s" % (fileset_name))
        entity.update(fileset_name,
                      int(quota.blockUsage),
                      int(quota.blockQuota),
                      int(quota.blockLimit),
                      int(quota.blockInDoubt),
                      expired,
                      timestamp)

    return entity


def process_fileset_quota(storage, gpfs, storage_name, filesystem, quota_map):
    """Store the quota information in the filesets.
    """

    filesets = gpfs.list_filesets()
    exceeding_filesets = []

    logger.info("filesets = %s" % (filesets))

    for (fileset, quota) in quota_map.items():
        logger.debug("Fileset %s quota: %s" % (filesets[filesystem][fileset]['filesetName'], quota))

        path = filesets[filesystem][fileset]['path']
        filename = os.path.join(path, ".quota_fileset.json.gz")
        path_stat = os.stat(path)

        # TODO: This should somehow be some atomic operation.
        cache = FileCache(filename)
        cache.update(key="quota", data=quota, threshold=0)
        cache.update(key="storage_name", data=storage_name, threshold=0)
        cache.close()

        gpfs.chmod(0640, filename)
        gpfs.chown(path_stat.st_uid, path_stat.st_gid, filename)

        logger.info("Stored fileset %s quota for storage %s at %s" % (fileset, storage, filename))

        #if quota.exceeds():
        if True:
            exceeding_filesets.append((fileset, quota))

    return exceeding_filesets


def process_user_quota(storage, gpfs, storage_name, filesystem, quota_map, user_map):
    """Store the information in the user directories.
    """
    exceeding_users = []
    login_mount_point = storage[storage_name].login_mount_point
    gpfs_mount_point = storage[storage_name].gpfs_mount_point

    for (user_id, quota) in quota_map.items():

        user_name = user_map.get(int(user_id), None)

        if user_name and not user_name.startswith('vsc40075'):
            continue

        if user_name and user_name.startswith('vsc'):
            user = VscUser(user_name)
            logger.debug("Checking quota for user %s with ID %s" % (user_name, user_id))
            logger.debug("User %s quota: %s" % (user, quota))

            path = user._get_path(storage_name)

            # FIXME: We need some better way to address this
            # Right now, we replace the nfs mount prefix which the symlink points to
            # with the gpfs mount point. this is a workaround until we resolve the
            # symlink problem once we take new default scratch into production
            if gpfs.is_symlink(path):
                target = os.path.realpath(path)
                if target.startswith(login_mount_point):
                    new_path = target.replace(login_mount_point, gpfs_mount_point, 1)
                    logger.info("Found a symlinked path %s to the nfs mount point %s. Replaced with %s" %
                                (path, login_mount_point, gpfs_mount_point))
            else:
                new_path = path

            path_stat = os.stat(new_path)
            filename = os.path.join(new_path, ".quota_user.json.gz")

            cache = FileCache(filename)
            cache.update(key="quota", data=quota, threshold=0)
            cache.update(key="storage_name", data=storage_name, threshold=0)
            cache.close()

            gpfs.ignorerealpathmismatch = True
            gpfs.chmod(0640, filename)
            gpfs.chown(path_stat.st_uid, path_stat.st_uid, filename)
            gpfs.ignorerealpathmismatch = False

            logger.info("Stored user %s quota for storage %s at %s" % (user_name, storage_name, filename))

            if quota.exceeds():
                exceeding_users.append((user_id, quota))

    return exceeding_users


def notify(storage, item, quota, dry_run=False):
    """Send out the notification"""
    if item.startswith("gvo"):
        vo = VscLdapGroup(item)
        for recipient in [VscLdapUser(m) for m in vo.moderator]:
            user_name = recipient.gecos
            storage = "The %s VO storage on %s" % (item, storage)
            quota_string = "%s" % (quota)

            logger.info("notification recipient %s" % (recipient))
            logger.info("notification storage %s" % (storage))
            logger.info("notification quota_string %s" % (quota_string))

    elif item.startswith("gpr"):
        pass
    elif item.startswith("vsc"):
        pass


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
        updated = cache.update(item, quota, 7 * 86400)
        logger.info("Cache entry for %s was updated: %s" % (item, updated))
        if updated:
            notify(storage, item, quota, dry_run)

    cache.close()


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
        'nagios': ('print out nagios information', None, 'store_true', False, 'n'),
        'nagios-check-filename': ('filename of where the nagios check data is stored', str, 'store', NAGIOS_CHECK_FILENAME),
        'nagios-check-interval-threshold': ('threshold of nagios checks timing out', None, 'store', NAGIOS_CHECK_INTERVAL_THRESHOLD),
        'storage': ('the VSC filesystems that are checked by this script', None, 'extend', []),
        'dry-run': ('do not make any updates whatsoever', None, 'store_true', False),
        'ha': ('high-availability master IP address', None, 'store', None),
    }
    opts = simple_option(options)

    logger.info('started GPFS quota check run.')

    nagios_reporter = NagiosReporter(NAGIOS_HEADER,
                                     opts.options.nagios_check_filename,
                                     opts.options.nagios_check_interval_threshold)

    if not proceed_on_ha_service(opts.options.ha):
        logger.warning("Not running on the target host in the HA setup. Stopping.")
        nagios_reporter.cache(NAGIOS_EXIT_WARNING,
                        NagiosResult("Not running on the HA master."))
        sys.exit(NAGIOS_EXIT_WARNING)

    if opts.options.nagios:
        nagios_reporter.report_and_exit()
        sys.exit(0)  # not reached

    lockfile = TimestampedPidLockfile(QUOTA_CHECK_LOCK_FILE)
    lock_or_bork(lockfile, nagios_reporter)

    try:
        user_id_map = map_uids_to_names() # is this really necessary?
        LdapQuery(VscConfiguration())
        gpfs = GpfsOperations()
        storage = VscStorage()

        filesystems = gpfs.list_filesystems().keys()
        logger.debug("Found the following GPFS filesystems: %s" % (filesystems))

        filesets = gpfs.list_filesets()
        logger.debug("Found the following GPFS filesets: %s" % (filesets))

        quota = gpfs.list_quota()
        exceeding_filesets = {}
        exceeding_users = {}

        for storage_name in opts.options.storage:

            logger.info("Processing quota for storage_name %s" % (storage_name))
            filesystem = storage[storage_name].filesystem

            if filesystem not in filesystems:
                logger.error("Non-existant filesystem %s" % (filesystem))
                continue

            if filesystem not in quota.keys():
                logger.error("No quota defined for storage_name %s [%s]" % (storage_name, filesystem))
                continue

            quota_storage_map = get_mmrepquota_maps(quota[filesystem], storage_name,filesystem, filesets)

            exceeding_filesets[storage_name] = process_fileset_quota(storage,
                                                                     gpfs,
                                                                     storage_name,
                                                                     filesystem,
                                                                     quota_storage_map['FILESET'])
            exceeding_users[storage_name] = process_user_quota(storage,
                                                               gpfs,
                                                               storage_name,
                                                               filesystem,
                                                               quota_storage_map['USR'],
                                                               user_id_map)

            logger.warning("storage_name %s found %d filesets that are exceeding their quota" % (storage_name,
                                                                                            len(exceeding_filesets)))
            for (e_fileset, e_quota) in exceeding_filesets[storage_name]:
                logger.warning("%s has quota %s" % (e_fileset, str(e_quota)))

            logger.warning("storage_name %s found %d users who are exceeding their quota" % (storage_name,
                                                                                            len(exceeding_users)))
            for (e_user_id, e_quota) in exceeding_users[storage_name]:
                logger.warning("%s has quota %s" % (e_user_id, str(e_quota)))

            notify_exceeding_filesets(gpfs=gpfs,
                                      storage=storage_name,
                                      filesystem=filesystem,
                                      exceeding_items=exceeding_filesets[storage_name],
                                      dry_run=opts.options.dry_run)
            notify_exceeding_users(gpfs=gpfs,
                                   storage=storage_name,
                                   filesystem=filesystem,
                                   exceeding_items=exceeding_users[storage_name],
                                   dry_run=opts.options.dry_run)

    except Exception, err:
        logger.exception("critical exception caught: %s" % (err))
        if not opts.options.dry_run:
            nagios_reporter.cache(NAGIOS_EXIT_CRITICAL, NagiosResult("CRITICAL script failed - %s" % (err.message)))
        if not opts.options.dry_run:
            lockfile.release()
        sys.exit(1)
    except Exception, err:
        logger.exception("exception caught: %s" % (err))
        if not opts.options.dry_run:
            lockfile.release()
        sys.exit(1)

    exceeding_users_count = reduce(lambda acc, k: acc + len(exceeding_users[k]), opts.options.storage, 0),
    exceeding_filesets_count = reduce(lambda acc, k: acc + len(exceeding_filesets[k]), opts.options.storage, 0),
    nagios_result = NagiosResult("quota check completed",
                                 exU=exceeding_users_count,
                                 exU_warning=10,
                                 exU_critical=20,
                                 exF=exceeding_filesets_count,
                                 exF_warning=1,
                                 exF_critical=2,
                                 )

    bork_result = copy.deepcopy(nagios_result)
    bork_result.message = "lock release failed"
    release_or_bork(lockfile, nagios_reporter, bork_result)

    nagios_reporter.cache(NAGIOS_EXIT_OK, nagios_result)
    logger.info("Nagios exit: (%s, %s)" % (nagios_exit_code, nagios_result))

if __name__ == '__main__':
    main()
