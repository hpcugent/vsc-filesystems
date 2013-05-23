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

from vsc.administration.user import VscUser
from vsc.filesystem.gpfs import GpfsOperations
from vsc.filesystem.quota.entities import QuotaUser, QuotaFileset
from vsc.gpfs.quota.report import GpfsQuotaMailReporter
from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.utils import LdapQuery
from vsc.utils import fancylogger
from vsc.utils.cache import FileCache
from vsc.utils.generaloption import simple_option
from vsc.utils.lock import lock_or_bork, release_or_bork
from vsc.utils.nagios import NagiosReporter, NagiosResult, NAGIOS_EXIT_OK, NAGIOS_EXIT_WARNING, NAGIOS_EXIT_CRITICAL
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


def get_mmrepquota_maps(quota_map, filesystem, filesets):
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

    logger.info("ordering USR quota")
    # Iterate over a list of named tuples -- GpfsQuota
    for (user, gpfs_quota) in quota_map['USR'].items():
        user_quota = user_map.get(user, QuotaUser(user))
        user_map[user] = _update_quota_entity(filesets,
                                              user_quota,
                                              filesystem,
                                              gpfs_quota,
                                              timestamp)

    logger.info("ordering FILESET quota")
    # Iterate over a list of named tuples -- GpfsQuota
    for (fileset, gpfs_quota) in quota_map['FILESET'].items():
        fileset_quota = fs_map.get(fileset, QuotaFileset(fileset))
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
                expired = (True, grace['days'] * 86400)
            elif grace.get('hours', None):
                expired = (True, grace['hours'] * 3600)
            elif grace.get('expired', None):
                expired = (True, 0)
            else:
                expired = (False, None)
        if quota.filesetname:
            fileset_name = filesets[filesystem][quota.filesetname]['filesetName']
        else:
            fileset_name = None
        entity.update(filesystem,
                      fileset_name,
                      int(quota.blockUsage),
                      int(quota.blockQuota),
                      int(quota.blockLimit),
                      int(quota.blockInDoubt),
                      expired,
                      timestamp)

    return entity


def process_fileset_quota(gpfs, storage, filesystem, quota_map):
    """Store the quota information in the filesets.
    """

    filesets = gpfs.list_filesets()

    logger.info("filesets = %s" % (filesets))

    for (fileset, quota) in quota_map.items():
        logger.debug("Fileset %s quota: %s" % (filesets[filesystem][fileset]['filesetName'], quota))

        path = filesets[filesystem][fileset]['path']
        filename = os.path.join(path, ".quota_fileset.json.gz")
        path_stat = os.stat(path)

        # TODO: This should somehow be some atomic operation.
        cache = FileCache(filename)
        cache.update(key="quota", data=quota, threshold=0)
        cache.update(key="storage", data=storage, threshold=0)
        cache.close()

        gpfs.chmod(0640, filename)
        gpfs.chown(path_stat.st_uid, path_stat.st_gid, filename)

        logger.info("Stored fileset %s quota for storage %s at %s" % (fileset, storage, filename))


def process_user_quota(gpfs, storage, filesystem, quota_map, user_map):
    """Store the information in the user directories.
    """
    for (user_id, quota) in quota_map.items():

        user_name = user_map.get(int(user_id), None)

        logger.debug("Checking quota for user %s with ID %s" % (user_name, user_id))

        if user_name and user_name.startswith('vsc4'):
            user = VscUser(user_name)
            logger.debug("User %s quota: %s" % (user, quota))

            path = user._get_path(storage)
            path_stat = os.stat(path)
            filename = os.path.join(path, ".quota_user.json.gz")

            cache = FileCache(filename)
            cache.update(key="quota", data=quota, threshold=0)
            cache.update(key="storage", data=storage, threshold=0)
            cache.close()

            gpfs.chmod(0640, filename)
            gpfs.chown(path_stat.st_uid, path_stat.st_uid, filename)

            logger.info("Stored user %s quota for storage %s at %s" % (user_name, storage, filename))



def nagios_analyse_data(ex_users, ex_vos, user_count, vo_count):
    """Analyse the data blobs we gathered and build a summary for nagios.

    @type ex_users: [ quota.entities.User ]
    @type ex_vos: [ quota.entities.VO ]
    @type user_count: int
    @type vo_count: int

    Returns a tuple with two elements:
        - the exit code to be provided when the script runs as a nagios check
        - the message to be printed when the script runs as a nagios check
    """
    ex_u = len(ex_users)
    ex_v = len(ex_vos)
    if ex_u == 0 and ex_v == 0:
        return (NAGIOS_EXIT_OK, NagiosResult("No quota exceeded", ex_u=0, ex_v=0, pU=0, pV=0))
    else:
        pU = float(ex_u) / user_count
        pV = float(ex_v) / vo_count
        return (NAGIOS_EXIT_OK, NagiosResult("Quota exceeded", ex_u=ex_u, ex_v=ex_v, pU=pU, pV=pV))


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
    }
    opts = simple_option(options)

    logger.info('started GPFS quota check run.')

    nagios_reporter = NagiosReporter(NAGIOS_HEADER,
                                     opts.options.nagios_check_filename,
                                     opts.options.nagios_check_interval_threshold)

    if opts.options.nagios:
        nagios_reporter.report_and_exit()
        sys.exit(0)  # not reached

    lockfile = TimestampedPidLockfile(QUOTA_CHECK_LOCK_FILE)
    lock_or_bork(lockfile, nagios_reporter)

    try:
        user_id_map = map_uids_to_names() # is this really necessary?
        LdapQuery(VscConfiguration())
        gpfs = GpfsOperations()
        filesystems = gpfs.list_filesystems().keys()
        logger.debug("Found the following GPFS filesystems: %s" % (filesystems))

        filesets = gpfs.list_filesets()
        logger.debug("Found the following GPFS filesets: %s" % (filesets))

        quota = gpfs.list_quota()

        for storage in opts.options.storage:

            logger.info("Processing quota for storage %s" % (storage))
            filesystem = opts.configfile_parser.get(storage, 'filesystem')

            if filesystem not in filesystems:
                logger.error("Non-existant filesystem %s" % (filesystem))
                continue

            if filesystem not in quota.keys():
                logger.error("No quota defined for storage %s [%s]" % (storage, filesystem))
                continue

            quota_storage_map = get_mmrepquota_maps(quota[filesystem], filesystem, filesets)

            process_fileset_quota(gpfs, storage, filesystem, quota_storage_map['FILESET'])
            process_user_quota(gpfs, storage, filesystem, quota_storage_map['USR'], user_id_map)


        sys.exit(1)

        # figure out which users are crossing their softlimits
        ex_users = filter(lambda u: u.exceeds(), mm_rep_quota_map_users.values())
        logger.warning("found %s users who are exceeding their quota: %s" % (len(ex_users), [u.user_id for u in ex_users]))

        # figure out which VO's are exceeding their softlimits
        # currently, we're not using this, VO's should have plenty of space
        ex_vos = filter(lambda v: v.exceeds(), mm_rep_quota_map_vos.values())
        logger.warning("found %s VOs who are exceeding their quota: %s" % (len(ex_vos), [v.fileset_id for v in ex_vos]))

        # FIXME: cache the storage quota information (test for exceeding users)
        u_storage = UserFsQuotaStorage()
        for user in mm_rep_quota_map_users.values():
            try:
                if not opts.options.dry_run:
                    u_storage.store_quota(user)
            except VscError, err:
                logger.error("Could not store data for user %s" % (user.user_id))
                pass  # we're just moving on, trying the rest of the users. The error will have been logged anyway.

        v_storage = VoFsQuotaStorage()
        for vo in mm_rep_quota_map_vos.values():
            try:
                if not opts.options.dry_run:
                    v_storage.store_quota(vo)
            except VscError, err:
                log.error("Could not store vo data for vo %s" % (vo.fileset_id))
                pass  # we're just moving on, trying the rest of the VOs. The error will have been logged anyway.

        if not opts.options.dry_run:
            # Report to the users who are exceeding their quota
            LdapQuery(VscConfiguration())  # Initialise here, the mailreporter will use it.
            reporter = GpfsQuotaMailReporter(QUOTA_CHECK_REMINDER_CACHE_FILENAME)
            for user in ex_users:
                reporter.report_user(user)
            log.info("Done reporting users.")
            reporter.close()

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

    (nagios_exit_code, nagios_result) = nagios_analyse_data(ex_users,
                                                            ex_vos,
                                                            user_count=len(mm_rep_quota_map_users.values()),
                                                            vo_count=len(mm_rep_quota_map_vos.values()))

    bork_result = copy.deepcopy(nagios_result)
    bork_result.message = "lock release failed"
    release_or_bork(lockfile, nagios_reporter, bork_result)

    nagios_reporter.cache(nagios_exit_code, "%s" % (nagios_result,))
    log.info("Nagios exit: (%s, %s)" % (nagios_exit_code, nagios_result))

if __name__ == '__main__':
    main()
