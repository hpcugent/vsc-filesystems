#!/usr/bin/env python
##
#
# Copyright 2013 Ghent University
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# All rights reserved
"""
This script stores the quota information for the various mounted GPFS filesystems
in a zip file, named by date and filesystem.

@author Andy Georges
"""
import gzip
import json
import os
import sys
import time

from vsc.filesystem.gpfs import GpfsOperations
from vsc.utils import fancylogger
from vsc.utils.availability import proceed_on_ha_service
from vsc.utils.generaloption import simple_option
from vsc.utils.lock import lock_or_bork, release_or_bork
from vsc.utils.nagios import NagiosReporter, NagiosResult, NAGIOS_EXIT_OK, NAGIOS_EXIT_WARNING, NAGIOS_EXIT_CRITICAL
from vsc.utils.timestamp_pid_lockfile import TimestampedPidLockfile

#Constants
NAGIOS_CHECK_FILENAME = '/var/log/pickles/quota_log.nagios.pickle'
NAGIOS_HEADER = 'quota_log'
NAGIOS_CHECK_INTERVAL_THRESHOLD = (6 * 60 + 5) * 60   # 365 minutes -- little over 6 hours.

QUOTA_LOG_LOCK_FILE = '/var/run/quota_log.tpid.lock'
QUOTA_LOG_ZIP_PATH = '/var/log/quota/zips'

logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()


def main():
    """The main."""

    # Note: debug option is provided by generaloption
    # Note: other settings, e.g., ofr each cluster will be obtained from the configuration file
    options = {
        'nagios': ('print out nagios information', None, 'store_true', False, 'n'),
        'nagios-check-filename': ('filename of where the nagios check data is stored',
                                  str,
                                  'store',
                                  NAGIOS_CHECK_FILENAME),
        'nagios-check-interval-threshold': ('threshold of nagios checks timing out',
                                            None,
                                            'store',
                                            NAGIOS_CHECK_INTERVAL_THRESHOLD),
        'location': ('path to store the gzipped files', None, 'store', QUOTA_LOG_ZIP_PATH),
        'ha': ('high-availability master IP address', None, 'store', None),
        'dry-run': ('do not make any updates whatsoever', None, 'store_true', False),
    }

    opts = simple_option(options)

    nagios_reporter = NagiosReporter(NAGIOS_HEADER,
                                     opts.options.nagios_check_filename,
                                     opts.options.nagios_check_interval_threshold)
    if opts.options.nagios:
        logger.debug("Producing Nagios report and exiting.")
        nagios_reporter.report_and_exit()
        sys.exit(0)  # not reached

    if not proceed_on_ha_service(opts.options.ha):
        logger.warning("Not running on the target host in the HA setup. Stopping.")
        nagios_reporter.cache(NAGIOS_EXIT_WARNING,
                              NagiosResult("Not running on the HA master."))
        sys.exit(NAGIOS_EXIT_WARNING)

    lockfile = TimestampedPidLockfile(QUOTA_LOG_LOCK_FILE)
    lock_or_bork(lockfile, nagios_reporter)

    logger.info("starting quota_log run")

    filesystem_error = 0
    filesystem_ok = 0
    error = False

    try:
        gpfs = GpfsOperations()
        quota = gpfs.list_quota()

        for key in quota:
            try:
                filename = "gpfs_quota_%s_%s.gz" % (time.strftime("%Y%m%d-%H:%M"), key)
                path = os.path.join(opts.options.location, filename)
                zipfile = gzip.open(path, 'wb', 9)  # Compress to the max
                zipfile.write(json.dumps(quota[key]))
                zipfile.close()
                filesystem_ok += 1
                logger.info("Stored quota information for FS %s" % (key))
            except Exception, err:
                logger.exception("Failed storing quota information for FS %s" % (key))
                filesystem_error += 1
    except Exception, err:
        logger.exception("Failure obtaining GPFS quota")
        error = True

    logger.info("Finished quota_log")

    bork_result = NagiosResult("lock release failed",
                               fs=filesystem_ok,
                               fs_error=filesystem_error)
    release_or_bork(lockfile, nagios_reporter, bork_result)

    logger.info("Released lock")

    if not error:
        nagios_reporter.cache(NAGIOS_EXIT_OK,
                              NagiosResult("quota logged",
                                           fs=filesystem_ok,
                                           fs_error=filesystem_error))
    else:
        nagios_reporter.cache(NAGIOS_EXIT_CRITICAL,
                              NagiosResult("quota not obtained",
                                           fs=0,
                                           fs_error=0))

    sys.exit(0)

if __name__ == '__main__':
    main()
