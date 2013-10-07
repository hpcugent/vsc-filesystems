#!/usr/bin/env python
# #
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
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption

# Constants
NAGIOS_CHECK_INTERVAL_THRESHOLD = (6 * 60 + 5) * 60  # 365 minutes -- little over 6 hours.
QUOTA_LOG_ZIP_PATH = '/var/log/quota/zips'

logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()

QUOTA_STORE_LOG_CRITICAL = 1

def main():
    """The main."""

    # Note: debug option is provided by generaloption
    # Note: other settings, e.g., ofr each cluster will be obtained from the configuration file
    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'location': ('path to store the gzipped files', None, 'store', QUOTA_LOG_ZIP_PATH),
    }

    opts = ExtendedSimpleOption(options)

    filesystem_error = 0
    filesystem_ok = 0
    error = False

    stats = {}

    try:
        gpfs = GpfsOperations()
        quota = gpfs.list_quota()

        if not os.path.exists(opts.options.location):
            os.makedirs(opts.options.location, 0755)

        for key in quota:
            stats["%s_quota_log_critical" % (key,)] = QUOTA_STORE_LOG_CRITICAL
            try:
                filename = "gpfs_quota_%s_%s.gz" % (time.strftime("%Y%m%d-%H:%M"), key)
                path = os.path.join(opts.options.location, filename)
                zipfile = gzip.open(path, 'wb', 9)  # Compress to the max
                zipfile.write(json.dumps(quota[key]))
                zipfile.close()
                stats["%s_quota_log" % (key,)] = 0
                logger.info("Stored quota information for FS %s" % (key))
            except Exception, err:
                stats["%s_quota_log" % (key,)] = 1
                logger.exception("Failed storing quota information for FS %s" % (key))
    except Exception, err:
        logger.exception("Failure obtaining GPFS quota")
        opts.critical("Failure to obtain GPFS quota information")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("Logged GPFS quota", stats)

if __name__ == '__main__':
    main()
