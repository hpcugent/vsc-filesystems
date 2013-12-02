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
This script stores the inode usage information for the various mounted GPFS filesystems
in a zip file, named by date and filesystem.

@author Andy Georges (Ghent University)
"""
import gzip
import json
import os
import socket
import sys
import time

from collections import namedtuple

from vsc.filesystem.gpfs import GpfsOperations
from vsc.utils import fancylogger
from vsc.utils.mail import VscMail
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption

# Constants
NAGIOS_CHECK_INTERVAL_THRESHOLD = (6 * 60 + 5) * 60  # 365 minutes -- little over 6 hours.
INODE_LOG_ZIP_PATH = '/var/log/quota/inode-zips'

logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()

INODE_STORE_LOG_CRITICAL = 1


InodeCritical = namedtuple('InodeCritical', 'allocated, maxinodes')


def process_inodes_information(filesets):
    """
    Determines which filesets have reached a critical inode limit.

    @returns: dict with (filesetname, InodeCritical) key-value pairs
    """
    critical_filesets = dict()

    for fs_info in filesets.values():
        allocated = int(fs_info['allocInodes'])
        maxinodes = int(fs_info['maxInodes'])

        if allocated > 0.9 * maxinodes:
            critical_filesets[fs_info['filesetName']] = InodeCritical(allocated=allocated, maxinodes=maxinodes)

    return critical_filesets


def mail_admins(critical_filesets, dry_run):
    """Send email to the HPC admin about the inodes running out soonish."""
    mail = VscMail(mail_host="smtp.ugent.be")

    message = """
Dear HPC admins,

The following filesets will be running out of inodes soon (or may already have run out).

%(fileset_info)s

Kind regards,
Your friendly inode-watching script
"""
    fileset_info = []
    for (fs_name, fs_info) in critical_filesets.items():
        for (fileset_name, inode_info) in fs_info.items():
            fileset_info.append("%s - %s: used %d (%d%%) of %d" % (fs_name,
                                                                 fileset_name,
                                                                 inode_info.allocated,
                                                                 int(inode_info.allocated * 100 / inode_info.maxinodes),
                                                                 inode_info.maxinodes))

    message = message % ({'fileset_info': "\n".join(fileset_info)})

    if dry_run:
        logger.info("Would have sent this message: %s" % (message,))
    else:
        mail.sendTextMail(mail_to="hpc-admin@lists.ugent.be",
                          mail_from="hpc-admin@lists.ugent.be",
                          reply_to="hpc-admin@lists.ugent.be",
                          mail_subject="Inode space(s) running out on %s" % (socket.gethostname()),
                          message=message)


def main():
    """The main."""

    # Note: debug option is provided by generaloption
    # Note: other settings, e.g., ofr each cluster will be obtained from the configuration file
    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'location': ('path to store the gzipped files', None, 'store', INODE_LOG_ZIP_PATH),
    }

    opts = ExtendedSimpleOption(options)

    stats = {}

    try:
        gpfs = GpfsOperations()
        filesets = gpfs.list_filesets()

        if not os.path.exists(opts.options.location):
            os.makedirs(opts.options.location, 0755)

        critical_filesets = dict()

        for filesystem in filesets:
            stats["%s_inodes_log_critical" % (filesystem,)] = INODE_STORE_LOG_CRITICAL
            try:
                filename = "gpfs_inodes_%s_%s.gz" % (time.strftime("%Y%m%d-%H:%M"), filesystem)
                path = os.path.join(opts.options.location, filename)
                zipfile = gzip.open(path, 'wb', 9)  # Compress to the max
                zipfile.write(json.dumps(filesets[filesystem]))
                zipfile.close()
                stats["%s_inodes_log" % (filesystem,)] = 0
                logger.info("Stored inodes information for FS %s" % (filesystem))

                cfs = process_inodes_information(filesets[filesystem])
                logger.info("Processed inodes information for filesystem %s" % (filesystem,))
                if cfs:
                    critical_filesets[filesystem] = cfs
                    logger.info("Filesystem %s has at least %d filesets reaching the limit" % (filesystem, len(cfs)))

            except Exception:
                stats["%s_inodes_log" % (filesystem,)] = 1
                logger.exception("Failed storing inodes information for FS %s" % (filesystem))

        logger.info("Critical filesets: %s" % (critical_filesets,))

        if critical_filesets:
            mail_admins(critical_filesets, opts.options.dry_run)

    except Exception:
        logger.exception("Failure obtaining GPFS inodes")
        opts.critical("Failure to obtain GPFS inodes information")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("Logged GPFS inodes", stats)

if __name__ == '__main__':
    main()

