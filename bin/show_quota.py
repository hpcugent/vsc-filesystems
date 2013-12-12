#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
# Copyright 2013-2013 Ghent University
#
# This file is part of vsc-filesystems,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
##
"""
Client-side script to gather quota information stored for the user on various filesystems and
display it in an understandable format

Note that this script was begun from scratch after the move to the new storage setup/layout.

@author: Andy Georges (Ghent University)
"""

import grp
import os
import time
from pwd import getpwuid

from vsc.config.base import VSC, VscStorage
from vsc.filesystem.quota.entities import QuotaUser, QuotaFileset
from vsc.utils import fancylogger
from vsc.utils.cache import FileCache
from vsc.utils.generaloption import simple_option


fancylogger.logToScreen(True)
fancylogger.setLogLevelWarning()
logger = fancylogger.getLogger('show_quota')

DEFAULT_ALLOWED_TIME_THRESHOLD = 17 * 60

def quota_pretty_print(storage_name, fileset, quota_information, fileset_prefixes):
    """Returns a nice looking string with all the required quota information."""

    if quota_information.soft == 0:
        return None  # we should not inform the users of filesets where there is no quota limit set

    if fileset.startswith('gvo'):
        storage_name_s = storage_name + "_VO"
    elif fileset.startswith('vsc4'):
        storage_name_s = storage_name
    elif fileset.startswith('gent'):
        storage_name_s = storage_name
    else:
        return None

    s = "%s: used %d MiB (%d%%) quota %d (%d hard limit) MiB" % (
        storage_name_s,
        quota_information.used / 1024^2,
        quota_information.used  * 100 / quota_information.soft,
        quota_information.soft / 1024^2,
        quota_information.hard / 1024^2)

    (exceeds, grace) = quota_information.expired
    if exceeds:
        s += " - quota exceeded, grace = %d" % (grace,)

    return s


def print_user_quota(opts, storage, user_name, now):
    """
    Print the quota for the user, i.e., USR quota in all filesets the user has access to.
    """
    print "User quota:"
    for storage_name in opts.options.storage:

        mount_point = storage[storage_name].login_mount_point
        path_template = storage.path_templates[storage_name]['user']
        path = os.path.join(mount_point, path_template[0], path_template[1](user_name), ".quota_user.json.gz")

        cache = FileCache(path, True)
        try:
            (timestamp, quota) = cache.load('quota')
        except TypeError:
            logger.debug("Cannot load data from %s" % (path,))
            print "%s: WARNING: No quota information found" % (storage_name,)
            continue

        if now - timestamp > opts.options.threshold:
            print "%s: WARNING: no recent quota information (age of data is %d minutes)" % (storage_name,
                                                                                            (now-timestamp)/60)
        else:
            for (fileset, qi) in quota.quota_map.items():
                pp = quota_pretty_print(storage_name, fileset, qi, opts.options.fileset_prefixes)
                if pp:
                    print pp


def print_vo_quota(opts, storage, vos, now):
    """
    Print the quota for the VO fileset.
    """
    print "\nVO quota:"
    for storage_name in [s for s in opts.options.storage if s != 'VSC_HOME']:  # No VOs on VSC_HOME atm

        mount_point = storage[storage_name].login_mount_point
        path_template = storage.path_templates[storage_name]['vo']
        path = os.path.join(mount_point, path_template[0], path_template[1](vos[0]), ".quota_fileset.json.gz")

        cache = FileCache(path, True)
        try:
            (timestamp, quota) = cache.load('quota')
        except TypeError:
            logger.debug("Cannot load data from %s" % (path,))
            print "%s: WARNING: No VO quota information found" % (storage_name,)
            continue

        if now - timestamp > opts.options.threshold:
            print "%s: WARNING: no recent VO quota information (age of data is %d minutes)" % (storage_name,
                                                                                                (now-timestamp)/60)
        else:
            for (fileset, qi) in quota.quota_map.items():
                pp = quota_pretty_print(storage_name, fileset, qi, opts.options.fileset_prefixes)
                if pp:
                    print pp



def main():

    options = {
        'storage': ('the VSC filesystems that are checked by this script', 'strlist', 'store', []),
        'threshold': ('allowed the time difference between the cached quota and the time of running', None, 'store',
                      DEFAULT_ALLOWED_TIME_THRESHOLD),
        'fileset_prefixes': ('the filesets that we allow for showing QuotaUser', 'strlist', 'store', []),
        'vo': ('provide storage details for the VO you belong to', None, 'store_true', False)
    }
    opts = simple_option(options, config_files=['/etc/quota_information.conf'])

    storage = VscStorage()
    vsc = VSC(False)
    user_name = getpwuid(os.getuid())[0]

    vos = [g.gr_name for g in grp.getgrall()
                     if user_name in g.gr_mem
                     and g.gr_name.startswith('gvo')
                     and g.gr_name != vsc.default_vo]  # default VO has no quota associated with it

    opts.options.vo = opts.options.vo and vos

    now = time.time()

    print_user_quota(opts, storage, user_name, now)

    if opts.options.vo:
        print_vo_quota(opts, storage, vos, now)


if __name__ == '__main__':
    main()
