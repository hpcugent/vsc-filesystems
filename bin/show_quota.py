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

import os
import time
from pwd import getpwuid

from vsc.config.base import VscStorage
from vsc.filesystem.quota.entities import QuotaUser, QuotaFileset
from vsc.utils import fancylogger
from vsc.utils.cache import FileCache
from vsc.utils.generaloption import simple_option


fancylogger.logToScreen(True)
fancylogger.setLogLevelWarning()
logger = fancylogger.getLogger('show_quota')

DEFAULT_ALLOWED_TIME_THRESHOLD = 15 * 60

def main():

    options = {
        'storage': ('the VSC filesystems that are checked by this script', None, 'extend', []),
        'threshold': ('allowed the time difference between the cached quota and the time of running', None, 'store',
                      DEFAULT_ALLOWED_TIME_THRESHOLD),
    }
    opts = simple_option(options, config_files='/etc/quota_information.conf')

    storage = VscStorage()
    user_name = getpwuid(os.getuid())[0]
    now = time.time()

    for storage_name in opts.options.storage:

        mount_point = storage[storage_name].login_mount_point
        path_template = storage.path_templates[storage_name]['user']
        path = os.path.join(mount_point, path_template[0], path_template(user_name))

        cache = FileCache(path)
        (timestamp, quota) = cache.load('quota')

        if now - timestamp > opts.options.threshold:
            print "%s: WARNING: no recent quota information (age of data is %d minutes)" % (storage_name,

                                                                                               (now-timestamp)/60)
        else:
            for (fileset, qi) in quota.quota_map.items():
                print "%s: used %d MiB (%d%%) quota %d MiB in fileset %d" % (storage_name,
                                                           quota)


if __name__ == '__main__':
    main()
