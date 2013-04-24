#!/usr/bin/env python
##
#
# Copyright 2009-2012 Ghent University
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


import os
import sys
import time

from vsc.utils import fancylogger
from vsc.utils.availability import proceed_on_ha_service
from vsc.utils.generaloption import simple_option
from vsc.utils.lock import lock_or_bork, release_or_bork
from vsc.utils.nagios import NagiosReporter, NagiosResult, NAGIOS_EXIT_OK, NAGIOS_EXIT_WARNING
from vsc.utils.timestamp_pid_lockfile import TimestampedPidLockfile




if __name__ == '__main__':
    main()
