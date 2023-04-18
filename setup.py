#!/usr/bin/env python
# -*- coding: latin-1 -*-
# #
# Copyright 2009-2016 Ghent University
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
# #
"""
vsc-filesystems base distribution setup.py

@author: Stijn De Weirdt (Ghent University)
@author: Andy Georges (Ghent University)
"""
import sys
import vsc.install.shared_setup as shared_setup
from vsc.install.shared_setup import ag, kh, sdw, kw, wdp

install_requires = [
    'vsc-base >= 3.0.3',
    'vsc-config >= 3.0.0',
    'vsc-utils >= 2.0.0',
    'future >= 0.16.0',
]

if sys.version_info < (3, 0):
    install_requires.append('pyyaml < 6')
else:
    install_requires.append('pyyaml')

PACKAGE = {
    'version': '1.4.0',
    'author': [sdw, ag, kh, kw],
    'maintainer': [sdw, ag, kh, kw, wdp],
    'setup_requires': ['vsc-install >= 0.15.2'],
    'tests_require': ['mock'],
    'install_requires': install_requires
}

if __name__ == '__main__':
    shared_setup.action_target(PACKAGE)
