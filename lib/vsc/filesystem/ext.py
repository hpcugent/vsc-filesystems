# -*- coding: latin-1 -*-
#
# Copyright 2009-2023 Ghent University
#
# This file is part of vsc-filesystems,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-filesystems
#
# All rights reserved.
#
"""
Ext{2,3,4} specialised interface.

For now, this just offers the posix operations, nothing more.

@author: Stijn De Weirdt (Ghent University)
@author: Andy Georges (Ghent University)
"""

from vsc.filesystem.posix import PosixOperations, PosixOperationError

class ExtOperationError(PosixOperationError):
    pass


class ExtOperations(PosixOperations):

    def __init__(self):
        super(ExtOperations, self).__init__()
        self.supportedfilesystems = ['ext2', 'ext3', 'ext4']
