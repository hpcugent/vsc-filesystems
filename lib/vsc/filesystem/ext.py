#!/usr/bin/env python
##
#
# Copyright 2012 Ghent University
# Copyright 2012 Stijn De Weirdt
# Copyright 2012 Andy Georges
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
##
"""
Ext{2,3,4} specialised interface. 

For now, this just offers the posix operations, nothing more.
"""

from vsc.filesystem.posix import PosixOperations, PosixOperationError

class ExtOperationError(PosixOperationError):
    pass


class ExtOperations(PosixOperations):

    def __init__(self):
        super(ExtOperations, self).__init__()
        self.supportedfilesystems = ['ext2', 'ext3', 'ext4']
