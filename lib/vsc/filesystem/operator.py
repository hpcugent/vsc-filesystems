# -*- coding: latin-1 -*-
#
# Copyright 2009-2022 Ghent University
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
Interface to dynamically load vsc.filesystem modules and instantiate its XxxxOperations class

@author: Alex Domingo (Vrije Universiteit Brussel)
"""
import logging

def load_storage_operator(storage):
    """
    Load and initialize corresponding operator class for this filesystem
    Return *Operations object

    @type storage: storage attribute from a VscStorage instance (which is a Storage object)
    """
    if getattr(storage, 'backend_operator', None):
        return storage.backend_operator

    Operator, OperatorError = import_operator(storage.backend)

    try:
        storage.backend_operator = Operator(**storage.api)
    except TypeError:
        logging.exception("Operator of storage backend not found: %s", storage.backend)
        raise
    else:
        storage.backend_operator_err = OperatorError

    return storage.backend_operator

def import_operator(backend):
    """
    Import corresponding filesystem operator class and exception for storage backend

    @type backend: string with name of storage backend
    """
    Operator = None
    OperatorError = None

    if backend == 'posix':
        try:
            from vsc.filesystem.posix import PosixOperations as Operator, PosixOperationError as OperatorError
        except ImportError:
            logging.exception("Failed to load PosixOperations from vsc.filesystem.posix")
            raise
    elif backend == 'gpfs':
        try:
            from vsc.filesystem.gpfs import GpfsOperations as Operator, GpfsOperationError as OperatorError
        except ImportError:
            logging.exception("Failed to load GpfsOperations from vsc.filesystem.gpfs")
            raise
    elif backend == 'oceanstor':
        try:
            from vsc.filesystem.oceanstor import OceanStorOperations as Operator
            from vsc.filesystem.oceanstor import OceanStorOperationError as OperatorError
        except ImportError:
            logging.exception("Failed to load OceanStorOperations from vsc.filesystem.oceanstor")
            raise
    else:
        logging.error("Storage backend '%s' is unsupported by vsc.filesystem", backend)

    return Operator, OperatorError

