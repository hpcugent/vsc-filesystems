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
import importlib
import logging

STORAGE_OPERATORS = {
    'posix': ('PosixOperations', 'PosixOperationError'),
    'gpfs': ('GpfsOperations', 'GpfsOperationError'),
    'oceanstor': ('OceanStorOperations', 'OceanStorOperationError'),
    'lustre': ('LustreOperations', 'LustreOperationError'),
}

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
        storage.backend_operator = Operator(**storage.operator_config)
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

    backend_module_name = '.'.join(['vsc', 'filesystem', backend])

    try:
       backend_module = importlib.import_module(backend_module_name)
    except (ImportError, ModuleNotFoundError):
        logging.exception("Failed to load %s module", backend_module_name)
        raise

    try:
        Operator = getattr(backend_module, STORAGE_OPERATORS[backend][0])
        OperatorError = getattr(backend_module, STORAGE_OPERATORS[backend][1])
    except AttributeError as err:
        logging.exception("Operator for %s backend not found: %s", backend, err)
        raise
    except KeyError:
        logging.exception("Unsupported storage backend: %s", backend)
        raise

    return Operator, OperatorError

