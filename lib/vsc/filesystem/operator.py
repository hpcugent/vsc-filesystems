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

STORAGE_OPERATORS = ('Posix', 'Gpfs', 'OceanStor', 'Lustre')
OPERATOR_CLASS_SUFFIX = 'Operations'
OPERATOR_ERROR_CLASS_SUFFIX = 'OperationError'

class StorageOperator(object):
    """
    Load and initialize the operator class to manage the storage
    """

    def __init__(self, storage):
        """
        Inititalise operator for given storage backend

        @type storage: storage attribute from a VscStorage instance (which is a Storage object)
        """

        Operator, OperatorError = self.import_operator(storage.backend)

        try:
            self.backend_operator = Operator(**storage.operator_config)
        except TypeError:
            logging.exception("Operator of storage backend not found: %s", storage.backend)
            raise
        else:
            self.error = OperatorError

    def __call__(self):
        """Return the backend operator instance"""
        return self.backend_operator

    @staticmethod
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

        backend_operator_name = [label for label in STORAGE_OPERATORS if label.lower() == backend]
        try:
            Operator = getattr(backend_module, backend_operator_name[0] + OPERATOR_CLASS_SUFFIX)
            OperatorError = getattr(backend_module, backend_operator_name[0] + OPERATOR_ERROR_CLASS_SUFFIX)
        except AttributeError as err:
            logging.exception("Operator for %s backend not found: %s", backend, err)
            raise
        except IndexError:
            logging.exception("Unsupported storage backend: %s", backend)
            raise

        return Operator, OperatorError
