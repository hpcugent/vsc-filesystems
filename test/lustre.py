# -*- coding: latin-1 -*-
#
# Copyright 2015-2020 Ghent University
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
Tests for the lustre library.

@author: Kenneth Waegeman (Ghent University)
"""
from __future__ import print_function

import mock
import os
import vsc.filesystem.lustre as lustre

from vsc.install.testing import TestCase


class ToolsTest(TestCase):
    """
    Tests for various auxilliary functions in the lustre lib.
    """


    @mock.patch('vsc.filesystem.posix.PosixOperations._execute')
    @mock.patch('vsc.filesystem.lustre.LustreOperations._sanity_check')
    @mock.patch('vsc.filesystem.lustre.LustreOperations.exists')
    def test__set_grace(self, mock_exists, mock_sanity_check, mock_execute):
        """Test that the command passes is properly constructed so it can be executed by execve."""

        test_path = os.path.join("lustre", "scratch", "gent", "vsc406", "vsc40605")
        mock_sanity_check.return_value = test_path
        mock_exists.return_value = True

        ll = lustre.LustreOperations()
        mock_execute.return_value = (0, "")

        ll._set_grace(test_path, 'user', 7*24*60*60)

        (args, _) = mock_execute.call_args
        self.assertTrue(isinstance(args[0], list))
        self.assertTrue(all([isinstance(a, str) for a in args[0]]))
        self.assertTrue(all([len(s.split(" ")) == 1 for s in args[0]]))

    @mock.patch('vsc.filesystem.posix.PosixOperations._execute')
    @mock.patch('vsc.filesystem.lustre.LustreOperations._sanity_check')
    @mock.patch('vsc.filesystem.lustre.LustreOperations.exists')
    def test__set_quota(self, mock_exists, mock_sanity_check, mock_execute):
        """Test that the command passed is properly constructed so it can be executed by execve."""

        test_path = os.path.join("lustre", "scratch", "gent", "vsc406", "vsc40605")
        mock_sanity_check.return_value = test_path
        mock_exists.return_value = True

        ll = lustre.LustreOperations()
        mock_execute.return_value = (0, "")

        ll._set_quota(2540075, test_path, 1024)

        (args, _) = mock_execute.call_args
        self.assertTrue(isinstance(args[0], list))
        self.assertTrue(all([isinstance(a, str) for a in args[0]]))
        self.assertTrue(all([len(s.split(" ")) == 1 for s in args[0]]))

        ll._set_quota(2540075, test_path, 1024, inode_soft=1000)
        (args, _) = mock_execute.call_args
        self.assertTrue("-i 1000" in ' '.join(args[0]))
        self.assertTrue("-I 1050" in ' '.join(args[0]))

        ll._set_quota(2540075, test_path, 1024, inode_soft=2000, inode_hard=2123)
        (args, _) = mock_execute.call_args
        self.assertTrue("-i 2000" in ' '.join(args[0]))
        self.assertTrue("-I 2123" in ' '.join(args[0]))


