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
# Allopsrights reserved.
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

        llops = lustre.LustreOperations()
        mock_execute.return_value = (0, "")

        llops._set_grace(test_path, 'user', 7 * 24 * 60 * 60)

        (args, _) = mock_execute.call_args
        self.assertEqual(args[0], ['/usr/bin/lfs', 'setquota', '-t', '-u',
            '-b', '604800', '-i', '604800', 'lustre/scratch/gent/vsc406/vsc40605'])

    @mock.patch('vsc.filesystem.posix.PosixOperations._execute')
    @mock.patch('vsc.filesystem.lustre.LustreOperations._sanity_check')
    @mock.patch('vsc.filesystem.lustre.LustreOperations.exists')
    def test__set_quota(self, mock_exists, mock_sanity_check, mock_execute):
        """Test that the command passed is properly constructed so it can be executed by execve."""

        test_path = os.path.join("/lustre", "scratch", "gent", "vsc406", "vsc40605")
        mock_sanity_check.return_value = test_path
        mock_exists.return_value = True

        llops = lustre.LustreOperations()
        mock_execute.return_value = (0, "")

        llops._set_quota(2540075, test_path, 'user', 10240000)

        (args, _) = mock_execute.call_args
        self.assertEqual(args[0], [
            '/usr/bin/lfs', 'setquota', '-u', '2540075',
            '-b', '9m', '-B', '10m',
            '/lustre/scratch/gent/vsc406/vsc40605'])

        llops._set_quota(2540075, test_path, 'user', 10240000, inode_soft=1000)
        (args, _) = mock_execute.call_args
        self.assertEqual(args[0], [
            '/usr/bin/lfs', 'setquota', '-u', '2540075',
            '-b', '9m', '-B', '10m', '-i', '1000', '-I', '1050',
            '/lustre/scratch/gent/vsc406/vsc40605'])

        llops._set_quota(2540075, test_path, 'user', inode_soft=2000, inode_hard=2123)
        (args, _) = mock_execute.call_args
        self.assertEqual(args[0], [
            '/usr/bin/lfs', 'setquota', '-u', '2540075',
            '-i', '2000', '-I', '2123',
            '/lustre/scratch/gent/vsc406/vsc40605'])

    @mock.patch('vsc.filesystem.posix.PosixOperations._execute')
    @mock.patch('vsc.filesystem.lustre.LustreOperations._sanity_check')
    def test_get_project_id(self, mock_sanity_check, mock_execute):
        test_path = os.path.join("/lustre", "scratch", "gent", "vsc406", "vsc40605")
        mock_sanity_check.return_value = test_path

        llops = lustre.LustreOperations()
        mock_execute.return_value = (0, '    1 P /lustre/scratch/gent/vsc406/vsc40605')
        self.assertEqual(llops.get_project_id(test_path), '1')
        mock_execute.return_value = (0, '    0 - /lustre/scratch/gent/vsc406/vsc40605')
        self.assertEqual(llops.get_project_id(test_path, False), None)
        self.assertRaises(lustre.LustreOperationError, llops.get_project_id, test_path)

    @mock.patch('vsc.filesystem.lustre.LustreOperations.get_project_id')
    @mock.patch('vsc.filesystem.posix.PosixOperations._execute')
    @mock.patch('vsc.filesystem.lustre.LustreOperations._sanity_check')
    @mock.patch('vsc.filesystem.lustre.LustreOperations.exists')
    def test_set_fileset_quota(self, mock_exists, mock_sanity_check, mock_execute, mock_get_project_id):
        test_path = os.path.join("/lustre", "scratch", "gent", "vsc406", "vsc40605")
        mock_sanity_check.return_value = test_path
        mock_execute.return_value = (0, "")

        mock_get_project_id.return_value = '1'
        llops = lustre.LustreOperations()
        llops.set_fileset_quota(None, test_path, inode_soft=1000)
        mock_get_project_id.assert_called_with(test_path)
        (args, _) = mock_execute.call_args
        self.assertEqual(args[0], ['/usr/bin/lfs', 'setquota', '-p', '1', '-i', '1000', '-I', '1050', '/lustre/scratch/gent/vsc406/vsc40605'])
        mock_get_project_id.return_value = '0'
        self.assertRaises(lustre.LustreOperationError, llops.set_fileset_quota, None, '/gent', inode_soft=1000)
        mock_get_project_id.assert_called_with('/gent')

    def test_list_filesystems(self):

        llops = lustre.LustreOperations()

        llops.localfilesystems = [ #posix.py _local_filesystems sets this...
                ['ext2', '/boot', 64769, '/dev/vda1'],
                ['ext4', '/var', 64513, '/dev/mapper/vg0-var'],
                ['ext4', '/tmp', 64512, '/dev/mapper/vg0-scratch'],
                ['tmpfs', '/var/lib/sss/db', 38, 'tmpfs'],
                ['lustre', '/mnt/mdt', 41, '/dev/vdb'],
                ['lustre', '/mnt/ost0', 42, '/dev/vdc'],
                ['lustre', '/mnt/ost1', 43, '/dev/vdd'],
                ['lustre', '/lustre/mylfs', 452646254, '10.141.21.204@tcp:/mylfs'],
                ['tmpfs', '/run/user/2006', 40, 'tmpfs']
                ]
        self.assertEqual(llops.list_filesystems(), {'mylfs': {'defaultMountPoint': '/lustre/mylfs', 'location': '10.141.21.204@tcp'}})
        self.assertEqual(llops.list_filesystems('mylfs'), {'mylfs': {'defaultMountPoint': '/lustre/mylfs', 'location': '10.141.21.204@tcp'}})
        self.assertRaises(lustre.LustreOperationError, llops.list_filesystems, 'nofs')

    def test__execute_lctl_get_param_qmt_yaml(self):
        output_dt_prj = '''qmt.mylfs-QMT0000.dt-0x0.glb-prj=
global_pool0_dt_prj
- id:      0
  limits:  { hard:                    0, soft:                    0, granted:                    0, time:               604800 }
- id:      1
  limits:  { hard:              3798016, soft:              3591168, granted:              3875852, time:           1600334880 }
- id:      2
  limits:  { hard:                    0, soft:                    0, granted:                    0, time:      281474976710656 }
- id:      3
  limits:  { hard:                    0, soft:                    0, granted:                    0, time:      281474976710656 }
- id:      4
  limits:  { hard:                    0, soft:                    0, granted:                    0, time:      281474976710656 }
- id:      6
  limits:  { hard:                    0, soft:                    0, granted:                    0, time:      281474976710656 }
- id:      598
  limits:  { hard:                    0, soft:                    0, granted:                    0, time:      281474976710656 }
'''
        output_md_usr = '''qmt.kwlust-QMT0000.md-0x0.glb-usr=
global_pool0_md_usr
- id:      0
  limits:  { hard:                    0, soft:                    0, granted:                    0, time:               604800 }
- id:      2006
  limits:  { hard:                    0, soft:                    0, granted:                    0, time:      281474976710656 }
'''

        mock_execute.return_vakue = output_dt_prj
        llops = LustreOperations()

    def test_list_quota(self, mock_exists, mock_sanity_check, mock_execute):

        llops = lustre.LustreOperations()
        #llops.list_quota()
        pass

    def test__set_new_project_id(self, mock_exists, mock_sanity_check, mock_execute):
        pass

    def test__get_fshint_for_path(self, mock_exists, mock_sanity_check, mock_execute, mock_what_filesystem):
        pass

    def test__list_filesets(self, mock_exists, mock_sanity_check, mock_execute):
        pass

    def test_list_filesets(self, mock):
        pass

    def test_make_fileset(self, mock_exists, mock_sanity_check, mock_execute):
        pass
