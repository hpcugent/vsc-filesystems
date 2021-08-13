# -*- coding: latin-1 -*-
#
# Copyright 2015-2021 Ghent University
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
import glob
import vsc.filesystem.lustre as lustre
from vsc.filesystem.lustre import LustreQuota, LustreVscTier1cScratchFs

from vsc.install.testing import TestCase

LUSTRE_QUOTA_OUTPUT = {
    'USR': {
        'block':[
            {'id': 0, 'limits': {'hard': 0, 'soft': 0, 'granted': 0, 'time': 604800}},
            {'id': 2005, 'limits': {'hard': 10000000, 'soft': 5000000, 'granted': 0, 'time': 604800}},
            {'id': 2006, 'limits': {'hard': 50000000, 'soft': 45000000, 'granted': 10240000, 'time': 281474976710656}}],
        'inode':[
            {'id': 0, 'limits': {'hard': 0, 'soft': 0, 'granted': 0, 'time': 604800}},
            {'id': 2006, 'limits': {'hard': 1200000, 'soft': 1000000, 'granted': 200, 'time': 281474976710656}}],
        },
    'GRP': {
        'block':[
            {'id': 0, 'limits': {'hard': 0, 'soft': 0, 'granted': 0, 'time': 604800}},
            {'id': 2006, 'limits': {'hard': 3584000, 'soft': 3072000, 'granted': 4285456, 'time': 1600685548}}],
        'inode':[
            {'id': 0, 'limits': {'hard': 0, 'soft': 0, 'granted': 0, 'time': 604800}},
            {'id': 2006, 'limits': {'hard': 1200000, 'soft': 1000000, 'granted': 200, 'time': 281474976710656}}],
        },
    'FILESET': {
        'block':[
            {'id': 0, 'limits': {'hard': 0, 'soft': 0, 'granted': 0, 'time': 604800}},
            {'id': 1, 'limits': {'hard': 3798016, 'soft': 3591168, 'granted': 3875852, 'time': 1600334880}},
            {'id': 598, 'limits': {'hard': 1100000, 'soft': 1000000, 'granted': 0, 'time': 281474976710656}}],
        'inode':[
            {'id': 0, 'limits': {'hard': 0, 'soft': 0, 'granted': 0, 'time': 604800}},
            {'id': 1, 'limits': {'hard': 1000, 'soft': 900, 'granted': 950, 'time': 1600334880}},
            {'id': 598, 'limits': {'hard': 1100000, 'soft': 1000000, 'granted': 0, 'time': 281474976710656}}]
        },
    }

LUSTRE_FILESET_TREE = {
    'mylfs': {
        '5': {'path': '/lustre/mylfs/gent/archive', 'filesetName': 'archive'},
        '900006': {'path': '/lustre/mylfs/gent/projects/000/pj00006', 'filesetName': 'pj00006'},
        '900002': {'path': '/lustre/mylfs/gent/projects/000/pj00002', 'filesetName': 'pj00002'},
        '900004': {'path': '/lustre/mylfs/gent/projects/000/pj00004', 'filesetName': 'pj00004'},
        '900110': {'path': '/lustre/mylfs/gent/projects/001/pj00110', 'filesetName': 'pj00110'},
        '900112': {'path': '/lustre/mylfs/gent/projects/001/pj00112', 'filesetName': 'pj00112'},
        '900111': {'path': '/lustre/mylfs/gent/projects/001/pj00111', 'filesetName': 'pj00111'}
        }
    }

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

        llops._set_grace(test_path, lustre.Typ2Opt.user, 7 * 24 * 60 * 60)

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

        llops._set_quota(2540075, test_path, lustre.Typ2Opt.user, 10240000)

        (args, _) = mock_execute.call_args
        self.assertEqual(args[0], [
            '/usr/bin/lfs', 'setquota', '-u', '2540075',
            '-b', '9m', '-B', '10m',
            '/lustre/scratch/gent/vsc406/vsc40605'])

        llops._set_quota(2540075, test_path, lustre.Typ2Opt.user, 10240000, inode_soft=1000)
        (args, _) = mock_execute.call_args
        self.assertEqual(args[0], [
            '/usr/bin/lfs', 'setquota', '-u', '2540075',
            '-b', '9m', '-B', '10m', '-i', '1000', '-I', '1050',
            '/lustre/scratch/gent/vsc406/vsc40605'])

        llops._set_quota(2540075, test_path, lustre.Typ2Opt.user, inode_soft=2000, inode_hard=2123)
        (args, _) = mock_execute.call_args
        self.assertEqual(args[0], [
            '/usr/bin/lfs', 'setquota', '-u', '2540075',
            '-i', '2000', '-I', '2123',
            '/lustre/scratch/gent/vsc406/vsc40605'])

    @mock.patch('vsc.filesystem.posix.PosixOperations._execute')
    @mock.patch('vsc.filesystem.lustre.LustreOperations._sanity_check')
    def test_get_project_id(self, mock_sanity_check, mock_execute):
        """ Test the retrieval of projectid on fs """
        test_path = os.path.join("/lustre", "scratch", "gent", "vsc406", "vsc40605")
        mock_sanity_check.side_effect = lambda x: x

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
        """ test setting quota for specified fileset """
        test_path = os.path.join("/lustre", "scratch", "gent", "vsc406", "vsc40605")
        mock_sanity_check.side_effect = lambda x: x
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
        """ Test retrieving all Lustre file systems """

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
        self.assertEqual(llops.list_filesystems(['mylfs']), {'mylfs': {'defaultMountPoint': '/lustre/mylfs', 'location': '10.141.21.204@tcp'}})
        self.assertRaises(lustre.LustreOperationError, llops.list_filesystems, 'nofs')

    @mock.patch('vsc.utils.run.RunAsyncLoop.run')
    @mock.patch('vsc.filesystem.posix.PosixOperations._execute')
    def test__execute_lctl_get_param_qmt_yaml(self, mock_execute, mock_run):
        """ Test executing lctl get_param output parsing """
        output_dt_prj = '''qmt.mylfs-QMT0000.dt-0x0.glb-prj=
global_pool0_dt_prj
- id:      0
  limits:  { hard:                    0, soft:                    0, granted:                    0, time:               604800 }
- id:      1
  limits:  { hard:              3798016, soft:              3591168, granted:              3875852, time:           1600334880 }
- id:      598
  limits:  { hard:              1100000, soft:              1000000, granted:                    0, time:      281474976710656 }
'''
        output_md_usr = '''qmt.mylfs-QMT0000.md-0x0.glb-usr=
global_pool0_md_usr
- id:      0
  limits:  { hard:                    0, soft:                    0, granted:                    0, time:               604800 }
- id:      2006
  limits:  { hard:              1200000, soft:              1000000, granted:                   200, time:      281474976710656 }
'''

        quota_result_data = [{
            'id': 0, 'limits': {'hard': 0, 'soft': 0, 'granted': 0, 'time': 604800}},
            {'id': 1, 'limits': {'hard': 3798016, 'soft': 3591168, 'granted': 3875852, 'time': 1600334880}},
            {'id': 598, 'limits': {'hard': 1100000, 'soft': 1000000, 'granted': 0, 'time': 281474976710656}}]

        quota_result_md = [
              {'id': 0, 'limits': {'hard': 0, 'soft': 0, 'granted': 0, 'time': 604800}},
              {'id': 2006, 'limits': {'hard': 1200000, 'soft': 1000000, 'granted': 200, 'time': 281474976710656}}]

        mock_execute.return_value = (0, output_dt_prj)
        llops = lustre.LustreOperations()
        quots = llops._execute_lctl_get_param_qmt_yaml('mylfs', lustre.Typ2Param.FILESET, lustre.Quotyp2Param.block)
        (args, _) = mock_execute.call_args
        self.assertEqual(args[0], ['/usr/sbin/lctl', 'get_param', 'qmt.mylfs-*.dt-*.glb-prj'])
        self.assertEqual(quots, quota_result_data)

        mock_run.return_value = (0, output_dt_prj)
        quots = llops._execute_lctl_get_param_qmt_yaml('mylfs', lustre.Typ2Param.FILESET, lustre.Quotyp2Param.block, False)
        mock_run.assert_called_with(['cat', '/var/cache/lustre/qmt.mylfs-*.dt-*.glb-prj'])
        self.assertEqual(quots, quota_result_data)

        mock_execute.return_value = (0, output_md_usr)
        quots = llops._execute_lctl_get_param_qmt_yaml('mylfs', lustre.Typ2Param.USR, lustre.Quotyp2Param.inode)
        (args, _) = mock_execute.call_args
        self.assertEqual(args[0], ['/usr/sbin/lctl', 'get_param', 'qmt.mylfs-*.md-*.glb-usr'])
        self.assertEqual(quots, quota_result_md)

        mock_run.return_value = (0, output_md_usr)
        quots = llops._execute_lctl_get_param_qmt_yaml('mylfs', lustre.Typ2Param.USR, lustre.Quotyp2Param.inode, False)
        mock_run.assert_called_with(['cat', '/var/cache/lustre/qmt.mylfs-*.md-*.glb-usr'])
        self.assertEqual(quots, quota_result_md)

    @mock.patch('vsc.utils.run.RunNoWorries.run')
    @mock.patch('vsc.filesystem.lustre.LustreOperations._execute_lctl')
    @mock.patch('vsc.filesystem.lustre.LustreOperations._execute_lctl_get_param_qmt_yaml')
    def test_list_quota(self, mock_lctl_yaml, mock_lctl, mock_execute):
        """ Test making quota structure """

        quota_result = {'mylfs':{
            'USR': {
                '0': [LustreQuota(name='0', blockUsage=0, blockQuota=0, blockLimit=0, blockGrace=604800, blockInDoubt=0, filesUsage=0, filesQuota=0, filesLimit=0, filesGrace=604800, filesInDoubt=0)],
                '2005': [LustreQuota(name='2005', blockUsage=0, blockQuota=5000000, blockLimit=10000000, blockGrace=604800, blockInDoubt=0, filesUsage=0, filesQuota=0, filesLimit=0, filesGrace=0, filesInDoubt=0)],
                '2006': [LustreQuota(name='2006', blockUsage=10240000, blockQuota=45000000, blockLimit=50000000, blockGrace=281474976710656, blockInDoubt=0, filesUsage=200, filesQuota=1000000, filesLimit=1200000, filesGrace=281474976710656, filesInDoubt=0)]
            },
            'GRP': {
                '0': [LustreQuota(name='0', blockUsage=0, blockQuota=0, blockLimit=0, blockGrace=604800, blockInDoubt=0, filesUsage=0, filesQuota=0, filesLimit=0, filesGrace=604800, filesInDoubt=0)],
                '2006': [LustreQuota(name='2006', blockUsage=4285456, blockQuota=3072000, blockLimit=3584000, blockGrace=1600685548, blockInDoubt=0, filesUsage=200, filesQuota=1000000, filesLimit=1200000, filesGrace=281474976710656, filesInDoubt=0)]
            },
            'FILESET': {
                '0': [LustreQuota(name='0', blockUsage=0, blockQuota=0, blockLimit=0, blockGrace=604800, blockInDoubt=0, filesUsage=0, filesQuota=0, filesLimit=0, filesGrace=604800, filesInDoubt=0, filesetname='0')],
                '1': [LustreQuota(name='1', blockUsage=3875852, blockQuota=3591168, blockLimit=3798016, blockGrace=1600334880, blockInDoubt=0, filesUsage=950, filesQuota=900, filesLimit=1000, filesGrace=1600334880, filesInDoubt=0, filesetname='1')],
                '598': [LustreQuota(name='598', blockUsage=0, blockQuota=1000000, blockLimit=1100000, blockGrace=281474976710656, blockInDoubt=0, filesUsage=0, filesQuota=1000000, filesLimit=1100000, filesGrace=281474976710656, filesInDoubt=0, filesetname='598')]}
            }
        }

        def quota_mock(fs, typ, quotyp, ok):
            return LUSTRE_QUOTA_OUTPUT[typ.name][quotyp.name]

        mock_lctl_yaml.side_effect = quota_mock
        mock_lctl.return_value = True
        mock_execute.return_value = (2, "Not ok")
        llops = lustre.LustreOperations()
        self.assertEqual(llops.list_quota(['mylfs']), quota_result)
        mock_lctl.assert_called_with(['list_param', 'qmt.mylfs-*.*.glb-*'])
        mock_lctl.side_effect = lustre.LustreOperationError
        self.assertRaises(lustre.LustreOperationError, llops.list_quota, ['mylfs'])
        mock_execute.return_value = (0, "ok")
        self.assertEqual(llops.list_quota(['mylfs']), quota_result)
        mock_execute.assert_called_with(['ls', '/var/cache/lustre/qmt.mylfs-*.*.glb-*'])


    @mock.patch('vsc.filesystem.lustre.LustreOperations.get_project_id')
    @mock.patch('vsc.filesystem.posix.PosixOperations._execute')
    @mock.patch('vsc.filesystem.lustre.LustreOperations._sanity_check')
    def test__set_new_project_id(self, mock_sanity_check, mock_execute, mock_get_projectid):
        """ Test setting project id on fileset folder """
        test_path = os.path.join("/lustre", "scratch", "gent", "vsc406", "vsc40605")
        mock_sanity_check.side_effect = lambda x: x
        mock_execute.return_value = (0, '')
        mock_get_projectid.return_value = False
        llops = lustre.LustreOperations()

        llops._set_new_project_id(test_path, 4)
        (args, _) = mock_execute.call_args
        self.assertEqual(args[0], ['/usr/bin/lfs', 'project', '-p', 4, '-r', '-s', '/lustre/scratch/gent/vsc406/vsc40605'])

    @mock.patch('glob.glob')
    @mock.patch('vsc.filesystem.posix.PosixOperations.what_filesystem')
    def test__get_fshint_for_path(self, mock_what_filesystem, mock_glob):
        """ Test getting the search paths and pjid mapping for a file system """
        mock_glob.side_effect = [['/lustre/mylfs/gent'], ['/lustre/mylfs/gent/projects/000', '/lustre/mylfs/gent/projects/001']]
        mock_what_filesystem.return_value = ['lustre', '/lustre/mylfs', 452646254, '10.141.21.204@tcp:/mylfs']
        llops = lustre.LustreOperations()
        llops.set_default_mapping(LustreVscTier1cScratchFs)
        fsclass = llops._get_fshint_for_path('/lustre/mylfs/mypath')
        self.assertEqual(fsclass.get_search_paths(), ['/lustre/mylfs/gent', '/lustre/mylfs/gent/projects/000', '/lustre/mylfs/gent/projects/001'])
        self.assertEqual(fsclass.pjid_from_name('pj00002'), '900002')


    @mock.patch('glob.glob')
    @mock.patch('vsc.filesystem.posix.PosixOperations.what_filesystem')
    @mock.patch('vsc.filesystem.posix.PosixOperations._execute')
    @mock.patch('vsc.filesystem.lustre.LustreOperations._sanity_check')
    def test__list_filesets(self, mock_sanity_check, mock_execute, mock_what_filesystem, mock_glob):
        """ Test listing all filesets for a specified file system"""
        mock_sanity_check.side_effect = lambda x: x
        mock_glob.side_effect = [['/lustre/mylfs/gent'], ['/lustre/mylfs/gent/projects/000', '/lustre/mylfs/gent/projects/001']]
        mock_what_filesystem.return_value = ['lustre', '/lustre/mylfs', 452646254, '10.141.21.204@tcp:/mylfs']
        mock_execute.side_effect = [
            (0, '''    0 P /lustre/mylfs/gent/training
    0 P /lustre/mylfs/gent/stats
    0 P /lustre/mylfs/gent/tmp
    0 P /lustre/mylfs/gent/vo
    5 P /lustre/mylfs/gent/archive
'''),
            (0, '''900006 P /lustre/mylfs/gent/projects/000/pj00006
900002 P /lustre/mylfs/gent/projects/000/pj00002
900004 P /lustre/mylfs/gent/projects/000/pj00004
'''),
            (0,'''900110 P /lustre/mylfs/gent/projects/001/pj00110
900112 P /lustre/mylfs/gent/projects/001/pj00112
900111 P /lustre/mylfs/gent/projects/001/pj00111
''')]
        llops = lustre.LustreOperations()
        llops.set_default_mapping(LustreVscTier1cScratchFs)
        fsystems = {'mylfs': {'defaultMountPoint': '/lustre/mylfs', 'location': '10.141.21.204@tcp'}}
        filesets = llops._list_filesets(fsystems['mylfs'])
        mock_execute.assert_called_with(['/usr/bin/lfs', 'project', '/lustre/mylfs/gent/projects/001'], False)
        self.assertEqual(filesets, LUSTRE_FILESET_TREE['mylfs'])


    @mock.patch('vsc.filesystem.lustre.LustreOperations._list_filesets')
    @mock.patch('vsc.filesystem.lustre.LustreOperations.list_filesystems')
    def test_list_filesets(self, mock_list_filesystems, mock__list_filesets):
        """ Test listing or updating all filesets """
        mock_list_filesystems.return_value = {
            'mylfs': {'defaultMountPoint': '/lustre/mylfs', 'location': '10.141.21.204@tcp'}}
        mock__list_filesets.return_value = LUSTRE_FILESET_TREE['mylfs']

        llops = lustre.LustreOperations()
        llops.set_default_mapping(LustreVscTier1cScratchFs)
        self.assertEqual(llops.list_filesets(), LUSTRE_FILESET_TREE)
        self.assertEqual(llops.filesets, LUSTRE_FILESET_TREE)
        mock__list_filesets.assert_called_with({'defaultMountPoint': '/lustre/mylfs', 'location': '10.141.21.204@tcp'})

    @mock.patch('vsc.filesystem.lustre.LustreOperations._set_quota')
    @mock.patch('vsc.filesystem.posix.PosixOperations._execute')
    @mock.patch('vsc.filesystem.posix.PosixOperations.make_dir')
    @mock.patch('vsc.filesystem.posix.PosixOperations.what_filesystem')
    @mock.patch('vsc.filesystem.lustre.LustreOperations.list_filesets')
    @mock.patch('vsc.filesystem.posix.PosixOperations._sanity_check')
    @mock.patch('vsc.filesystem.posix.PosixOperations.exists')
    def test_make_fileset(self, mock_exists, mock_sanity_check, mock_list_filesets,
            mock_what_filesystem, mock_make_dir, mock_execute, mock_set_quota):
        """ Test making a new fileset on a specified file system """
        mock_sanity_check.side_effect = lambda x: x
        mock_execute.return_value = (0, "")
        mock_list_filesets.return_value = LUSTRE_FILESET_TREE
        mock_what_filesystem.return_value = ['lustre', '/lustre/mylfs', 452646254, '10.141.21.204@tcp:/mylfs']
        mock_make_dir.return_value = True
        mock_exists.return_value = True
        mock_set_quota.return_value = True
        llops = lustre.LustreOperations()
        llops.set_default_mapping(LustreVscTier1cScratchFs)
        self.assertRaises(lustre.LustreOperationError, llops.make_fileset, '/lustre/mylfs/gent/projects/000/pj00002', 'pj00002')
        mock_exists.return_value = False
        self.assertRaises(lustre.LustreOperationError, llops.make_fileset, '/lustre/mylfs/gent/projects/000/pj00002', 'pj00002')
        mock_exists.side_effect = [False, True]
        self.assertRaises(lustre.LustreOperationError, llops.make_fileset, '/lustre/mylfs/gent/projects/000/pj00002', 'pj00002')
        mock_exists.side_effect = [False, True]
        mock_execute.side_effect = [(0, '    0 P /lustre/mylfs/gent/projects/000/pj00003'), (0, "")]
        llops.make_fileset('/lustre/mylfs/gent/projects/000/pj00003', 'pj00003')
        mock_set_quota.assert_called_with(who='900003', obj='/lustre/mylfs/gent/projects/000/pj00003', typ=lustre.Typ2Opt.project, soft=1048576, hard=1048576, inode_soft=1048576, inode_hard=1048576)

        mock_make_dir.assert_called_with('/lustre/mylfs/gent/projects/000/pj00003')
        mock_execute.assert_called_with(['/usr/bin/lfs', 'project', '-p', '900003', '-r', '-s', '/lustre/mylfs/gent/projects/000/pj00003'], True)
