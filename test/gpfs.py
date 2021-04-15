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
Tests for the gpfs library.

@author: Andy Georges (Ghent University)
"""
from __future__ import print_function

import os
import mock
import vsc.filesystem.gpfs as gpfs

from vsc.install.testing import TestCase


class ToolsTest(TestCase):
    """
    Tests for various auxilliary functions in the gpfs lib.
    """

    def test_split_output_lines(self):
        """
        Check the split of lines without any colon at the end.
        """

        test_lines = [
            "this:is:the:header:line",
            "and:here:is:line:1",
            "and:here:is:line:2",
            "and:here:is:line:3:",
            "and:here:is:line:4",
        ]

        split_lines = gpfs.split_output_lines(test_lines)

        self.assertEqual(list(map(len, split_lines)), [5,5,5,5,5])  # all lines have the same number of fields

    def test_split_output_lines_with_header_colon(self):
        """
        Check the split of lines without any colon at the end.
        """

        test_lines = [
            "this:is:the:header:line:",
            "and:here:is:line:1",
            "and:here:is:line:2",
            "and:here:is:line:3",
            "and:here:is:line:4",
        ]

        split_lines = gpfs.split_output_lines(test_lines)

        self.assertEqual(list(map(len, split_lines)), [6, 5, 5, 5, 5])   # all lines have the same number of fields

    def test_split_output_lines_with_header_colon_colons(self):
        """
        Check the split of lines without any colon at the end.
        """

        test_lines = [
            "this:is:the:header:line:",
            "and:here:is:line:1:",
            "and:here:is:line:2:",
            "and:here:is:line:3:",
            "and:here:is:line:4:",
        ]

        split_lines = gpfs.split_output_lines(test_lines)

        self.assertEqual(list(map(len, split_lines)), [6, 6, 6, 6, 6])   # all lines have the same number of fields

    def test_split_output_lines_without_header_colon_colons(self):
        """
        Check the split of lines without any colon at the end.
        """

        test_lines = [
            "this:is:the:header:line",
            "and:here:is:line:1:",
            "and:here:is:line:2:",
            "and:here:is:line:3:",
            "and:here:is:line:4:",
        ]

        split_lines = gpfs.split_output_lines(test_lines)

        self.assertEqual(list(map(len, split_lines)), [5, 5, 5, 5, 5])   # all lines have the same number of fields

    @mock.patch('vsc.filesystem.gpfs.GpfsOperations._execute')
    def test_list_snapshots(self, mock_exec):
        mock_exec.return_value = (1, 'mocked!')
        gpfsi = gpfs.GpfsOperations()
        self.assertRaises(gpfs.GpfsOperationError, gpfsi.list_snapshots, 'fstest')
        mock_exec.assert_called_once_with('mmlssnapshot', ['fstest', '-Y'])
        lssnapshot_output = "mmlssnapshot::HEADER:version:reserved:reserved:filesystemName:directory:snapID:status:created:quotas:data:metadata:fileset:snapType: \nmmlssnapshot::0:1:::fstest:autumn_20151012:1517:Valid:Mon Oct 12 14%3A24%3A41 2015::0:0:::\nmmlssnapshot::0:1:::fstest:okt_20151028:1518:Valid:Wed Oct 28 11%3A34%3A06 2015::0:0:::"
        mock_exec.return_value = (0, lssnapshot_output)
        self.assertEqual(gpfsi.list_snapshots('fstest'), ['autumn_20151012', 'okt_20151028'])

    @mock.patch('vsc.filesystem.gpfs.GpfsOperations._execute')
    @mock.patch('vsc.filesystem.gpfs.GpfsOperations.list_snapshots')
    @mock.patch('vsc.filesystem.gpfs.GpfsOperations.list_filesets')
    def test_create_filesystem_snapshot(self, mock_filesets, mock_list, mock_exec):
        mock_list.return_value = ['autumn_20151012', 'okt_20151028']
        mock_filesets.return_value = {
            'fstest': {
                '0': {'filesetName': 'foo'},
                '1': {'filesetName': 'bar'},
                '2': {'filesetName': 'fs1'},
                '3': {'filesetName': 'fs2'},
            },
        }
        gpfsi = gpfs.GpfsOperations()
        self.assertEqual(gpfsi.create_filesystem_snapshot('fstest', 'okt_20151028'), 0)
        mock_exec.return_value = (1, 'mocked!')
        self.assertRaises(gpfs.GpfsOperationError, gpfsi.create_filesystem_snapshot, 'fstest', '@backup')
        mock_exec.assert_called_once_with('mmcrsnapshot', ['fstest', '@backup'], True)
        mock_exec.return_value = (0, 'mocked!')
        self.assertTrue(gpfsi.create_filesystem_snapshot('fstest', 'backup'))

        mock_exec.reset_mock()
        self.assertTrue(gpfsi.create_filesystem_snapshot('fstest', 'backup', filesets=['fs1', 'bar']))
        mock_exec.assert_called_once_with('mmcrsnapshot', ['fstest', 'backup', '-j', 'fs1,bar'], True)

    @mock.patch('vsc.filesystem.gpfs.GpfsOperations._execute')
    @mock.patch('vsc.filesystem.gpfs.GpfsOperations.list_snapshots')
    def test_delete_filesystem_snapshot(self, mock_list, mock_exec):
        mock_list.return_value = ['autumn_20151012', 'okt_20151028']
        gpfsi = gpfs.GpfsOperations()
        self.assertEqual(gpfsi.delete_filesystem_snapshot('fstest', 'backup'), 0)
        mock_exec.return_value = (1, 'mocked!')
        self.assertRaises(gpfs.GpfsOperationError, gpfsi.delete_filesystem_snapshot, 'fstest', 'autumn_20151012')
        mock_exec.assert_called_once_with('mmdelsnapshot', ['fstest', 'autumn_20151012'], True)
        mock_exec.return_value = (0, 'mocked!')
        self.assertTrue(gpfsi.delete_filesystem_snapshot('fstest', 'okt_20151028'))

    @mock.patch('vsc.filesystem.posix.PosixOperations._execute')
    @mock.patch('vsc.filesystem.gpfs.GpfsOperations._sanity_check')
    @mock.patch('vsc.filesystem.gpfs.GpfsOperations.exists')
    def test__set_grace(self, mock_exists, mock_sanity_check, mock_execute):
        """Test that the command passes is properly constructed so it can be executed by execve."""

        test_path = os.path.join("user", "scratchdelcatty", "gent", "vsc400", "vsc40075")
        mock_sanity_check.return_value = test_path
        mock_exists.return_value = True

        gpfsi = gpfs.GpfsOperations()
        mock_execute.return_value = (0, "")

        gpfsi._set_grace(test_path, 'user', 7 * 24 * 60 * 60)

        (args, _) = mock_execute.call_args
        self.assertTrue(isinstance(args[0], list))
        self.assertTrue(all([isinstance(a, str) for a in args[0]]))
        self.assertTrue(all([len(s.split(" ")) == 1 for s in args[0]]))

    @mock.patch('vsc.filesystem.posix.PosixOperations._execute')
    @mock.patch('vsc.filesystem.gpfs.GpfsOperations._sanity_check')
    @mock.patch('vsc.filesystem.gpfs.GpfsOperations.exists')
    def test__set_quota(self, mock_exists, mock_sanity_check, mock_execute):
        """Test that the command passed is properly constructed so it can be executed by execve."""

        test_path = os.path.join("user", "scratchdelcatty", "gent", "vsc400", "vsc40075")
        mock_sanity_check.return_value = test_path
        mock_exists.return_value = True

        gpfsi = gpfs.GpfsOperations()
        mock_execute.return_value = (0, "")

        gpfsi._set_quota(1024, 2540075, test_path)

        (args, _) = mock_execute.call_args
        self.assertTrue(isinstance(args[0], list))
        self.assertTrue(all([isinstance(a, str) for a in args[0]]))
        self.assertTrue(all([len(s.split(" ")) == 1 for s in args[0]]))

        gpfsi._set_quota(1024, 2540075, test_path, inode_soft=1000)
        (args, _) = mock_execute.call_args
        self.assertTrue("-S 1000" in ' '.join(args[0]))
        self.assertTrue("-H 1050" in ' '.join(args[0]))

        gpfsi._set_quota(1024, 2540075, test_path, inode_soft=2000, inode_hard=2123)
        (args, _) = mock_execute.call_args
        self.assertTrue("-S 2000" in ' '.join(args[0]))
        self.assertTrue("-H 2123" in ' '.join(args[0]))

    @mock.patch('vsc.filesystem.gpfs.GpfsOperations._execute')
    def test_get_mmhealth(self, mock_exec):

        gpfsi = gpfs.GpfsOperations()

        mmhealth_output = """mmhealth:State:HEADER:version:reserved:reserved:node:component:entityname:entitytype:status:
mmhealth:Event:HEADER:version:reserved:reserved:node:component:entityname:entitytype:event:arguments:
mmhealth:State:0:1:::storage2206.shuppet.gent.vsc:NODE:storage2206.shuppet.gent.vsc:NODE:FAILED:
mmhealth:State:0:1:::storage2206.shuppet.gent.vsc:GPFS:storage2206.shuppet.gent.vsc:NODE:FAILED:
mmhealth:Event:0:1:::storage2206.shuppet.gent.vsc:GPFS:storage2206.shuppet.gent.vsc:NODE:gpfs_down::
mmhealth:Event:0:1:::storage2206.shuppet.gent.vsc:GPFS:storage2206.shuppet.gent.vsc:NODE:quorum_down::
mmhealth:Event:0:1:::storage2206.shuppet.gent.vsc:GPFS:storage2206.shuppet.gent.vsc:NODE:gpfsport_up:1191,1191:
mmhealth:State:0:1:::storage2206.shuppet.gent.vsc:NETWORK:storage2206.shuppet.gent.vsc:NODE:HEALTHY:
mmhealth:Event:0:1:::storage2206.shuppet.gent.vsc:NETWORK:storage2206.shuppet.gent.vsc:NODE:network_ips_up::
mmhealth:State:0:1:::storage2206.shuppet.gent.vsc:FILESYSTEM:storage2206.shuppet.gent.vsc:NODE:DEPEND:
mmhealth:State:0:1:::storage2206.shuppet.gent.vsc:DISK:storage2206.shuppet.gent.vsc:NODE:HEALTHY:
mmhealth:State:0:1:::storage2206.shuppet.gent.vsc:CES:storage2206.shuppet.gent.vsc:NODE:DEPEND:
mmhealth:State:0:1:::storage2206.shuppet.gent.vsc:HADOOPCONNECTOR:storage2206.shuppet.gent.vsc:NODE:DEGRADED:"""

        mock_exec.return_value = (0, mmhealth_output)
        res = gpfsi.get_mmhealth_state()
        expected_res = {
            'CES_storage2206.shuppet.gent.vsc': 'DEPEND',
            'DISK_storage2206.shuppet.gent.vsc': 'HEALTHY',
            'FILESYSTEM_storage2206.shuppet.gent.vsc': 'DEPEND',
            'GPFS_storage2206.shuppet.gent.vsc': 'FAILED',
            'HADOOPCONNECTOR_storage2206.shuppet.gent.vsc': 'DEGRADED',
            'NETWORK_storage2206.shuppet.gent.vsc': 'HEALTHY',
            'NODE_storage2206.shuppet.gent.vsc': 'FAILED'}

        mock_exec.assert_called_once_with('mmhealth', ['node', 'show', '-Y'])
        self.assertEqual(res, expected_res)

        mmhealth_more_out = """mmhealth:State:HEADER:version:reserved:reserved:node:component:entityname:entitytype:status:laststatuschange:
mmhealth:Event:HEADER:version:reserved:reserved:node:component:entityname:entitytype:event:arguments:activesince:identifier:ishidden:
mmhealth:State:0:1:::nsd03.gastly.data:NODE:nsd03.gastly.data:NODE:HEALTHY:2017-08-11 14%3A47%3A09.717896 CEST:
mmhealth:State:0:1:::nsd03.gastly.data:DISK:nsd03.gastly.data:NODE:HEALTHY:2017-08-11 14%3A47%3A09.710337 CEST:
mmhealth:State:0:1:::nsd03.gastly.data:DISK:f1v07e0p0_S15o1:NSD:HEALTHY:2017-08-08 15%3A47%3A56.794469 CEST:
mmhealth:State:0:1:::nsd03.gastly.data:DISK:f1v01e0p0_D25o1:NSD:HEALTHY:2017-08-08 15%3A47%3A56.799986 CEST:
mmhealth:State:0:1:::nsd03.gastly.data:DISK:f1v05e0p0_H21o0:NSD:HEALTHY:2017-08-08 15%3A47%3A56.803198 CEST:
mmhealth:State:0:1:::nsd03.gastly.data:DISK:f1v01e0p0_D09o1:NSD:HEALTHY:2017-08-08 15%3A47%3A56.803266 CEST:
mmhealth:State:0:1:::nsd03.gastly.data:GPFS:nsd03.gastly.data:NODE:HEALTHY:2017-08-11 14%3A47%3A09.717276 CEST:
mmhealth:State:0:1:::nsd03.gastly.data:NETWORK:nsd03.gastly.data:NODE:HEALTHY:2017-08-03 15%3A02%3A47.508757 CEST:
mmhealth:State:0:1:::nsd03.gastly.data:NETWORK:bond0:NIC:HEALTHY:2017-08-03 15%3A02%3A47.508897 CEST:
mmhealth:State:0:1:::nsd03.gastly.data:NETWORK:ib0:NIC:HEALTHY:2017-08-03 15%3A02%3A47.508812 CEST:
mmhealth:State:0:1:::nsd03.gastly.data:NETWORK:em1:NIC:HEALTHY:2017-08-03 15%3A02%3A47.508938 CEST:
mmhealth:State:0:1:::nsd03.gastly.data:NETWORK:mlx5_0:NIC:HEALTHY:2017-08-03 15%3A07%3A47.822473 CEST:
mmhealth:State:0:1:::nsd03.gastly.data:NETWORK:mlx5_1:NIC:HEALTHY:2017-08-03 15%3A07%3A47.822373 CEST:
mmhealth:State:0:1:::nsd03.gastly.data:NETWORK:ib1:NIC:HEALTHY:2017-08-03 15%3A02%3A47.508856 CEST:
mmhealth:State:0:1:::nsd03.gastly.data:FILESYSTEM:nsd03.gastly.data:NODE:DEGRADED:2017-08-11 16%3A32%3A11.509018 CEST:
mmhealth:State:0:1:::nsd03.gastly.data:FILESYSTEM:kyukonpilot:FILESYSTEM:HEALTHY:2017-08-11 12%3A02%3A41.096460 CEST:
mmhealth:State:0:1:::nsd03.gastly.data:FILESYSTEM:kyukonhome:FILESYSTEM:DEGRADED:2017-08-11 16%3A32%3A11.509173 CEST:
mmhealth:Event:0:1:::nsd03.gastly.data:FILESYSTEM:kyukonhome:FILESYSTEM:unmounted_fs_check:kyukonhome:2017-08-11 16%3A32%3A11.497114 CEST:kyukonhome:no:
mmhealth:State:0:1:::nsd03.gastly.data:FILESYSTEM:kyukonscratch:FILESYSTEM:HEALTHY:2017-08-08 15%3A47%3A56.804124 CEST:
mmhealth:State:0:1:::nsd03.gastly.data:FILESYSTEM:kyukondata:FILESYSTEM:HEALTHY:2017-08-11 12%3A02%3A41.096584 CEST:
mmhealth:State:0:1:::test01.gastly.data:CES:test01.gastly.data:NODE:HEALTHY:2017-08-11 16%3A28%3A45.603602 CEST:
mmhealth:State:0:1:::test01.gastly.data:OBJECT:test01.gastly.data:NODE:DISABLED:2017-08-11 12%3A02%3A59.957044 CEST:"""

        expected_res = {
            'CES_test01.gastly.data': 'HEALTHY',
            'DISK_f1v01e0p0_D09o1': 'HEALTHY',
            'DISK_f1v01e0p0_D25o1': 'HEALTHY',
            'DISK_f1v05e0p0_H21o0': 'HEALTHY',
            'DISK_f1v07e0p0_S15o1': 'HEALTHY',
            'DISK_nsd03.gastly.data': 'HEALTHY',
            'FILESYSTEM_kyukondata': 'HEALTHY',
            'FILESYSTEM_kyukonhome': 'DEGRADED',
            'FILESYSTEM_kyukonpilot': 'HEALTHY',
            'FILESYSTEM_kyukonscratch': 'HEALTHY',
            'FILESYSTEM_nsd03.gastly.data': 'DEGRADED',
            'GPFS_nsd03.gastly.data': 'HEALTHY',
            'NETWORK_bond0': 'HEALTHY',
            'NETWORK_em1': 'HEALTHY',
            'NETWORK_ib0': 'HEALTHY',
            'NETWORK_ib1': 'HEALTHY',
            'NETWORK_mlx5_0': 'HEALTHY',
            'NETWORK_mlx5_1': 'HEALTHY',
            'NETWORK_nsd03.gastly.data': 'HEALTHY',
            'NODE_nsd03.gastly.data': 'HEALTHY',
            'OBJECT_test01.gastly.data': 'DISABLED'
        }
        mock_exec.return_value = (0, mmhealth_more_out)
        res = gpfsi.get_mmhealth_state()
        print(res)
        self.assertEqual(res, expected_res)
