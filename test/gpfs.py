# -*- coding: latin-1 -*-
#
# Copyright 2015-2016 Ghent University
#
# This file is part of vsc-filesystems,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
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
import subprocess
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

        test_lines = "\n".join([
            "this:is:the:header:line"
            "and:here:is:line:1"
            "and:here:is:line:2"
            "and:here:is:line:3"
            "and:here:is:line:4"
        ])

        split_lines = gpfs.split_output_lines(test_lines)

        self.assertTrue(len(set(map(len, split_lines))) == 1)  # all lines have the same number of fields

    def test_split_output_lines_with_header_colon(self):
        """
        Check the split of lines without any colon at the end.
        """

        test_lines = "\n".join([
            "this:is:the:header:line:"
            "and:here:is:line:1"
            "and:here:is:line:2"
            "and:here:is:line:3"
            "and:here:is:line:4"
        ])

        split_lines = gpfs.split_output_lines(test_lines)

        self.assertTrue(len(set(map(len, split_lines))) == 1)  # all lines have the same number of fields

    def test_split_output_lines_with_header_colon_colons(self):
        """
        Check the split of lines without any colon at the end.
        """

        test_lines = "\n".join([
            "this:is:the:header:line:"
            "and:here:is:line:1:"
            "and:here:is:line:2:"
            "and:here:is:line:3:"
            "and:here:is:line:4:"
        ])

        split_lines = gpfs.split_output_lines(test_lines)

        self.assertTrue(len(set(map(len, split_lines))) == 1)  # all lines have the same number of fields

    def test_split_output_lines_without_header_colon_colons(self):
        """
        Check the split of lines without any colon at the end.
        """

        test_lines = "\n".join([
            "this:is:the:header:line"
            "and:here:is:line:1:"
            "and:here:is:line:2:"
            "and:here:is:line:3:"
            "and:here:is:line:4:"
        ])

        split_lines = gpfs.split_output_lines(test_lines)

        self.assertTrue(len(set(map(len, split_lines))) == 1)  # all lines have the same number of fields


    @mock.patch('vsc.filesystem.gpfs.GpfsOperations._execute')
    def test_list_snapshots(self, mock_exec):
        mock_exec.return_value=(1, 'mocked!')
        gpfsi = gpfs.GpfsOperations()
        self.assertRaises(gpfs.GpfsOperationError, gpfsi.list_snapshots, 'fstest')
        mock_exec.assert_called_once_with('mmlssnapshot', ['fstest', '-Y'])
        lssnapshot_output = "mmlssnapshot::HEADER:version:reserved:reserved:filesystemName:directory:snapID:status:created:quotas:data:metadata:fileset:snapType: \nmmlssnapshot::0:1:::fstest:autumn_20151012:1517:Valid:Mon Oct 12 14%3A24%3A41 2015::0:0:::\nmmlssnapshot::0:1:::fstest:okt_20151028:1518:Valid:Wed Oct 28 11%3A34%3A06 2015::0:0:::"
        mock_exec.return_value=(0, lssnapshot_output)
        self.assertEqual(gpfsi.list_snapshots('fstest'), ['autumn_20151012', 'okt_20151028'])

    @mock.patch('vsc.filesystem.gpfs.GpfsOperations._execute')
    @mock.patch('vsc.filesystem.gpfs.GpfsOperations.list_snapshots')
    def test_create_filesystem_snapshot(self, mock_list, mock_exec):
        mock_list.return_value = ['autumn_20151012', 'okt_20151028']
        gpfsi = gpfs.GpfsOperations()
        self.assertEqual(gpfsi.create_filesystem_snapshot('fstest', 'okt_20151028'),0)
        mock_exec.return_value=(1, 'mocked!')
        self.assertRaises(gpfs.GpfsOperationError, gpfsi.create_filesystem_snapshot, 'fstest', '@backup')
        mock_exec.assert_called_once_with('mmcrsnapshot', ['fstest', '@backup'], True)
        mock_exec.return_value=(0, 'mocked!')
        self.assertTrue(gpfsi.create_filesystem_snapshot('fstest', 'backup'))

    @mock.patch('vsc.filesystem.gpfs.GpfsOperations._execute')
    @mock.patch('vsc.filesystem.gpfs.GpfsOperations.list_snapshots')
    def test_delete_filesystem_snapshot(self, mock_list, mock_exec):
        mock_list.return_value = ['autumn_20151012', 'okt_20151028']
        gpfsi = gpfs.GpfsOperations()
        self.assertEqual(gpfsi.delete_filesystem_snapshot('fstest', 'backup'),0)
        mock_exec.return_value=(1, 'mocked!')
        self.assertRaises(gpfs.GpfsOperationError, gpfsi.delete_filesystem_snapshot, 'fstest', 'autumn_20151012')
        mock_exec.assert_called_once_with('mmdelsnapshot', ['fstest', 'autumn_20151012'], True)
        mock_exec.return_value=(0, 'mocked!')
        self.assertTrue(gpfsi.delete_filesystem_snapshot('fstest', 'okt_20151028'))

