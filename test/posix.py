#
# Copyright 2014-2019 Ghent University
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
Unit tests for vsc.filesystems.posix

@author: Kenneth Hoste (Ghent University)
"""
import mock
import os
import tempfile
from collections import namedtuple

from vsc.install.testing import TestCase

from vsc.filesystem.posix import PosixOperations


class PosixTest(TestCase):
    """Tests for vsc.filesystem.posix"""

    def setUp(self):
        """Set things up for running tests."""
        super(PosixTest, self).setUp()

        self.po = PosixOperations()
        # mock the sanity check, to ease testing
        self.po._sanity_check = lambda x: x

    def test_is_dir(self):
        """Tests for is_dir method."""
        self.assertEqual(self.po.is_dir(os.environ['HOME']), True)
        self.assertEqual(self.po.is_dir('/no/such/dir'), False)

    @mock.patch('vsc.filesystem.open')
    @mock.patch('vsc.filesystem.posix.os.path.exists')
    @mock.patch('vsc.filesystem.posix.os.path.islink')
    @mock.patch('vsc.filesystem.posix.os.path.realpath')
    @mock.patch('vsc.filesystem.posix.os.unlink')
    def test__deploy_dot_file(self, mock_unlink, mock_realpath, mock_islink, mock_exists, mock_open):
        """Test for the _deploy_dot_file method"""

        (handle, path) = tempfile.mkstemp()
        mock_realpath.return_value = path

        # if branch
        mock_exists.return_value = True
        self.po._deploy_dot_file(path, "test_tempfile", "vsc40075", ["huppel"])

        mock_islink.assert_not_called()
        mock_unlink.assert_not_called()
        mock_realpath.assert_not_called()

        # else + if branch
        mock_exists.reset_mock()
        mock_islink.reset_mock()
        mock_unlink.reset_mock()
        mock_open.reset_mock()
        mock_realpath.reset_mock()
        mock_exists.return_value = False
        mock_islink.return_value = True
        self.po._deploy_dot_file(path, "test_tempfile", "vsc40075", ["huppel"])

        mock_unlink.assert_called_with(path)
        mock_realpath.assert_called_with(path)

        # else + else branch
        mock_exists.reset_mock()
        mock_islink.reset_mock()
        mock_unlink.reset_mock()
        mock_open.reset_mock()
        mock_realpath.reset_mock()
        mock_exists.return_value = False
        mock_islink.return_value = False
        self.po._deploy_dot_file(path, "test_tempfile", "vsc40075", ["huppel"])

        mock_realpath.assert_not_called()
        mock_unlink.assert_not_called()

        os.close(handle)

    @mock.patch('vsc.filesystem.posix.os.stat')
    @mock.patch('vsc.filesystem.posix.os.makedirs')
    @mock.patch('vsc.filesystem.posix.os.chown')
    @mock.patch('vsc.filesystem.posix.os.chmod')
    def test_create_stat_dir_new(self, mock_chmod, mock_chown, mock_makedirs, mock_os_stat):
        """
        Test to see what happens if the dir already exists
        """
        mock_os_stat.side_effect = OSError('dir not found')
        mock_chmod.result_value = True
        mock_chown.result_value = True
        mock_makedirs.result_value = None

        test_uid = 2048
        test_gid = 4096
        test_path = '/tmp/test'
        test_permissions = 0o711

        self.po.create_stat_directory(test_path, test_permissions, test_uid, test_gid, False)

        mock_os_stat.assert_called_with(test_path)
        mock_makedirs.assert_called_with(test_path)
        mock_chmod.assert_called_with(test_path, test_permissions)
        mock_chown.assert_called_with(test_path, test_uid, test_gid)

    @mock.patch('vsc.filesystem.posix.os.stat')
    @mock.patch('vsc.filesystem.posix.os.makedirs')
    @mock.patch('vsc.filesystem.posix.os.chown')
    @mock.patch('vsc.filesystem.posix.os.chmod')
    def test_create_stat_dir_existing_no_override_same_id(self, mock_chmod, mock_chown, mock_makedirs, mock_os_stat):
        """
        Test to see what happens if the dir already exists
        """

        test_uid = 2048
        test_gid = 4096
        test_path = '/tmp/test'
        test_permissions = 0o711

        Statinfo = namedtuple("Statinfo", ["st_uid", "st_gid"])
        mock_os_stat.result_value = Statinfo(test_uid, test_gid)

        self.po.create_stat_directory(test_path, test_permissions, test_uid, test_gid, False)

        mock_os_stat.assert_called_with(test_path)
        mock_makedirs.assert_not_called
        mock_chmod.assert_not_called

    @mock.patch('vsc.filesystem.posix.os.stat')
    @mock.patch('vsc.filesystem.posix.os.makedirs')
    @mock.patch('vsc.filesystem.posix.os.chown')
    @mock.patch('vsc.filesystem.posix.os.chmod')
    def test_create_stat_dir_existing_no_override_diff_uid(self, mock_chmod, mock_chown, mock_makedirs, mock_os_stat):

        """
        Test to see what happens if the dir already exists
        """

        test_uid = 2048
        test_gid = 4096
        test_path = '/tmp/test'
        test_permissions = 0o711

        Statinfo = namedtuple("Statinfo", ["st_uid", "st_gid"])
        mock_os_stat.result_value = Statinfo(test_uid + 1, test_gid)

        self.po.create_stat_directory(test_path, test_permissions, test_uid, test_gid, False)

        mock_os_stat.assert_called_with(test_path)
        mock_makedirs.assert_not_called
        mock_chown.assert_called_with(test_path, test_uid, test_gid)

    @mock.patch('vsc.filesystem.posix.os.stat')
    @mock.patch('vsc.filesystem.posix.os.makedirs')
    @mock.patch('vsc.filesystem.posix.os.chown')
    @mock.patch('vsc.filesystem.posix.os.chmod')
    def test_create_stat_dir_existing_no_override_diff_gid(self, mock_chmod, mock_chown, mock_makedirs, mock_os_stat):
        """
        Test to see what happens if the dir already exists
        """

        test_uid = 2048
        test_gid = 4096
        test_path = '/tmp/test'
        test_permissions = 0o711

        Statinfo = namedtuple("Statinfo", ["st_uid", "st_gid"])
        mock_os_stat.result_value = Statinfo(test_uid, test_gid+1)

        self.po.create_stat_directory(test_path, test_permissions, test_uid, test_gid, False)

        mock_os_stat.assert_called_with(test_path)
        mock_makedirs.assert_not_called
        mock_chown.assert_called_with(test_path, test_uid, test_gid)

    @mock.patch('vsc.filesystem.posix.os.stat')
    @mock.patch('stat.S_IMODE')
    @mock.patch('vsc.filesystem.posix.os.makedirs')
    @mock.patch('vsc.filesystem.posix.os.chown')
    @mock.patch('vsc.filesystem.posix.os.chmod')
    def test_create_stat_dir_existing_override(self, mock_chmod, mock_chown, mock_makedirs, mock_stat_s_imode, mock_os_stat):
        """
        Test to see what happens if the dir already exists
        """

        test_uid = 2048
        test_gid = 4096
        test_path = '/tmp/test'
        test_permissions = 0o711

        Statinfo = namedtuple("Statinfo", ["st_uid", "st_gid"])
        mock_os_stat.result_value = Statinfo(test_uid, test_gid)
        mock_stat_s_imode.return_value = 0o755

        self.po.create_stat_directory(test_path, test_permissions, test_uid, test_gid, True)

        mock_os_stat.assert_called_with(test_path)
        mock_makedirs.assert_not_called
        mock_chmod.assert_called_with(test_path, test_permissions)
