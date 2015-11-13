##
# Copyright 2014-2015 Ghent University
#
# This file is part of vsc-filesystems,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
##
"""
Unit tests for vsc.filesystems.posix

@author: Kenneth Hoste (Ghent University)
"""
import os
from vsc.install.testing import TestCase

from vsc.filesystem.posix import PosixOperations


class PosixTest(TestCase):
    """Tests for vsc.filesystems.posix"""

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
