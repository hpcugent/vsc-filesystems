#
# Copyright 2018-2020 Ghent University
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
Unit tests for vsc.filesystems.quota.*

@author: Andy Georges (Ghent University)
"""
from vsc.install.testing import TestCase
from vsc.filesystem.quota.entities import QuotaUser


class QuotaEntityTest(TestCase):
    """Tests for vsc.filesystem.quota.entities"""

    def setUp(self):
        """Set things up for running tests."""
        super(QuotaEntityTest, self).setUp()

    def test_quota_update(self):

        q = QuotaUser("mystorage", "myfilesystem", "vsc40075")
        q.update(fileset="myfileset_01", used=10, soft=100, hard=101, doubt=1, expired=(False, None), 
               files_used=2, files_soft=4, files_hard=6, files_doubt=1, files_expired=(False, None), 
               timestamp=20180901)
 
        q.update(fileset="myfileset_02", used=20, soft=100, hard=101, doubt=1, expired=(False, None), 
               files_used=2, files_soft=4, files_hard=6, files_doubt=1, files_expired=(False, None), 
               timestamp=20180901)

        self.assertTrue(not q.exceed)
        self.assertTrue(len(q.quota_map) == 2)


