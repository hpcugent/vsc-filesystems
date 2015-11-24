# -*- coding: latin-1 -*-
#
# Copyright 2015-2015 Ghent University
#
# This file is part of vsc-filesystems,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
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
