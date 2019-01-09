# -*- coding: latin-1 -*-
#
# Copyright 2013-2019 Ghent University
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
"""Various classes that can represent quota information for the various
devices an entity (user, vo, ...) uses on the VSC storage.

@author: Andy Georges (Ghent University)
"""
from collections import namedtuple


QuotaInformation = namedtuple('QuotaInformation', [
    'timestamp',  # timestamp of the recording moment
    'used',  # used quota in KiB
    'soft',  # soft quota limit in KiB
    'hard',  # hard quota limit in KiB
    'doubt',  # the KiB GPFS is not sure about
    'expired',  # tuple (boolean, grace period expressed in seconds)
    'files_used',  # used number of inodes
    'files_soft',  # soft limit for inodes
    'files_hard',  # hard limit for inodes
    'files_doubt',  # the inodes GPFS is not sure about
    'files_expired',  # tuple (boolean, grace period expressed in seconds)
])


class QuotaEntity(object):
    """Definition of a entity with associated quota.

    The relevant information is stored in a RUDict instance,
    with the following structure:
    - string filesystem (name as known to GPFS)
        - string fileset (name as known to GPFS)
            - QuotaInformation
    """
    def __init__(self, storage, filesystem):
        """ Initialiser """
        self.storage = storage
        self.filesystem = filesystem
        self.quota_map = {}
        self.exceed_map = {}
        self.exceed = False

    def update(self, fileset, used=0, soft=0, hard=0, doubt=0, expired=(False, None), 
               files_used=0, files_soft=0, files_hard=0, files_doubt=0, files_expired=(False, None), 
               timestamp=None):
        """Store the quota for a given device.

        The arguments to this function are turned into a recursive
        dictionary that can easily be added to the existing information.
        """

        self.quota_map[fileset] = QuotaInformation(
            timestamp=timestamp,
            used=used,
            soft=soft,
            hard=hard,
            doubt=doubt,
            expired=expired,
            files_used=files_used,
            files_soft=files_soft,
            files_hard=files_hard,
            files_doubt=files_doubt,
            files_expired=files_expired,
        )

        self.exceed = self.exceed or expired[0] or files_expired[0]

    def exceeds(self):
        """Is the soft limit exceeded for some device?"""
        return self.exceed

    def __str__(self):
        return "%s: %s" % (self.storage, self.quota_map)


class QuotaUser(QuotaEntity):
    """Definition of a user with his associated quota."""
    def __init__(self, storage, filesystem, user_id):
        super(QuotaUser, self).__init__(storage, filesystem)
        self.user_id = user_id

    def key(self):
        return self.user_id

    def __str__(self):
        """Returns the quota information as a string."""
        result = []
        for (fileset, quota_info) in self.quota_map.items():
            if fileset and fileset.startswith("gvo"):
                suffix = "_VO"
            elif fileset and fileset.startswith("gp"):
                suffix = "_PROJECT"
            else:
                suffix = ''

            if quota_info.soft > 0:
                percentage = int(100.0 * quota_info.used / quota_info.soft)
            else:
                percentage = 0
            
            if quota_info.hard == 0:
                block_limit = "no quota set"
            else:
                block_limit = "quota %dMiB" % quota_info.hard / 1024

            if quota_info.files_hard == 0:
                inode_limit = "without limit"
            else:
                inode_limit = "with %d k files limit" % (quota_info.files_hard / 1000,)

            s = "%s%s: used %dMiB (%d%%) %s for %d k used files %s" % (
                self.storage,
                suffix,
                quota_info.used / 1024,
                percentage,
                block_limit,
                quota_info.files_used / 1000,
                inode_limit,)
            if quota_info.expired[0]:
                s += " grace: %d hours" % (quota_info.expired[1] / 3600)

            result.append(s)

        return "\n".join(result)


class QuotaFileset(QuotaEntity):
    """Definition of a Fileset with its associated quota."""
    def __init__(self, storage, filesystem, fileset_id):
        super(QuotaFileset, self).__init__(storage, filesystem)
        self.fileset_id = fileset_id

    def key(self):
        return self.fileset_id

    def __str__(self):
        return "Fileset <%s> has quota %s" % (self.fileset_id, super(QuotaFileset, self).__str__())


class QuotaGroup(QuotaEntity):
    """Definition of a group with it associated quota."""
    def __init__(self, group_id):
        """Initialisation.

        @type group_id: string
        @param group_id: the alphanumerical ID of the group.
        """
        super(QuotaGroup, self).__init__()
        self.group_id = group_id

    def key(self):
        return self.group_id

    def __str__(self):
        return "Group <%s> has quota %s" % (self.group_id, super(QuotaGroup, self).__str__())

    def __repr__(self):
        return self.__str__()

