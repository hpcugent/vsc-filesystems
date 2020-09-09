# -*- coding: latin-1 -*-
#
# Copyright 2009-2020 Ghent University
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
LUSTRE specialised interface

@author: Kenneth Waegeman (Ghent University)
"""
from __future__ import print_function
from future.utils import with_metaclass

import copy
import os
import re
import yaml

from collections import namedtuple
from vsc.utils.py2vs3 import unquote as percentdecode

from vsc.filesystem.posix import PosixOperations, PosixOperationError
from vsc.utils.patterns import Singleton

LustreQuota = namedtuple('LustreQuota',
    ['name',
     'blockUsage', 'blockQuota', 'blockLimit', 'blockGrace',
     'filesUsage', 'filesQuota', 'filesLimit', 'filesGrace',])

typ2opt = {
    'user': 'u',
    'group': 'g',
    'project': 'p',
}
typ2param = {
    'USR': 'usr',
    'GRP': 'grp',
    'FILESET': 'prj',
}
quotyp2param = {
    'block': 'dt',
    'inode': 'md',
}

class LustreOperationError(PosixOperationError):
    pass

class LustreVscFsError(Exception):
    pass

class LustreVscFS():
    """Default class for a vsc managed Lustre file system
        Since Lustre doesn't have a 'lsfileset' kind of command,
        we need some hints to defining names, ids and mappings
        This can be a regular mapping function/hash,saved in some special file on lustre,..
        Alternatively we could replace this by changing the API
    """

    def __init__(self, mountpoint, project_locations, projectid_maps):

        self.mountpoint = mountpoint
        self.project_locations = project_locations
        self.projectid_maps = projectid_maps
        self.pjparser = re.compile("([a-zA-Z]+)([0-9]+)")

        def pjid_from_name(self, name):
            """ This only generates an id based on name and should be sanity_checked before using """
            prefix, pjid = self.pjparser.match(name).groups()
            if prefix in self.projectid_maps.keys():
                return self.projectid_maps[prefix] + int(pjid)
            else:
                self.log.raiseException("_pjid_from_name: project prefix %s not recognized" % prefix, LustreVscFSError)



class LustreVscGhentScratchFs(LustreVscFs):
    """ Make some assumptions on where to find filesets
        This could also be extended to be done by importing config files """
    def __init__(self, mountpoint):

        project_locations = ['gent/vo/*']
        projectid_maps = { 'gvo' : 900000 }
        super(LustreVscGhentScratchFs, self).__init__(mountpoint, project_locations, projectid_maps)


class LustreOperations(with_metaclass(Singleton, PosixOperations)):

    def __init__(self):
        super(LustreOperations, self).__init__()
        self.supportedfilesystems = ['lustre']
        self.filesystems = {}


    # pylint: disable=arguments-differ
    def _execute_lfs(self, name, opts=None, changes=False):
        """Return and check the LUSTRE lfs command.
        """

        cmd = ['/usr/bin/lfs', name]

        if opts is not None:
            if isinstance(opts, (tuple, list,)):
                cmd += list(opts)
            else:
                self.log.raiseException("_execute_lfs: please use a list or tuple for options: cmd %s opts %s" %
                                        (cmdname, opts), LustreOperationError)

        ec, out = self._execute(cmd, changes)

        return ec, out

    def _execute_lctl_get_param_qmt_yaml(self, device, typ, quotyp='block'):
        """ executy LUSTRE lctl get_param qmt.* command and parse output

        eg:
        lctl get_param qmt.kwlust-*.dt*.glb-prj
        qmt.kwlust-QMT0000.dt-0x0.glb-prj=
        global_pool0_dt_prj
        - id:      0
          limits:  { hard:     0, soft:   0, granted:    0, time:    604800 }
        - id:      1
          limits:  { hard:  3798016, soft:   3591168, granted:   3874836, time:  1599748949 }
        - id:      2
          limits:  { hard:   0, soft:   0, granted:  0, time:   281474976710656 }
        - id:      3
          limits:  { hard:   0, soft:   0, granted:  0, time: 281474976710656 }

        """
        if typ not in typ2param:
            self.log.raiseException("_execute_lctl_get_param_qmt_yaml: unsupported type %s. Use USR,GRP or FILESET"
                    % typ, LustreOperationError)
        if quotyp not in quotyp2param:
            self.log.raiseException("_execute_lctl_get_param_qmt_yaml: unsupported type %s. Use 'block' or 'inode'" %
                    quotyp, LustreOperationError)

        param = 'qmt.%s-*.%s-*.glb-%s' % (device, quotyp2param[quotyp], typ2param[typ])
        opts =['get_param', param]
        ec, res = self._execute_lctl(opts)
        quota_info = res.split("\n",2)
        try:
            newres = yaml.safe_load(quota_info[2])
        except yaml.YAMLError as exc:
            self.log.raiseException("_execute_lctl_get_param_qmt_yaml: Error in yaml output: %s" % exc, LustreOperationError)

        return newres

    def _execute_lctl(self, opts, changes=False):
        """ Return output of lctl command """

        cmd = ['/usr/sbin/lctl']
        if opts is not None:
            if isinstance(opts, (tuple, list,)):
                cmd += list(opts)
            else:
                self.log.raiseException("_execute_lctl: please use a list or tuple for options: cmd %s opts %s" %
                                        (cmdname, opts), LustreOperationError)

        ec, res = self._execute(cmd, changes)

        return ec, res

    def list_filesystems(self):

    def get_fs_for_path(self, path):

        fs = self.what_filesystem(path)
        fsname = fs[3].split(':/')[1]
        fsmount = fs[1]
        if not self.filesystems[fsname]:
            # TODO: Ideally this is set up from immutable config of some sorts instead of hard coded
            # Or need API change)
            self.filesystems[fsname] = LustreVscGhentScratchFs(fsmount)
        return self.filesystems[fsname]

    def map_project_id(self, project_path, fileset_name):
        fs = self.get_fs_for_path(project_path)
        return fs.pjid_from_name(fileset_name)

    def set_new_project_id(self, project_path, fileset_name):

        if not self.get_project_id(project_path, False):
            pjid = self.map_project_id(project_path, fileset_name)
            # recursive and inheritance flag set
            opts = ['-p', pjid, '-r', '-s', project_path]
            ec, res = self._execute_lfs('project', opts)
            if ec == 0:
                return pjid
            else:
                self.log.raiseException("Could not set new projectid %s for path %s" % (pjid, project_path),
                    LustreOperationError)
        else:
            self.log.raiseException("Path %s already has a projectid" % project_path, LustreOperationError)

    def get_project_id(self, project_path, existing=True):
        opts = ['-d', project_path]

        ec, res = self._execute_lfs('project', opts)
        pjid, flag, path = res.split()
        if flag == 'P' and path == project_path:
            return pjid
        elif existing:
            self.log.raiseException("Something went wrong fetching project id for %s. Output was %s"
                    % (project_path, res), LustreOperationError)
        else:
            self.log.debug('path has no pjid set')
            return None


    def list_quota(self, devices):
        """get quota info for filesystems for all user,group,project
            Output has been remapped to format of gpfs.py
                dict: key = deviceName, value is
                    dict with key quotaType (user | group | fileset) value is dict with
                        key = id, value dict with
                            key = remaining header entries and corresponding values as

        """
        if devices is None:
            devices = self.list_filesystems().keys()
        elif isinstance(devices, str):
            devices = [devices]

        quota = {}
        for fsname in devices:
            quota[fsname] = {}
            for typ in typ2param.keys():
                quota[fsname][typ] = {};
                blockres = self._execute_lctl_get_param_qmt_yaml(fsname, typ, 'block')
                inoderes = self._execute_lctl_get_param_qmt_yaml(fsname, typ, 'inode')
                for qentry in blockres:
                    qid = qentry['id']
                    qlim = qentry['limits']
                    qinfo = {
                        'name': qid,
                        'blockUsage' : qlim['granted'],
                        'blockQuota' : qlim['soft'],
                        'blockLimit' : qlim['hard'],
                        'blockGrace' : qlim['time'],
                    }
                    quota[fsname][typ][qid] = qinfo
                for qentry in inoderes:
                    qid = qentry['id']
                    qlim = qentry['limits']
                    quota[fsname][typ][qid].update({
                        'filesUsage' : qlim['granted'],
                        'filesQuota' : qlim['soft'],
                        'filesLimit' : qlim['hard'],
                        'filesGrace' : qlim['time'],
                    })
                    quota[fsname][typ][qid] = [LustreQuota(**quota[fsname][typ][qid])]

        self.quota_cached = quota
        return quota

    def list_filesets(self, devices=None,  ):
        """
        Get all the filesets for one or more specific devices

        @type devices: list of devices (if string: 1 device)
        @type filesetnames: report only on specific filesets (if string: 1 filesetname)

        """

        opts = []

        if isinstance(devices, str):
            devices = [devices]

        self.log.debug("Looking up filesets for devices %s" % (devices))

        return res


    def make_fileset(self, new_fileset_path, fileset_name=None, inodes_max=1048576):
        """
        Given path, create a new directory and set file quota
          - check uniqueness

        @type new_fileset_path: string representing the full path where the new fileset should be
        @type fileset_name: string representing the name of the new fileset
        @type inodes_max: int representing file quota

        """

        fsetpath = self._sanity_check(new_fileset_path)

        # does the path exist ?
        if self.exists(fsetpath):
            self.log.raiseException(("makeFileset for new_fileset_path %s returned sane fsetpath %s,"
                                     " but it already exists.") % (new_fileset_path, fsetpath), LustreOperationError)

        parentfsetpath = os.path.dirname(fsetpath)
        if not self.exists(parentfsetpath):
            self.log.raiseException(("parent dir %s of fsetpath %s does not exist. Not going to create it "
                                     "automatically.") % (parentfsetpath, fsetpath), LustreOperationError)

        if fileset_name is None:
            self.log.raiseException('fileset name is mandatory')

        fs = self.what_filesystem(parentfsetpath)
        fsname = fs[3].split(':/')[1]

        fsinfo = self.get_fileset_info(fsname, fileset_name)
        if not fsinfo:
            self.make_dir(fsetpath)
            project = self.set_new_project_id(fsetpath, fileset_name)
            # set inode quota
            self._set_quota(who=project, obj=fileset_path, typ='project',inode_soft=inodes_max, inode_hard=inodes_max)

            pass
            # create the fileset: dir and project
        else:
        # bail if there is a fileset with the same name
            self.log.raiseException(("Found existing fileset %s with the same name at %s ") %
                                        (fileset_name, fsinfo['path']), LustreOperationError)



    def set_user_quota(self, soft, user, obj=None, hard=None, inode_soft=None, inode_hard=None):
        """Set quota for a user.

        @type soft: integer representing the soft limit expressed in bytes
        @type user: string identifying the user
        @type obj: the path
        @type hard: integer representing the hard limit expressed in bytes. If None, then 1.05 * soft.
        @type inode_soft: integer representing the soft files limit
        @type inode_soft: integer representing the hard files quota
        """
        self._set_quota(who=user, obj=obj, typ='user', soft=soft, hard=hard, inode_soft=inode_soft, inode_hard=inode_hard)

    def set_group_quota(self, soft, group, obj=None, hard=None, inode_soft=None, inode_hard=None):
        """Set quota for a group on a given object (e.g., a path in the filesystem, which may correpond to a fileset)

        @type soft: integer representing the soft limit expressed in bytes
        @type group: string identifying the group
        @type obj: the path
        @type hard: integer representing the hard limit expressed in bytes. If None, then 1.05 * soft.
        @type inode_soft: integer representing the soft files limit
        @type inode_soft: integer representing the hard files quota
        """
        self._set_quota(who=group, obj=obj, typ='group', soft=soft, hard=hard, inode_soft=inode_soft, inode_hard=inode_hard)

    def set_fileset_quota(self, soft, fileset_path, hard=None, inode_soft=None, inode_hard=None):
        """Set quota on a fileset. This maps to projects in Lustre

        @type soft: integer representing the soft limit expressed in bytes
        @type fileset_path: the linked path to the fileset
        @type hard: integer representing the hard limit expressed in bytes. If None, then 1.05 * soft.
        @type inode_soft: integer representing the soft files limit
        @type inode_soft: integer representing the hard files quota
        """
        # we need the corresponding project id
        project = self._get_project_id(fileset_path)
        if int(project) == 0:
            self.log.raiseException("Can not set quota for fileset with projectid 0", LustreOperationError)
        else:
            self._set_quota(who=project, obj=fileset_path, typ='project', soft=soft, hard=hard,
                        inode_soft=inode_soft, inode_hard=inode_hard)

    def set_user_grace(self, obj, grace=0):
        """Set the grace period for user data.

        @type obj: string representing the path where the FS was mounted
        @type grace: grace period expressed in seconds
        """
        self._set_grace(obj, 'user', grace)

    def set_group_grace(self, obj, grace=0):
        """Set the grace period for user data.

        @type obj: string representing the path where the FS was mounted
        @type grace: grace period expressed in seconds
        """
        self._set_grace(obj, 'group', grace)

    def set_fileset_grace(self, obj, grace=0):
        """Set the grace period for fileset data.
        This maps to projects in Lustre
        @type obj: string representing the path where the FS was mounted
        @type grace: grace period expressed in seconds
        """
        self._set_grace(obj, 'project', grace)

    def _set_grace(self, obj, typ, grace=0):
        """Set the grace period for a given type of objects

        @type obj: the path
        @type typ: the type of entities for which we set the grace
        @type grace: int representing the grace period in seconds
        """

        obj = self._sanity_check(obj)
        if not self.dry_run and not self.exists(obj):
            self.log.raiseException("setQuota: can't set quota on none-existing obj %s" % obj, LustreOperationError)

        opts = ['-t']
        opts += ["-%s" % typ2opt[typ]]
        opts += ["-b", "%s" % int(grace)]
        opts += ["-i", "%s" % int(grace)]

        opts.append(obj)

        ec, _ = self._execute_lfs('setquota', opts, True)
        if ec > 0:
            self.log.raiseException("_set_grace: setquota with opts %s failed" % (opts), LustreOperationError)

    def _get_quota(self, who, obj, typ):
        """Get quota of a given object.

        @type who: identifier (username, uid, gid, group, projectid)
        @type obj: the path
        @type typ: string representing the type of object to set quota for: user, project or group.
        """

        obj = self._sanity_check(obj)
        if not self.dry_run and not self.exists(obj):
            self.log.raiseException("setQuota: can't set quota on none-existing obj %s" % obj, LustreOperationError)

        if typ not in typ2opt:
            self.log.raiseException("_set_quota: unsupported type %s" % typ, LustreOperationError)

        opts = []
        opts += ["-%s" % typ2opt[typ], "%s" % who]
        opts.append(obj)

        ec, res = self._execute_lfs('quota', opts)
        return res


    def _set_quota(self, who, obj, typ='user', soft=None, hard=None, inode_soft=None, inode_hard=None):
        """Set quota on the given object.

        @type soft: integer representing the soft limit expressed in bytes
        @type who: identifier (eg username or userid)
        @type obj: the path
        @type typ: string representing the type of object to set quota for: user, fileset or group.
        @type hard: integer representing the hard limit expressed in bytes. If None, then 1.05 * soft.

        @type inode_soft: integer representing the soft inodes quota
        @type inode_hard: integer representing the hard inodes quota. If None, then 1.05 * inode_soft
        """

        obj = self._sanity_check(obj)
        if not self.dry_run and not self.exists(obj):
            self.log.raiseException("setQuota: can't set quota on none-existing obj %s" % obj, LustreOperationError)

        soft2hard_factor = 1.05

        if typ not in typ2opt:
            self.log.raiseException("_set_quota: unsupported type %s" % typ, LustreOperationError)

        opts = []

        if hard is None:
            hard = int(soft * soft2hard_factor)
        elif hard < soft:
            self.log.raiseException("setQuota: can't set hard limit %s lower then soft limit %s" %
                                    (hard, soft), LustreOperationError)

        opts += ["-%s" % typ2opt[typ], "%s" % who]
        opts += ["-b", "%sm" % int(soft / 1024 ** 2)]  # round to MB
        opts += ["-B", "%sm" % int(hard / 1024 ** 2)]  # round to MB

        if inode_soft is not None:
            if inode_hard is None:
                inode_hard = int(inode_soft * soft2hard_factor)
            elif inode_hard < inode_soft:
                self.log.raiseException("setQuota: can't set hard inode limit %s lower then soft inode limit %s" %
                                        (inode_hard, inode_soft), LustreOperationError)

            opts += ["-i", str(inode_soft)]
            opts += ["-I", str(inode_hard)]

        opts.append(obj)

        ec, _ = self._execute_lfs('setquota', opts, True)
        if ec > 0:
            self.log.raiseException("_set_quota: tssetquota with opts %s failed" % (opts), LustreOperationError)



if __name__ == '__main__':
    lust = LustreOperations()

    print(lust.list_quota('lustrefs'))

    print(lust.list_filesets('lustrefs'))

