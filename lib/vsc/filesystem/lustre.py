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

import os
import re

from collections import namedtuple

from vsc.filesystem.posix import PosixOperations, PosixOperationError
from vsc.utils.patterns import Singleton
from vsc.utils import fancylogger

from future.utils import with_metaclass
import yaml

LustreQuota = namedtuple('LustreQuota',
    ['name',
        'blockUsage', 'blockQuota', 'blockLimit', 'blockGrace', 'blockInDoubt',
        'filesUsage', 'filesQuota', 'filesLimit', 'filesGrace', 'filesInDoubt'])

TYP2OPT = {
    'user': 'u',
    'group': 'g',
    'project': 'p',
}
TYP2PARAM = {
    'USR': 'usr',
    'GRP': 'grp',
    'FILESET': 'prj',
}
QUOTYP2PARAM = {
    'block': 'dt',
    'inode': 'md',
}

class LustreOperationError(PosixOperationError):
    """ Lustre Error """

class LustreVscFSError(Exception):
    """ LustreVSCFS Error """

class LustreVscFS():
    """Default class for a vsc managed Lustre file system
        Since Lustre doesn't have a 'lsfileset' kind of command,
        we need some hints to defining names, ids and mappings
        This can be a regular mapping function/hash,saved in some special file on lustre,..
        Alternatively we could replace this by changing the API
    """

    def __init__(self, mountpoint, project_locations, projectid_maps):

        self.log = fancylogger.getLogger(name=self.__class__.__name__, fname=False)
        self.mountpoint = mountpoint
        self.project_locations = project_locations
        self.projectid_maps = projectid_maps
        self.pjparser = re.compile("([a-zA-Z]+)([0-9]+)")

    def pjid_from_name(self, name):
        """ This only generates an id based on name and should be sanity_checked before using """
        prefix, pjid = self.pjparser.match(name).groups()
        if prefix in self.projectid_maps.keys():
            res = self.projectid_maps[prefix] + int(pjid)
            return res
        else:
            self.log.raiseException("_pjid_from_name: project prefix %s not recognized" % prefix, LustreVscFSError)
            return None

    def get_search_paths(self):
        """ Get all the paths we should look for projects """
        res = []
        for loc in self.project_locations:
            res.append(os.path.join(self.mountpoint, loc))
        return res


class LustreVscGhentScratchFs(LustreVscFS):
    """ Make some assumptions on where to find filesets
        This could also be extended to be done by importing config files """

    def __init__(self, mountpoint):

        project_locations = ['gent', 'gent/vo/*']
        projectid_maps = {'gvo' : 900000}
        super(LustreVscGhentScratchFs, self).__init__(mountpoint, project_locations, projectid_maps)


class LustreOperations(with_metaclass(Singleton, PosixOperations)):
    """ Lustre Operations """

    def __init__(self):
        super(LustreOperations, self).__init__()
        self.supportedfilesystems = ['lustre']
        self.filesystems = {}
        self.filesets = {}


    def _execute_lfs(self, name, opts=None, changes=False):
        """Return and check the LUSTRE lfs command.
        """

        cmd = ['/usr/bin/lfs', name]

        if opts is not None:
            if isinstance(opts, (tuple, list,)):
                cmd += list(opts)
            else:
                self.log.raiseException("_execute_lfs: please use a list or tuple for options: cmd %s opts %s" %
                                        (cmd, opts), LustreOperationError)

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
        if typ not in TYP2PARAM:
            self.log.raiseException("_execute_lctl_get_param_qmt_yaml: unsupported type %s. Use USR,GRP or FILESET"
                % typ, LustreOperationError)
        if quotyp not in QUOTYP2PARAM:
            self.log.raiseException("_execute_lctl_get_param_qmt_yaml: unsupported type %s. Use 'block' or 'inode'" %
                quotyp, LustreOperationError)

        param = 'qmt.%s-*.%s-*.glb-%s' % (device, QUOTYP2PARAM[quotyp], TYP2PARAM[typ])
        opts = ['get_param', param]
        _ec, res = self._execute_lctl(opts)
        quota_info = res.split("\n", 2)
        try:
            newres = yaml.safe_load(quota_info[2])
        except yaml.YAMLError as exc:
            self.log.raiseException("_execute_lctl_get_param_qmt_yaml: Error in yaml output: %s"
                % exc, LustreOperationError)

        return newres

    def _execute_lctl(self, opts, changes=False):
        """ Return output of lctl command """

        cmd = ['/usr/sbin/lctl']
        if opts is not None:
            if isinstance(opts, (tuple, list,)):
                cmd += list(opts)
            else:
                self.log.raiseException("_execute_lctl: please use a list or tuple for options: cmd %s opts %s" %
                                        (cmd, opts), LustreOperationError)

        ec, res = self._execute(cmd, changes)

        return ec, res

    def list_filesystems(self, device=None):
        """ List all Lustre file systems """
        if not self.localfilesystems:
            self._local_filesystems()

        if device is None:
            devices = []
        elif not isinstance(device, list):
            devices = [device]
        else:
            devices = device

        lustrefss = {}
        for fs in self.localfilesystems:
            if fs[0] == 'lustre':
                fsloc = fs[3].split(':/')
                if len(fsloc) == 2:
                    fsname = fsloc[1]
                    if not devices or fsname in devices:
                        # keeping gpfs terminology
                        lustrefss[fsname] = {
                            'defaultMountPoint': fs[1],
                            'location': fsloc[0]
                        }

        if not devices and not lustrefss:
            self.log.raiseException("No Lustre Filesystems found", LustreOperationError)
        elif not all(elem in lustrefss.keys() for elem in devices):
            self.log.raiseException("Not all Lustre Filesystems of %s found, found %s" % (devices, lustrefss.keys()),
                LustreOperationError)

        return lustrefss

    def _get_fsinfo_for_path(self, path):
        fs = self.what_filesystem(path)
        return (fs[3].split(':/')[1], fs[1])

    def _get_fshint_for_path(self, path):
        fsname, fsmount = self._get_fsinfo_for_path(path)
        if fsname not in self.filesystems:
            # TODO: Ideally this is set up from immutable config of some sorts instead of hard coded
            # Or need API change)
            self.filesystems[fsname] = LustreVscGhentScratchFs(fsmount)
        return self.filesystems[fsname]

    def _map_project_id(self, project_path, fileset_name):
        fs = self._get_fshint_for_path(project_path)
        return fs.pjid_from_name(fileset_name)

    def _set_new_project_id(self, project_path, pjid):

        exid = self.get_project_id(project_path, False)
        if not exid or int(exid) == 0:
            # recursive and inheritance flag set
            opts = ['-p', pjid, '-r', '-s', project_path]
            ec, _res = self._execute_lfs('project', opts, True)
            if ec == 0:
                return pjid
            else:
                self.log.raiseException("Could not set new projectid %s for path %s" % (pjid, project_path),
                    LustreOperationError)
        else:
            self.log.raiseException("Path %s already has a projectid %s" % (project_path, exid), LustreOperationError)

        return None

    def get_project_id(self, project_path, existing=True):
        """ Parse lfs project output to get the project id for fileset """

        project_path = self._sanity_check(project_path)
        opts = ['-d', project_path]

        _ec, res = self._execute_lfs('project', opts)
        pjid, flag, path = res.split()
        self.log.info('got pjid %s, flag %s, path %s' % (pjid, flag, path))
        if flag == 'P' and path == project_path:
            return pjid
        elif existing:
            self.log.raiseException("Something went wrong fetching project id for %s. Output was %s"
                    % (project_path, res), LustreOperationError)
        else:
            self.log.debug('path has no pjid set')
        return None

    # pylint: disable=arguments-differ
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
            for typ in TYP2PARAM.keys():
                quota[fsname][typ] = {}
                blockres = self._execute_lctl_get_param_qmt_yaml(fsname, typ, 'block')
                inoderes = self._execute_lctl_get_param_qmt_yaml(fsname, typ, 'inode')
                for qentry in blockres:
                    qid = qentry['id']
                    qlim = qentry['limits']
                    # map quota fields to same names as for GPFS
                    qinfo = {
                        'name': qid,
                        'blockUsage' : qlim['granted'],
                        'blockQuota' : qlim['soft'],
                        'blockLimit' : qlim['hard'],
                        'blockGrace' : qlim['time'],
                        'blockInDoubt': 0, # non-existent in Lustre
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
                        'filesInDoubt': 0 # non-existent in Lustre
                    })
                    quota[fsname][typ][qid] = [LustreQuota(**quota[fsname][typ][qid])]

        return quota

    def _list_filesets(self, device):
        """ Get all filesets for a Lustre device"""

        fs = self._get_fshint_for_path(device['defaultMountPoint'])

        filesets = {}
        for upath in fs.get_search_paths():
            spath = self._sanity_check(upath)
            ec, res = self._execute_lfs('project', [spath])
            if ec != 0:
                self.log.raiseException("Unable to get projects for path %s" % spath, LustreOperationError)

            for pjline in res.splitlines():
                pjid, flag, path = pjline.split()
                if int(pjid) == 0:
                    self.log.warning("path %s is part of default project", path)
                    continue
                else:
                    if pjid in filesets:
                        self.log.raiseException("projectids mapping multiple paths: %s: %s, %s" %
                            (pjid, filesets[pjid]['path'], path), LustreOperationError)
                    elif flag != 'P':
                        # Not sure if this should give error or raise Exception
                        self.log.raiseException("Project inheritance flag not set for project %s: %s"
                            % (pjid, path), LustreOperationError)
                    else:
                        path = self._sanity_check(path)
                        filesets[pjid] = {'path': path, 'filesetName': os.path.basename(path)}


        return filesets

    def set_fs_update(self, device):
        """ Update this FS next run of list_filesets """

        del self.filesets[device]

    def get_fileset_info(self, filesystem_name, fileset_name):
        """ get the info of a specific fileset """
        fsets = self.list_filesets(filesystem_name)
        for fileset in fsets[filesystem_name].values():
            if fileset['filesetName'] == fileset_name:
                return fileset

        return None

    def list_filesets(self, devices=None):
        """
        Get all the filesets for one or more specific devices

        @type devices: list of devices (if string: 1 device)
        @type filesetnames: report only on specific filesets (if string: 1 filesetname)
        """

        self.log.debug("Looking up filesets for devices %s", devices)

        devices = self.list_filesystems(devices)

        filesetsres = {}
        for dev in devices.keys():
            if dev not in self.filesets:
                self.filesets[dev] = self._list_filesets(devices[dev])

            filesetsres[dev] = self.filesets[dev]

        return filesetsres


    def make_fileset(self, new_fileset_path, fileset_name, inodes_max=1048576):
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

        fsname, _fsmount = self._get_fsinfo_for_path(parentfsetpath)
        fsinfo = self.get_fileset_info(fsname, fileset_name)
        if not fsinfo:
            pjid = self._map_project_id(fsetpath, fileset_name)
            filesets = self.list_filesets(fsname)
            if pjid in filesets[fsname]:
                self.log.raiseException("Found existing projectid %s in file system %s: %s"
                    % (pjid, fsname, filesets[pjid]), LustreOperationError)

            # create the fileset: dir and project
            self.make_dir(fsetpath)
            self._set_new_project_id(fsetpath, pjid)
            # set inode quota
            self._set_quota(who=pjid, obj=fsetpath, typ='project', inode_soft=inodes_max, inode_hard=inodes_max)
            self.log.info("Created new fileset %s at %s with id %s", fileset_name, fsetpath, pjid)
            self.set_fs_update(fsname)

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
        self._set_quota(who=user, obj=obj, typ='user', soft=soft, hard=hard,
                inode_soft=inode_soft, inode_hard=inode_hard)

    def set_group_quota(self, soft, group, obj=None, hard=None, inode_soft=None, inode_hard=None):
        """Set quota for a group on a given object (e.g., a path in the filesystem, which may correpond to a fileset)

        @type soft: integer representing the soft limit expressed in bytes
        @type group: string identifying the group
        @type obj: the path
        @type hard: integer representing the hard limit expressed in bytes. If None, then 1.05 * soft.
        @type inode_soft: integer representing the soft files limit
        @type inode_soft: integer representing the hard files quota
        """
        self._set_quota(who=group, obj=obj, typ='group', soft=soft, hard=hard,
                inode_soft=inode_soft, inode_hard=inode_hard)

    def set_fileset_quota(self, soft, fileset_path, hard=None, inode_soft=None, inode_hard=None):
        """Set quota on a fileset. This maps to projects in Lustre

        @type soft: integer representing the soft limit expressed in bytes
        @type fileset_path: the linked path to the fileset
        @type hard: integer representing the hard limit expressed in bytes. If None, then 1.05 * soft.
        @type inode_soft: integer representing the soft files limit
        @type inode_soft: integer representing the hard files quota
        """
        # we need the corresponding project id
        project = self.get_project_id(fileset_path)
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
        opts += ["-%s" % TYP2OPT[typ]]
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

        if typ not in TYP2OPT:
            self.log.raiseException("_set_quota: unsupported type %s" % typ, LustreOperationError)

        opts = []
        opts += ["-%s" % TYP2OPT[typ], "%s" % who]
        opts.append(obj)

        _ec, res = self._execute_lfs('quota', opts)
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

        if typ not in TYP2OPT:
            self.log.raiseException("_set_quota: unsupported type %s" % typ, LustreOperationError)

        opts = ["-%s" % TYP2OPT[typ], "%s" % who]

        if soft is None and inode_soft is None:
            self.log.raiseException("setQuota: At least one type of quota (block,inode) should be specified",
                    LustreOperationError)

        if soft:
            if hard is None:
                hard = int(soft * soft2hard_factor)
            elif hard < soft:
                self.log.raiseException("setQuota: can't set hard limit %s lower then soft limit %s" %
                                    (hard, soft), LustreOperationError)
            softm = int(soft / 1024 ** 2) # round to MB
            hardm = int(hard / 1024 ** 2) # round to MB
            if softm == 0 or hardm == 0:
                self.log.raiseException("setQuota: setting quota to 0 would be infinite quota", LustreOperationError)
            else:
                opts += ["-b", "%sm" % softm]
                opts += ["-B", "%sm" % hardm]

        if inode_soft:
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
