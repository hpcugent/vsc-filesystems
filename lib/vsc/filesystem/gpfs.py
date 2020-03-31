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
GPFS specialised interface

@author: Stijn De Weirdt (Ghent University)
@author: Andy Georges (Ghent University)
"""
from __future__ import print_function
from future.utils import with_metaclass

import copy
import os
import re

from collections import namedtuple
from vsc.utils.py2vs3 import unquote as percentdecode
from socket import gethostname
from itertools import dropwhile

from vsc.config.base import GPFS_DEFAULT_INODE_LIMIT
from vsc.filesystem.posix import PosixOperations, PosixOperationError
from vsc.utils.missing import nub, find_sublist_index, Monoid, MonoidDict, RUDict
from vsc.utils.patterns import Singleton

GPFS_BIN_PATH = '/usr/lpp/mmfs/bin'

GpfsQuota = namedtuple('GpfsQuota',
    ['name',
     'blockUsage', 'blockQuota', 'blockLimit', 'blockInDoubt', 'blockGrace',
     'filesUsage', 'filesQuota', 'filesLimit', 'filesInDoubt', 'filesGrace',
     'remarks', 'quota', 'defQuota', 'fid', 'filesetname'])

GPFS_OK_STATES = ['HEALTHY', 'DISABLED']
GPFS_WARNING_STATES = ['DEGRADED']
GPFS_ERROR_STATES = ['FAILED', 'DEPEND']
GPFS_UNKNOWN_STATES = ['CHECKING', 'UNKNOWN']
GPFS_HEALTH_STATES = GPFS_OK_STATES + GPFS_WARNING_STATES + GPFS_ERROR_STATES + GPFS_UNKNOWN_STATES


def _automatic_mount_only(fs):
    """
    Filter that returns true if the filesystem is automount enabled
    """
    return fs['automaticMountOption'] in ('yes', 'automount')


def split_output_lines(out):
    """
    Split the output into field.

    Takes into acount that some lines may end in a :, some might not. If the header exhibits no colon,
    the field count might be off and we might not be able to parse the output as expected.
    """
    header_ends_in_colon = out[0][-1] == ":"

    def clean(line):
        if not header_ends_in_colon and line[-1] == ":":
            return line[:-1]
        else:
            return line

    return [[percentdecode(y) for y in clean(x).strip().split(':')] for x in out.strip().split('\n')]


class GpfsOperationError(PosixOperationError):
    pass


class GpfsOperations(with_metaclass(Singleton, PosixOperations)):

    def __init__(self):
        super(GpfsOperations, self).__init__()
        self.supportedfilesystems = ['gpfs', 'nfs']

        self.gpfslocalfilesystems = None  # the locally found GPFS filesystems
        self.gpfslocalquotas = None
        self.gpfslocalfilesets = None

        self.gpfsdisks = None

    # pylint: disable=arguments-differ
    def _execute(self, name, opts=None, changes=False):
        """Return and check the GPFS command.
            @type cmd: string, will be prefixed by GPFS_BIN_PATH if not absolute
        """

        if os.path.isabs(name):
            cmdname = name
        else:
            cmdname = os.path.join(GPFS_BIN_PATH, name)

        cmd = [cmdname]

        if opts is not None:
            if isinstance(opts, (tuple, list,)):
                cmd += list(opts)
            else:
                self.log.raiseException("_execute: please use a list or tuple for options: cmd %s opts %s" %
                                        (cmdname, opts), GpfsOperationError)

        ec, out = super(GpfsOperations, self)._execute(cmd, changes)

        return ec, out

    def _local_filesystems(self):
        """Add the gpfs device name"""
        super(GpfsOperations, self)._local_filesystems()

        if self.gpfslocalfilesystems is None:
            self.list_filesystems()

        self.localfilesystemnaming.append('gpfsdevice')
        for fs in self.localfilesystems:
            if fs[self.localfilesystemnaming.index('type')] == 'gpfs':
                localdevice = fs[self.localfilesystemnaming.index('device')]

                expectedprefix = '/dev'
                if localdevice.startswith(expectedprefix):
                    tmp = localdevice.split(os.sep)[len(expectedprefix.split(os.sep)):]
                    if len(tmp) == 1:
                        gpfsdevice = tmp[0]
                    else:
                        fs.append(None)
                        self.log.raiseException(("Something went wrong trying to resolve GPFS device from "
                                                 "localfilesystem device: fs %s") % fs, GpfsOperationError)
                else:
                    gpfsdevice = localdevice

                if gpfsdevice in self.gpfslocalfilesystems:
                    fs.append(gpfsdevice)
                else:
                    fs.append(None)
                    self.log.warning(("While trying to resolve GPFS device from localfilesystem device"
                                      " fs %s found gpfsdevice %s that is not in "
                                      "gpfslocalfilesystems %s") %
                                      (fs, gpfsdevice, self.gpfslocalfilesystems.keys()),
                                      GpfsOperationError)
            else:
                fs.append(None)

    def fixup_executeY_line(self, fields, description_count):
        """Try to fix an erroneous output line from an executeY run.

        @type fields: list
        @type description_count: int

        @param fields: the fields found in the output line.
        @param description_count: the number of expected fields.

        @return: list with the corrected lines

        FIXME: This really should be done by a decent parser, this is just a dirty hack
        """
        expected_start_fields = fields[:6]
        ls = fields[6:]
        sub_ls = expected_start_fields[1:]

        sub_index = find_sublist_index(ls, sub_ls)
        if sub_index is None:
            self.log.raiseException("Too many fields: %d (description has %d fields).\
                                        Cannot find match for the start field. Not fixing line %s" %
                                    (len(fields), description_count, fields))
        else:
            self.log.info("Fixing found an index for the sublist at %d" % (sub_index))
            line = expected_start_fields + ls[:sub_index]
            remainder = ls[sub_index:]

            if len(line) > description_count:
                self.log.raiseException("After fixing, line still has too many fields: line (%s), original (%s)" %
                                        (line, fields))

            # now we need to check if the string in the first field has somehow magically merged with the previous line
            first_field = fields[0]
            if remainder[0] == first_field:
                line.extend([''] * (description_count - len(line)))
                return [line, remainder]
            elif line[-1].endswith(first_field):
                line[-1] = line[-1].rstrip(first_field)
                line.extend([''] * (description_count - len(line)))
                remainder.insert(0, first_field)
                return [line, remainder]
            else:
                self.log.raiseException(("Failed to find the initial field of the line: %s after fixup and "
                                         "splitting line into [%s, %s]") % (first_field, line, remainder))

        return []

    def _assemble_fields(self, fields, out):
        """Assemble executeY output fields """

        # do we have multiple field counts?
        field_counts = [i for (i, _) in fields]
        if len(nub(field_counts)) > 1:
            maximum_field_count = max(field_counts)
            description_field_count = field_counts[0]
            for (field_count, line) in fields[1:]:
                if field_count == description_field_count:
                    continue
                elif field_count < description_field_count:
                    self.log.debug("Description length %s greater then %s. Adding whitespace. (names %s, row %s)" %
                                   (maximum_field_count, field_count, fields[0][6:], line[6:]))
                    line.extend([''] * (maximum_field_count - field_count))
                else:
                    # try to fix the line
                    self.log.info("Line has too many fields (%d > %d), trying to fix %s" %
                                  (field_count, description_field_count, line))
                    fixed_lines = self.fixup_executeY_line(line, description_field_count)
                    i = fields.index((field_count, line))
                    fields[i:i + 1] = map(lambda fs: (len(fs), fs), fixed_lines)

        # assemble result
        listm = Monoid([], lambda xs, ys: xs + ys)  # not exactly the fastest mappend for lists ...
        res = MonoidDict(listm)
        try:
            for index, name in enumerate(fields[0][1][6:]):
                if name != '':
                    for (_, line) in fields[1:]:
                        res[name] = [line[6 + index]]
        except IndexError:
            self.log.raiseException("Failed to regroup data %s (from output %s)" % (fields, out))

        return res


    def _executeY(self, name, opts=None, prefix=False):
        """Run with -Y and parse output in dict of name:list of values
           type prefix: boolean, if true prefix the -Y to the options (otherwise append the option).
        """
        if opts is None:
            opts = []
        elif isinstance(opts, (tuple, list,)):
            opts = list(opts)
        else:
            self.log.error("_executeY: have to use a list or tuple for options: name %s opts %s" % (name, opts))
            return {}

        if prefix:
            opts.insert(0, '-Y')
        else:
            opts.append('-Y')

        _, out = self._execute(name, opts)

        """Output looks like
        [root@node612 ~]# mmlsfs all -Y
        mmlsfs::HEADER:version:reserved:reserved:deviceName:fieldName:data:remarks:
        mmlsfs::0:1:::scratch:minFragmentSize:8192:
        mmlsfs::0:1:::scratch:inodeSize:512:

        # it's percent encoded: first split in :, then decode
        b = [[percentdecode(y) for y in  x.split(':')] for x in a]
        """
        what = split_output_lines(out)
        expectedheader = [name, '', 'HEADER', 'version', 'reserved', 'reserved']

        # verify result and remove all items that do not match the expected output data
        # e.g. mmrepquota start with single line of unnecessary ouput (which may be repeated for USR, GRP and FILESET)
        retained = dropwhile(lambda line: expectedheader != line[:6], what)

        # sanity check: all output lines should have the same number of fields. if this is not the case, padding is
        # added
        fields = [(len(x), x) for x in retained]
        if len(fields) != 0:
            return self._assemble_fields(fields, out)
        else:
            # mmhealth command has other header, no other known command does this.
            self.log.info('Not the default header, trying state and event headers')
            rest = {}
            for typ in ('State', 'Event'):
                try:
                    fields = [(len(x), x) for x in what if x[1] == typ]
                except IndexError:
                    self.log.raiseException("No valid lines for output: %s" % (out), GpfsOperationError)
                if len(fields):
                    res = self._assemble_fields(fields, out)
                    rest[typ] = res

                else:
                    self.log.raiseException("No valid lines of header type %s for output: %s" % (typ, out),
                        GpfsOperationError)

            return rest

    def list_filesystems(self, device='all', update=False, fs_filter=_automatic_mount_only):
        """
        List all filesystems.

        Set self.gpfslocalfilesystems to a convenient dict structure of the returned dict
        where the key is the deviceName, the value is a dict
            where the key is the fieldName and the values are the corresponding value, i.e., the
        """

        if not update and self.gpfslocalfilesystems:
            return self.gpfslocalfilesystems

        if not isinstance(device, list):
            devices = [device]
        else:
            devices = device

        res = RUDict()
        for device in devices:

            info = self._executeY('mmlsfs', [device])
            # for v3.5 deviceName:fieldName:data:remarks:

            # set the gpfsdevices
            gpfsdevices = nub(info.get('deviceName', []))
            if len(gpfsdevices) == 0:
                self.log.raiseException("No devices found. Returned info %s" % info, GpfsOperationError)
            else:
                self.log.debug("listAllFilesystems found device %s out of requested %s" % (gpfsdevices, devices))

            res_ = dict([(dev, {}) for dev in gpfsdevices])  # build structure
            res.update(res_)
            for dev, k, v in zip(info['deviceName'], info['fieldName'], info['data']):
                res[dev][k] = v

        if fs_filter:
            res = dict((f, v) for (f, v) in res.items() if fs_filter(v))

        self.gpfslocalfilesystems = res
        return res

    def list_quota(self, devices=None):
        """get quota info for all filesystems for all USR,GRP,FILESET
            set self.gpfslocalquota to
                dict: key = deviceName, value is
                    dict with key quotaType (USR | GRP | FILESET) value is dict with
                        key = id, value dict with
                            key = remaining header entries and corresponding values as a NamedTuple

        - GPFS 3.5 has the following fields in the output lines of mmrepquota (colon separated)
            - filesystemName
            - quotaType
            - id
            - name
            - blockUsage
            - blockQuota
            - blockLimit
            - blockInDoubt
            - blockGrace
            - filesUsage
            - filesQuota
            - filesLimit
            - filesInDoubt
            - filesGrace
            - remarks
            - quota
            - defQuota
            - fid
            - filesetname

        - GPFS 3.5 also is able to list multiple e.g., USR lines in different filesets.
        """
        if devices is None:
            devices = self.list_filesystems().keys()
        elif isinstance(devices, str):
            devices = [devices]

        listm = Monoid([], lambda xs, ys: xs + ys)  # not exactly the fastest mappend for lists ...
        info = MonoidDict(listm)
        for device in devices:
            res = self._executeY('mmrepquota', ['-n', device], prefix=True)
            for (key, value) in res.items():
                info[key] = value

        datakeys = info.keys()
        datakeys.remove('filesystemName')
        datakeys.remove('quotaType')
        datakeys.remove('id')

        fss = nub(info.get('filesystemName', []))
        self.log.debug("Found the following filesystem names: %s" % (fss))

        quotatypes = nub(info.get('quotaType', []))
        quotatypesstruct = dict([(qt, MonoidDict(Monoid([], lambda xs, ys: xs + ys))) for qt in quotatypes])

        res = dict([(fs, copy.deepcopy(quotatypesstruct)) for fs in fss])  # build structure

        for idx, (fs, qt, qid) in enumerate(zip(info['filesystemName'], info['quotaType'], info['id'])):
            details = dict([(k, info[k][idx]) for k in datakeys])
            if qt == 'FILESET':
                # GPFS fileset quota have empty filesetName field
                details['filesetname'] = details['name']
            res[fs][qt][qid] = [GpfsQuota(**details)]

        self.gpfslocalquotas = res
        return res

    def list_filesets(self, devices=None, filesetnames=None, update=False):
        """
        Get all the filesets for one or more specific devices

        @type devices: list of devices (if string: 1 device; if None: all found devices)
        @type filesetnames: report only on specific filesets (if string: 1 filesetname)

            set self.gpfslocalfilesets is dict with
                key = filesystemName value is dict with
                    key = id value is dict
                        key = remaining header entries and corresponding values
        """

        if not update and self.gpfslocalfilesets:
            return self.gpfslocalfilesets

        opts = []

        if devices is None:
            # get all devices from all filesystems
            if self.gpfslocalfilesystems is None:
                self.list_filesystems()

            devices = self.gpfslocalfilesystems.keys()
        else:
            if isinstance(devices, str):
                devices = [devices]

        if filesetnames is not None:
            if isinstance(filesetnames, str):
                filesetnames = [filesetnames]

            filesetnamestxt = ','.join(filesetnames)
            opts.append(filesetnamestxt)

        self.log.debug("Looking up filesets for devices %s" % (devices))

        listm = Monoid([], lambda xs, ys: xs + ys)
        info = MonoidDict(listm)
        for device in devices:
            opts_ = copy.deepcopy(opts)
            opts_.insert(1, device)
            res = self._executeY('mmlsfileset', opts_)
            # for v3.5
            # filesystemName:filesetName:id:rootInode:status:path:parentId:created:inodes:dataInKB:comment:
            # filesetMode:afmTarget:afmState:afmMode:afmFileLookupRefreshInterval:afmFileOpenRefreshInterval:
            # afmDirLookupRefreshInterval:afmDirOpenRefreshInterval:afmAsyncDelay:reserved:afmExpirationTimeout:afmRPO:
            # afmLastPSnapId:inodeSpace:isInodeSpaceOwner:maxInodes:allocInodes:inodeSpaceMask:afmShowHomeSnapshots:
            # afmNumReadThreads:afmNumReadGWs:afmReadBufferSize:afmWriteBufferSize:afmReadSparseThreshold:
            # afmParallelReadChunkSize:afmParallelReadThreshold:snapId:
            self.log.debug("list_filesets res keys = %s " % (res.keys()))
            for (key, value) in res.items():
                info[key] = value

        datakeys = info.keys()
        datakeys.remove('filesystemName')
        datakeys.remove('id')

        fss = nub(info.get('filesystemName', []))
        res = dict([(fs, {}) for fs in fss])  # build structure

        for idx, (fs, qid) in enumerate(zip(info['filesystemName'], info['id'])):
            details = dict([(k, info[k][idx]) for k in datakeys])
            res[fs][qid] = details

        self.gpfslocalfilesets = res
        return res

    def get_filesystem_info(self, filesystem):
        """Get all the relevant information for a given GPFS filesystem.

        @type filesystem: string representing the name of the filesystem in GPFS

        @returns: dictionary with the GPFS information

        @raise GpfsOperationError: if there is no filesystem with the given name
        """
        self.list_filesystems()  # make sure we have the latest information
        try:
            return self.gpfslocalfilesystems[filesystem]
        except KeyError:
            self.log.raiseException("GPFS has no information for filesystem %s" % (filesystem), GpfsOperationError)

    def get_fileset_info(self, filesystem_name, fileset_name):
        """Get all the relevant information for a given fileset.

        @type filesystem_name: string representing a gpfs filesystem
        @type fileset_name: string representing a gpfs fileset name (not the ID)

        @returns: dictionary with the fileset information or None if the fileset cannot be found

        @raise GpfsOperationError: if there is no filesystem with the given name
        """
        self.list_filesystems()
        self.list_filesets()
        try:
            filesets = self.gpfslocalfilesets[filesystem_name]
        except KeyError:
            self.log.raiseException("GPFS has no fileset information for filesystem %s" %
                                    (filesystem_name), GpfsOperationError)

        for fset in filesets.values():
            if fset['filesetName'] == fileset_name:
                return fset

        return None

    def _list_disk_single_device(self, device):
        """Return disk info for specific device
            both -M and -L info
        """
        shorthn = gethostname().split('.')[0]

        infoL = self._executeY('mmlsdisk', [device, '-L'])
        keysL = infoL.keys()
        keysL.remove('nsdName')
        infoM = self._executeY('mmlsdisk', [device, '-M'])
        keysM = infoM.keys()
        keysM.remove('nsdName')

        # sanity check

        commondomain = None
        # if this fails, nodes probably have shortnames
        try:
            # - means disk offline, so no nodename
            alldomains = ['.'.join(x.split('.')[1:]) for x in infoM['IOPerformedOnNode'] if x not in ['-', 'localhost']]
            if len(set(alldomains)) > 1:
                self.log.error("More than one domain found: %s." % alldomains)
            commondomain = alldomains[0]  # TODO: should be most frequent one
        except (IndexError, KeyError):
            self.log.exception("Cannot determine domainname for nodes %s" % infoM['IOPerformedOnNode'])
            commondomain = None

        for idx, node in enumerate(infoM['IOPerformedOnNode']):
            if node == 'localhost':
                infoM['IOPerformedOnNode'][idx] = '.'.join([x for x in [shorthn, commondomain] if x is not None])

        res = dict([(nsd, {}) for nsd in infoL['nsdName']])  # build structure
        for idx, nsd in enumerate(infoL['nsdName']):
            for k in keysL:
                res[nsd][k] = infoL[k][idx]
            for k in keysM:
                Mk = k
                if k in keysL:
                    # duplicate key !!
                    if not infoL[k][idx] == infoM[k][idx]:
                        self.log.error(("nsdName %s has named value %s in both -L and -M, but have different value"
                                        " L=%s M=%s") % (nsd, infoL[k][idx], infoM[k][idx]))
                    Mk = "M_%s" % k
                res[nsd][Mk] = infoM[k][idx]

        return res

    def list_disks(self, devices=None):
        """List all disks for devices (if devices is None, use all devices
            Return dict with
                key = device values is dict
                    key is disk, value is remaining property
        """
        if devices is None:
            # get all devices from all filesystems
            if self.gpfslocalfilesystems is None:
                self.list_filesystems()

            devices = self.gpfslocalfilesystems.keys()
        else:
            if isinstance(devices, str):
                devices = [devices]

        res = {}
        for device in devices:
            devinfo = self._list_disk_single_device(device)
            res[device] = devinfo

        self.gpfsdisks = res

    def list_nsds(self):
        """List NSD info
            Not implemented due to missing -Y option of mmlsnsd
        """
        self.log.error("listNsds not implemented.")

    def getAttr(self, obj=None):
        """
        mmlsattr on obj to get GFPS details on file or directory
        """
        obj = self._sanity_check(obj)

        if not self.exists(obj):
            self.log.raiseException("getAttr: obj %s does not exist", GpfsOperationError)

        ec, out = self._execute('mmlsattr', ["-L", obj])
        if ec > 0:
            self.log.raiseException("getAttr: mmlsattr with opts -L %s failed" % (obj), GpfsOperationError)

        res = {}

        for line in out.split("\n"):
            line = re.sub(r"\s+", '', line)
            if len(line) == 0:
                continue
            items = line.split(":")
            if len(items) == 1:
                items.append('')  # fix anomalies
            # creationtime has : in value as well eg creationtime:ThuAug2313:04:202012
            res[items[0]] = ":".join(items[1:])

        return res

    def get_details(self, obj=None):
        """Given obj, return as much relevant info as possible
        """
        obj = self._sanity_check(obj)

        res = {'parent': None}
        res['exists'] = self.exists(obj)

        if res['exists']:
            realpath = obj
        else:
            realpath = self._largest_existing_path(obj)
            res['parent'] = realpath

        fs = self._what_filesystem(obj)
        res['fs'] = fs

        res['attrs'] = self.getAttr(obj)

        return res

    def make_fileset(self, new_fileset_path, fileset_name=None, parent_fileset_name=None, afm=None, inodes_max=None,
                     inodes_prealloc=None):
        """
        Given path, create a new fileset and link it to said path
          - check uniqueness
          - set comment ?

        @type new_fileset_path: string representing the full path where the new fileset should be linked to
        @type fileset_name: string representing the name of the new fileset
        @type parent_fileset_name: string representing the name of the fileset with whoch the inode space should
                                   be shared. If this is None, then a new inode space will be created for this fileset.
        @type afm: Unused at this point.
        @type inodes_max: int representing maximal number of inodes to allocate for this fileset
        @type inodes_preallloc: int representing the maximal number of inodes to preallocate for this fileset

        If the latter two arguments are not provided, the default GPFS_DEFAULT_INODE_LIMIT is used.

        [root@node612 ~]# mmcrfileset -h
        Usage:
          mmcrfileset Device FilesetName [-p afmAttribute=Value...] [-t Comment]
             [--inode-space {new [--inode-limit MaxNumInodes[:NumInodesToPreallocate]] | ExistingFileset}]

        [root@node612 ~]# man mmcrfileset
        [root@node612 ~]# mmchfileset
        mmchfileset: Missing arguments.
        Usage:
          mmchfileset Device {FilesetName | -J JunctionPath}
                      [-j NewFilesetName] [-t NewComment]
                      [-p afmAttribute=Value...]
                      [--inode-limit MaxNumInodes[:NumInodesToPreallocate]]

        [root@node612 ~]# mmlinkfileset -h
        Usage:
           mmlinkfileset Device FilesetName [-J JunctionPath]


        Tests to perform
        - fileset
        - recreate same fileset
        - create fileset in existing fileset
        - create fileset with fsetpath part of symlink
        """
        del afm
        self.list_filesystems()  # get known filesystems
        self.list_filesets()  # do NOT force an update here. We do this at the end, should there be a fileset created.

        fsetpath = self._sanity_check(new_fileset_path)

        # does the path exist ?
        if self.exists(fsetpath):
            self.log.raiseException(("makeFileset for new_fileset_path %s returned sane fsetpath %s,"
                                     " but it already exists.") % (new_fileset_path, fsetpath), GpfsOperationError)

        # choose unique name
        parentfsetpath = os.path.dirname(fsetpath)
        if not self.exists(parentfsetpath):
            self.log.raiseException(("parent dir %s of fsetpath %s does not exist. Not going to create it "
                                     "automatically.") % (parentfsetpath, fsetpath), GpfsOperationError)

        fs = self.what_filesystem(parentfsetpath)
        foundgpfsdevice = fs[self.localfilesystemnaming.index('gpfsdevice')]

        # FIXME: Not sure if this is a good idea.
        if fileset_name is None:
            # guess the device from the pathname
            # subtract the device mount path from filesetpath ? (what with filesets in filesets)
            mntpt = fs[self.localfilesystemnaming.index('mountpoint')]
            if fsetpath.startswith(mntpt):
                lastpart = fsetpath.split(os.sep)[len(mntpt.split(os.sep)):]
                fileset_name = "_".join(lastpart)
            else:
                fileset_name = os.path.basedir(fsetpath)
                self.log.error("fsetpath %s doesn't start with mntpt %s. using basedir %s" %
                               (fsetpath, mntpt, fileset_name))

        # bail if there is a fileset with the same name or the same link location, i.e., path
        for efset in self.gpfslocalfilesets[foundgpfsdevice].values():
            efsetpath = efset.get('path', None)
            efsetname = efset.get('filesetName', None)
            if efsetpath == fsetpath or efsetname == fileset_name:
                self.log.raiseException(("Found existing fileset %s that has same path %s or same name %s as new "
                                         "path %s or new name %s") %
                                        (efset, efsetpath, efsetname, fsetpath, fileset_name), GpfsOperationError)

        # create the fileset
        # if created, try to link it with -J to path
        mmcrfileset_options = [foundgpfsdevice, fileset_name]
        if parent_fileset_name is None:
            mmcrfileset_options += ['--inode-space', 'new']
            if inodes_max:
                INODE_LIMIT_STRING = "%d" % (inodes_max,)
                if inodes_prealloc:
                    INODE_LIMIT_STRING += ":%d" % (inodes_prealloc,)
            else:
                INODE_LIMIT_STRING = GPFS_DEFAULT_INODE_LIMIT
            mmcrfileset_options += ['--inode-limit', INODE_LIMIT_STRING]
        else:
            parent_fileset_exists = False
            for efset in self.gpfslocalfilesets[foundgpfsdevice].values():
                if parent_fileset_name and parent_fileset_name == efset.get('filesetName', None):
                    parent_fileset_exists = True
            if not parent_fileset_exists:
                self.log.raiseException("Parent fileset %s does not appear to exist." %
                                        parent_fileset_name, GpfsOperationError)
            mmcrfileset_options += ['--inode-space', parent_fileset_name]

        (ec, out) = self._execute('mmcrfileset', mmcrfileset_options, True)
        if ec > 0:
            self.log.raiseException("Creating fileset with name %s on device %s failed (out: %s)" %
                                    (fileset_name, foundgpfsdevice, out), GpfsOperationError)

        # link the fileset
        ec, out = self._execute('mmlinkfileset', [foundgpfsdevice, fileset_name, '-J', fsetpath], True)
        if ec > 0:
            self.log.raiseException("Linking fileset with name %s on device %s to path %s failed (out: %s)" %
                                    (fileset_name, foundgpfsdevice, fsetpath, out), GpfsOperationError)

        # at the end, rescan the filesets and force update the info
        self.list_filesets(update=True)

    def set_user_quota(self, soft, user, obj=None, hard=None, inode_soft=None, inode_hard=None):
        """Set quota for a user.

        @type soft: integer representing the soft limit expressed in bytes
        @type user: string identifying the user
        @type grace: integer representing the grace period expressed in days
        @type inode_soft: integer representing the soft files limit
        @type inode_soft: integer representing the hard files quota
        """
        self._set_quota(soft, who=user, obj=obj, typ='user', hard=hard, inode_soft=inode_soft, inode_hard=inode_hard)

    def set_group_quota(self, soft, group, obj=None, hard=None, inode_soft=None, inode_hard=None):
        """Set quota for a group on a given object (e.g., a path in the filesystem, which may correpond to a fileset)

        @type soft: integer representing the soft limit expressed in bytes
        @type group: string identifying the group
        @type obj: the object, whatever it is
        @type hard: integer representing the hard limit expressed in bytes. If None, then 1.05 * soft.
        @type grace: integer representing the grace period expressed in days
        @type inode_soft: integer representing the soft files limit
        @type inode_soft: integer representing the hard files quota
        """
        self._set_quota(soft, who=group, obj=obj, typ='group', hard=hard, inode_soft=inode_soft, inode_hard=inode_hard)

    def set_fileset_quota(self, soft, fileset_path, fileset_name=None, hard=None, inode_soft=None, inode_hard=None):
        """Set quota on a fileset.

        @type soft: integer representing the soft limit expressed in bytes
        @type fileset_path: the linked path to the fileset
        @type hard: integer representing the hard limit expressed in bytes. If None, then 1.05 * soft.
        @type grace: integer representing the grace period expressed in days
        @type inode_soft: integer representing the soft files limit
        @type inode_soft: integer representing the hard files quota
        """
        # we need the corresponding fileset name
        if fileset_name is None:
            attr = self.getAttr(fileset_path)
            if 'filesetname' in attr:
                fileset_name = attr['filesetname']
                self.log.info("set_fileset_quota: setting fileset to %s for obj %s" % (fileset_name, fileset_path))
            else:
                self.log.raiseException(("set_fileset_quota: attrs for obj %s don't have filestename property "
                                         "(attr: %s)") % (fileset_path, attr), GpfsOperationError)

        self._set_quota(soft, who=fileset_name, obj=fileset_path, typ='fileset', hard=hard,
                        inode_soft=inode_soft, inode_hard=inode_hard)

    def set_user_grace(self, obj, grace=0):
        """Set the grace period for user data.

        @type obj: string representing the path where the GPFS was mounted or the device itself
        @type grace: grace period expressed in seconds
        """
        self._set_grace(obj, 'user', grace)

    def set_group_grace(self, obj, grace=0):
        """Set the grace period for user data.

        @type obj: string representing the path where the GPFS was mounted or the device itself
        @type grace: grace period expressed in seconds
        """
        self._set_grace(obj, 'group', grace)

    def set_fileset_grace(self, obj, grace=0):
        """Set the grace period for fileset data.

        @type obj: string representing the path where the GPFS was mounted or the device itself
        @type grace: grace period expressed in seconds
        """
        self._set_grace(obj, 'fileset', grace)

    def _set_grace(self, obj, typ, grace=0, id_=0):
        """Set the grace period for a given type of objects in GPFS.

        @type obj: the path or the GPFS device
        @type typ: the type of entities for which we set the grace
        @type grace: int representing the grace period in seconds
        """

        obj = self._sanity_check(obj)
        if not self.dry_run and not self.exists(obj):
            self.log.raiseException("setQuota: can't set quota on none-existing obj %s" % obj, GpfsOperationError)

        # FIXME: this should be some constant or such
        typ2opt = {'user': 'u',
                   'group': 'g',
                   'fileset': 'j',
                   }

        opts = []
        opts += ["-%s" % typ2opt[typ], "%s" % id_]
        opts += ["-t", "%s" % int(grace)]

        opts.append(obj)

        ec, _ = self._execute('tssetquota', opts, True)
        if ec > 0:
            self.log.raiseException("_set_grace: tssetquota with opts %s failed" % (opts), GpfsOperationError)

    def _set_quota(self, soft, who, obj=None, typ='user', hard=None, inode_soft=None, inode_hard=None):
        """Set quota on the given object.

        @type soft: integer representing the soft limit expressed in bytes
        @type who: string identifying the group
        @type typ: string representing the type of object to set quota for: user, fileset or group.
        @type hard: integer representing the hard limit expressed in bytes. If None, then 1.05 * soft.

        @type who: identifier (eg username or userid) (is redefined with filesetname from mmlsattr for typ=fileset)
        @type grace: integer representing the grace period expressed in seconds.
        @type inode_soft: integer representing the soft inodes quota
        @type inode_hard: integer representing the hard inodes quota. If None, then 1.05 * inode_soft
        """
        """
        Usage:
             tssetquota <select*1> <limit*2> <path>
             tssetquota <select*1> <usage*3> <path>
             tssetquota <gracetime*4> <path>
             tssetquota -f <inputFile>
                *1 select:[-u <user>|<uid>] | [-g <group>|<gid>] | [-j <fsetName>|<fsetID>]
                *2 limit:[-h <hard>] [-s <soft>] [-H <inode-hard>] [-S <inode-soft>]
                *3 usage:[-x <usage>] [-X <inode-usage>]
                *4 gracetime:[-t <block-grace-time> | -T <inode-grace-time>] {[-u | -g | -j] [-r]}
               Disk area sizes are either w/o suffix (byte)
                or with 'k' (kiloByte), 'm' (Mega-), 'g' (Giga-)
                  't' (Tera-), 'p' (PetaByte) (caps ignored)
                Examples: 1500k, 80M, 1000000, 3G, 4T
               The effective quotas will be passed in kilobytes and matched to block sizes.
               Inode limits accept only 'k' and 'm' suffixes.

        *** Edit grace times:
        Time units may be : days, hours, minutes, or seconds
        Grace period before enforcing soft limits for USRs:
        gpfs0: block grace period: 7 days, file grace period: 7 days

        """

        obj = self._sanity_check(obj)
        if not self.dry_run and not self.exists(obj):  # FIXME: hardcoding this here is fugly.
            self.log.raiseException("setQuota: can't set quota on none-existing obj %s" % obj, GpfsOperationError)

        # FIXME: this should be some constant or such
        typ2opt = {
            'user': 'u',
            'group': 'g',
            'fileset': 'j',
        }

        soft2hard_factor = 1.05

        if typ not in typ2opt:
            self.log.raiseException("_set_quota: unsupported type %s" % typ, GpfsOperationError)

        opts = []

        if hard is None:
            hard = int(soft * soft2hard_factor)
        elif hard < soft:
            self.log.raiseException("setQuota: can't set hard limit %s lower then soft limit %s" %
                                    (hard, soft), GpfsOperationError)

        opts += ["-%s" % typ2opt[typ], "%s" % who]
        opts += ["-s", "%sm" % int(soft / 1024 ** 2)]  # round to MB
        opts += ["-h", "%sm" % int(hard / 1024 ** 2)]  # round to MB

        if inode_soft is not None:
            if inode_hard is None:
                inode_hard = int(inode_soft * soft2hard_factor)
            elif inode_hard < inode_soft:
                self.log.raiseException("setQuota: can't set hard inode limit %s lower then soft inode limit %s" %
                                        (inode_hard, inode_soft), GpfsOperationError)

            opts += ["-S", str(inode_soft)]
            opts += ["-H", str(inode_hard)]

        opts.append(obj)

        ec, _ = self._execute('tssetquota', opts, True)
        if ec > 0:
            self.log.raiseException("_set_quota: tssetquota with opts %s failed" % (opts), GpfsOperationError)

    def list_snapshots(self, filesystem):
        """ List the snapshots of the given filesystem """
        try:
            snaps = self._executeY('mmlssnapshot', [filesystem])
            return snaps['directory']
        except GpfsOperationError as err:
            if 'No snapshots in file system' in err.args[0]:
                self.log.debug('No snapshots in filesystem %s' % filesystem)
                return []
            else:
                self.log.raiseException(err.args[0], GpfsOperationError)

    def create_filesystem_snapshot(self, fsname, snapname):
        """
        Create a full filesystem snapshot
            @type fsname: string representing the name of the filesystem
            @type snapname: string representing the name of the new snapshot
        """
        snapshots = self.list_snapshots(fsname)
        if snapname in snapshots:
            self.log.error("Snapshotname %s already exists for filesystem %s!" % (snapname, fsname))
            return 0

        opts = [fsname, snapname]
        ec, out = self._execute('mmcrsnapshot', opts, True)
        if ec > 0:
            self.log.raiseException("create_filesystem_snapshot: mmcrsnapshot with opts %s failed: %s"
                % (opts, out), GpfsOperationError)

        return ec == 0

    def delete_filesystem_snapshot(self, fsname, snapname):
        """
        Delete a full filesystem snapshot
            @type fsname: string representing the name of the filesystem
            @type snapname: string representing the name of the snapshot to delete
        """

        snapshots = self.list_snapshots(fsname)
        if snapname not in snapshots:
            self.log.error("Snapshotname %s does not exists for filesystem %s!" % (snapname, fsname))
            return 0

        opts = [fsname, snapname]
        ec, out = self._execute('mmdelsnapshot', opts, True)
        if ec > 0:
            self.log.raiseException("delete_filesystem_snapshot: mmdelsnapshot with opts %s failed: %s" %
                (opts, out), GpfsOperationError)
        return ec == 0

    def get_mmhealth_state(self):
        """ Get the mmhealth state info of the GPFS components """
        opts = ['node', 'show']
        res = self._executeY('mmhealth', opts)
        states = res['State']
        comp_entities = ['%s_%s' % ident for ident in zip(states['component'], states['entityname'])]
        return dict(zip(comp_entities, states['status']))


if __name__ == '__main__':
    g = GpfsOperations()

    g.list_filesystems()
    print("fs", g.gpfslocalfilesystems)

    g.list_quota()
    print("quota", g.gpfslocalquotas)

    g.list_filesets()
    print("filesets", g.gpfslocalfilesets)

    g.list_disks()
    print("disks", g.gpfsdisks)
