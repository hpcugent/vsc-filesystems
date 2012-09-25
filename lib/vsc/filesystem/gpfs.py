"""
20/08/2012 Stijn De Weirdt HPC UGent-VSC

GPFS specialised interface
"""

import vsc.fancylogger as fancylogger

from vsc.filesystem.posix import PosixOperations
from urllib import unquote as percentdecode
from socket import gethostname

import os, re

GPFS_BIN_PATH = '/usr/lpp/mmfs/bin'

class GpfsOperations(PosixOperations):
    def __init__(self):
        PosixOperations.__init__(self)
        self.supportedfilesystems = ['gpfs']

        self.gpfslocalfilesystems = None ## the locally found GPFS filesystems
        self.gpfslocalquotas = None
        self.gpfslocalfilesets = None

        self.gpfsdisks = None

    def _execute(self, name, opts=None):
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
                self.log.raiseException("_execute: please use a list or tuple for options: cmd %s opts %s" % (cmdname, opts))
                cmd.append(opts)

        ec, out = PosixOperations._execute(self, cmd)

        return ec, out

    def _localFilesystems(self):
        """Add the gpfs device name"""
        PosixOperations._localFilesystems(self)

        if self.gpfslocalfilesystems is None:
            self.listFilesystems()

        self.localfilesystemnaming.append('gpfsdevice')
        for fs in self.localfilesystems:
            if fs[self.localfilesystemnaming.index('type')] == 'gpfs':
                localdevice = fs[self.localfilesystemnaming.index('device')]
                expectedprefix = '/dev'
                if localdevice.startswith(expectedprefix):
                    tmp = localdevice.split(os.sep)[len(expectedprefix.split(os.sep)):]
                    if len(tmp) == 1:
                        gpfsdevice = tmp[0]
                        if gpfsdevice in self.gpfslocalfilesystems:
                            fs.append(gpfsdevice)
                        else:
                            fs.append(None)
                            self.log.raiseException("While trying to resolve GPFS device from localfilesystem device fs %s found gpfsdevice %s that is not in gpfslocalfilesystems %s" % (fs, gpfsdevice, self.gpfslocalfilesystems.keys()))
                    else:
                        fs.append(None)
                        self.log.raiseException("Something went wrong trying to resolve GPFS device from localfilesystem device: fs %s" % fs)
            else:
                fs.append(None)

    def _executeY(self, name, opts=None, prefix=False):
        """Run with -Y and parse output in dict of name:list of values
            @type prefix: boolean, if true prefix the -Y to the options (otherwise append the option)
        """
        if opts is None:
            opts = []
        elif isinstance(opts, (tuple, list,)):
            opts = list(opts)
        else:
            self.log.error("_executeY: have to use a list or tuple for options: name %s opts %s" % (name, opts))
            return

        if prefix:
            opts.insert(0, '-Y')
        else:
            opts.append('-Y')

        ec, out = self._execute(name, opts)

        """Output looks like
        [root@node612 ~]# mmlsfs all -Y
        mmlsfs::HEADER:version:reserved:reserved:deviceName:fieldName:data:remarks:
        mmlsfs::0:1:::scratch:minFragmentSize:8192:
        mmlsfs::0:1:::scratch:inodeSize:512:

        ## it's percent encoded: first split in :, then decode
        b = [[percentdecode(y) for y in  x.split(':')] for x in a]
        """
        what = [[percentdecode(y) for y in  x.strip().split(':')] for x in out.strip().split('\n')]

        expectedheader = [name, '', 'HEADER', 'version', 'reserved', 'reserved']
        ## verify result
        ## eg mmrepquota start with single line of unnecessary ouput
        header = what[0][:6]
        while (not expectedheader == header) and len(what) > 0:
            self.log.warning('Unexpected header start: %s. Skipping.' % header)
            what.pop(0)
            header = what[0][:6]


        if len(what) == 0:
            self.log.raiseException('No valid header start for output: %s' % out)

        ## sanity check
        nrfields = [len(x) for x in what]
        if len(set(nrfields)) > 1:
            if nrfields[0] == max(nrfields[1:]):
                ## description length is equal to maximum. will be padded, since there is at least one list of values that matches the length
                self.log.debug("Number of entries in output %s. Nr of headers %s equal max of number of values." % (nrfields, nrfields[0]))
            else:
                self.log.error("Number of entries in output %s for what %s." % (nrfields, what))

            ## sanity check
            for idx in xrange(1, len(what)):
                if not (what[idx][0:2] == expectedheader[0:2]):
                    self.log.raiseException("No expected start of header %s for full row %s" % (expectedheader[0:2], what[idx ]))

                if nrfields[0] > nrfields[idx ]:
                    self.log.debug("Description length %s greater then %s. Adding whitespace. (names %s, row %s)" % (nrfields[0], nrfields[idx], what[0][6:], what[idx ][6:]))
                    what[idx].extend([''] * (nrfields[0] - nrfields[idx]))
                elif nrfields[0] < nrfields[idx ]:
                    self.log.raiseException("Description length %s smaller then %s. Not fixing. (names %s, row %s)" % (nrfields[0], nrfields[idx ], what[0][6:], what[idx ][6:]))

        res = {}
        try:
            for headeridx, name in enumerate(what[0][6:]):
                res[name] = []
                for idx in xrange(1, len(what)):
                    res[name].append(what[idx ][6 + headeridx])
        except:
            self.log.exception("Failed to regroup data %s (from output %s)" % (what, out))
            raise

        return res


    def listFilesystems(self, device='all'):
        """List all filesystems, set self.gpfslocalfilesystems convenient dict
            structure of returned dict
                key = deviceName, value dict
                    with key = fieldName and values corresponding value
        """
        info = self._executeY('mmlsfs', [device])
        ## for v3.5 deviceName:fieldName:data:remarks:

        ## set the gpfsdevices
        gpfsdevices = list(set(info.get('deviceName', [])))
        if len(gpfsdevices) == 0:
            self.log.raiseException("No devices found. Returned info %s" % info)
        else:
            self.log.debug("listAllFilesystems found devices %s" % gpfsdevices)

        res = dict([(dev, {}) for dev in gpfsdevices]) ## build structure
        for dev, k, v in zip(info['deviceName'], info['fieldName'], info['data']):
            res[dev][k] = v

        self.gpfslocalfilesystems = res



    def listQuota(self, devices=None):
        """get quota info for all filesystems for all USR,GRP,FILESET
            set self.gpfslocalquota to
                dict: key = deviceName, value is
                    dict with key quotaType (USR | GRP | FILESET) value is dict with
                        key = id, value dict with
                            key = remaining header entries and corresponding values
        """
        if devices is None:
            devices = ['-a']
        elif isinstance(devices, str):
            devices = [devices]

        info = self._executeY('mmrepquota', ['-n', " ".join(devices)], prefix=True)
        ## for v3.5 filesystemName:quotaType:id:name:blockUsage:blockQuota:blockLimit:blockInDoubt:blockGrace:filesUsage:filesQuota:filesLimit:filesInDoubt:filesGrace:remarks:quota:defQuota:fid:filesetname:

        datakeys = info.keys()
        datakeys.remove('filesystemName')
        datakeys.remove('quotaType')
        datakeys.remove('id')

        fss = list(set(info.get('filesystemName', [])))

        quotatypes = list(set(info.get('quotaType', [])))
        quotatypesstruct = dict([(qt, {}) for qt in quotatypes])

        res = dict([(fs, quotatypesstruct) for fs in fss]) ## build structure

        for idx, (fs, qt, qid) in enumerate(zip(info['filesystemName'], info['quotaType'], info['id'])):
            details = dict([(k, info[k][idx]) for k in datakeys])
            res[fs][qt][qid] = details

        self.gpfslocalquotas = res

    def listFilesets(self, devices=None, filesetnames=None):
        """get all filesets for specific device
            @type devices: list of devices (if string: 1 device; if None: all found devices)
            @type filesetnames: report only on specific filesets (if string: 1 filesetname)

            set self.gpfslocalfileset is dict with
                key = filesystemName value is dict with
                    key = id value is dict
                        key = remaining header entries and corresponding values
        """
        opts = []

        if devices is None:
            ## get all devices from all filesystems
            if self.gpfslocalfilesystems is None:
                self.listFilesystems()

            devices = self.gpfslocalfilesystems.keys()
        else:
            if isinstance(devices, str):
                devices = [devices]
        opts.append(','.join(devices))


        if filesetnames is not None:
            if isinstance(filesetnames, str):
                filesetnames = [filesetnames]

            filesetnamestxt = ','.join(filesetnames)
            opts.append(filesetnamestxt)

        info = self._executeY('mmlsfileset', opts)
        ## for v3.5 filesystemName:filesetName:id:rootInode:status:path:parentId:created:inodes:dataInKB:comment:filesetMode:afmTarget:afmState:afmMode:afmFileLookupRefreshInterval:afmFileOpenRefreshInterval:afmDirLookupRefreshInterval:afmDirOpenRefreshInterval:afmAsyncDelay:reserved:afmExpirationTimeout:afmRPO:afmLastPSnapId:inodeSpace:isInodeSpaceOwner:maxInodes:allocInodes:inodeSpaceMask:afmShowHomeSnapshots:afmNumReadThreads:afmNumReadGWs:afmReadBufferSize:afmWriteBufferSize:afmReadSparseThreshold:afmParallelReadChunkSize:afmParallelReadThreshold:snapId:

        datakeys = info.keys()
        datakeys.remove('filesystemName')
        datakeys.remove('id')

        fss = list(set(info.get('filesystemName', [])))
        res = dict([(fs, {}) for fs in fss]) ## build structure

        for idx, (fs, qid) in enumerate(zip(info['filesystemName'], info['id'])):
            details = dict([(k, info[k][idx]) for k in datakeys])
            res[fs][qid] = details

        self.gpfslocalfilesets = res

    def _listDiskSingleDevice(self, device):
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

        ## sanity check

        commondomain = None
        ## if this fails, nodes probably have shortnames
        try:
            ## - means disk offline, so no nodename
            alldomains = ['.'.join(x.split('.')[1:]) for x in infoM['IOPerformedOnNode'] if not x in ['-', 'localhost']]
            if len(set(alldomains)) > 1:
                self.log.error("More then one domain found: %s." % alldomains)
            commondomain = alldomains[0] ## TODO: should be most frequent one
        except:
            self.log.exception("Can't determine domainname for nodes %s" % infoM['IOPerformedOnNode'])
            commondomain = None

        for idx, node in enumerate(infoM['IOPerformedOnNode']):
            if node == 'localhost':
                infoM['IOPerformedOnNode'][idx] = '.'.join([x for x in [shorthn, commondomain] if x is not None])

        res = dict([(nsd, {}) for nsd in infoL['nsdName']]) ## build structure
        for idx, nsd in enumerate(infoL['nsdName']):
            for k in keysL:
                res[nsd][k] = infoL[k][idx]
            for k in keysM:
                Mk = k
                if k in keysL:
                    ## duplicate key !!
                    if not infoL[k][idx] == infoM[k][idx]:
                        self.log.error("nsdName %s has named value %s in both -L and -M, but have different value L=%s M=%s" % (nsd, infoL[k][idx], infoM[k][idx]))
                    Mk = "M_%s" % k
                res[nsd][Mk] = infoM[k][idx]

        return res

    def listDisks(self, devices=None):
        """List all disks for devices (if devices is None, use all devices
            Return dict with
                key = device values is dict
                    key is disk, value is remaining property
        """
        if devices is None:
            ## get all devices from all filesystems
            if self.gpfslocalfilesystems is None:
                self.listFilesystems()

            devices = self.gpfslocalfilesystems.keys()
        else:
            if isinstance(devices, str):
                devices = [devices]

        res = {}
        for device in devices:
            devinfo = self._listDiskSingleDevice(device)
            res[device] = devinfo

        self.gpfsdisks = res

    def listNsds(self):
        """List NSD info
            Not implemented due to missing -Y option of mmlsnsd
        """
        self.log.error("listNsds not implemeneted.")


    def getAttr(self, obj=None):
        """
        mmlsattr on obj to get GFPS details on file or directory
        """
        obj = self._sanityCheck(obj)

        if not self.exists(obj):
            self.raiseException("getAttr: obj %s does not exist")

        ec, out = self._execute('mmlsattr', ["-L", obj])
        if ec > 0:
            self.log.raiseException("getAttr: mmlsattr with opts -L %s failed" % (obj))

        res = {}

        for line in out.split("\n"):
            line = re.sub(r"\s+", '', line)
            if len(line) == 0: continue
            items = line.split(":")
            if len(items) == 1:
                items.append('') ## fix anomalies
            res[items[0]] = ":".join(items[1:]) ## creationtime has : in value as well eg creationtime:ThuAug2313:04:202012

        return res


    def getDetails(self, obj=None):
        """Given obj, return as much relevant info as possible
        """
        obj = self._sanityCheck(obj)

        res = {'parent':None}
        res['exists'] = self.exists(obj)

        if res['exists']:
            realpath = obj
        else:
            realpath = self._largestExistingPath()
            res['parent'] = realpath

        fs = self._whatFilesystem(obj)
        res['fs'] = fs

        res['attrs'] = self.getAttr(obj)

        return res

    def makeFileset(self, newfilesetpath, fsetname=None, afm=None):
        """
        Given path, create a new fileset and link it to said path
          - check uniqueness
          - set comment ?
        """
        """
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
        self.listFilesets() ## get all info uptodate

        self.listFilesystems() ## get known filesystems

        fsetpath = self._sanityCheck(newfilesetpath)
        ## does the path exist ?
        if self.exists(fsetpath):
            self.log.raiseException("makeFileset for newfilesetpath %s returned sane fsetpath %s, but it already exists." % (newfilesetpath, fsetpath))
        ## choose unique name
        parentfsetpath = os.path.dirname(fsetpath)
        if not self.exists(parentfsetpath):
            self.log.raiseException("parent dir %s of fsetpath %s does not exist. Not going to create it automatically." % (parentfsetpath, fsetpath))

        fs = self.whatFilesystem(parentfsetpath)
        foundgpfsdevice = fs[self.localfilesystemnaming.index('gpfsdevice')]

        if fsetname is None:
            ## guess the device from the pathname
            ## subtract the device mount path from filesetpath ? (what with filesets in filesets)
            mntpt = fs[self.localfilesystemnaming.index('mountpoint')]
            if fsetpath.startswith(mntpt):
                lastpart = fsetpath.split(os.sep)[len(mntpt.split(os.sep)):]
                fsetname = "_".join(lastpart)
            else:
                fsetname = os.path.basedir(fsetpath)
                self.log.error("fsetpath %s doesn't start with mntpt %s. using basedir %s" % (fsetpath, mntpt, fsetname))

        for efsetid, efset in self.gpfslocalfilesets[foundgpfsdevice].items():
            ## is there one with same path or with same name ?
            efsetpath = efset.get('path', None)
            efsetname = efset.get('filesetName', None)
            if efsetpath == fsetpath or efsetname == fsetname:
                self.log.raiseException("Found existing fileset %s that has same path %s or same name %s as new path %s or new name %s" % (efset, efsetpath, efsetname, fsetpath, fsetname))

        ## create the fileset
        ## -- what with --inode-space (it can't be changed later) ?
        ## if created, try to link it with -J to path
        ec, out = self._execute('mmcrfileset', [foundgpfsdevice, fsetname])
        if ec > 0:
            self.log.raiseException("Creating fileset with name %s on device %s failed" % (fsetname, foundgpfsdevice))

        ## link the fileset
        ec, out = self._execute('mmlinkfileset', [foundgpfsdevice, fsetname, '-J', fsetpath])
        if ec > 0:
            self.log.raiseException("Linking fileset with name %s on device %s to path %s failed" % (fsetname, foundgpfsdevice, fsetpath))

        ## at the end, rescan the filesets and update the info
        self.listFilesets()

    def setQuota(self, soft, who, obj=None, typ='user', hard=None, grace=None):
        """Set quota: set softlimit for type typ on obj
            @type soft: int, number of bytes as softlimit
            @type who: identifier (eg username or userid) (is redefined with filesetname from mmlsattr for typ=fileset)
            @type grace: int, grace period in seconds for the whole type, not per user !!

            current implementation only set block limits
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

        obj = self._sanityCheck(obj)
        if not self.exists(obj):
            self.raiseException("setQuota: can't set quota on none-existing obj %s" % obj)

        typ2opt = {'user':'u',
                   'group':'g',
                   'fileset':'j',
                   }

        soft2hard_factor = 1.05

        if not typ in typ2opt:
            self.log.raiseException("setQuota: unsupported type %s" % typ)

        if typ == 'fileset':
            ## who is the fileset name or fsid
            attr = self.getAttr(obj)
            if 'filesetname' in attr:
                who = attr['filesetname']  ## force it
                self.log.info("setQuota: typ %s setting fileset to %s for obj %s" % (typ, who, obj))
            else:
                self.log.raiseException("setQuota: typ %s specified, but attrs for obj %s don't have filestename property (attr: %s)" % (typ, obj, attr))

        opts = []

        if grace is None:
            if hard is None:
                hard = int(soft * soft2hard_factor)
            elif hard < soft:
                self.raiseException("setQuota: can't set hard limit %s lower then soft limit %s" % (hard, soft))


            opts += ["-%s" % typ2opt[typ], who]
            opts += ["-s", "%sm" % int(soft // 1024 ** 2)]  ## round to MB
            opts += ["-h", "%sm" % int(hard // 1024 ** 2)]  ## round to MB
        else:
            ## only set grace period
            opts += ["-%s" % typ2opt[typ]]
            opts += ["-t", "%sseconds" % int(grace)]

        opts.append(obj)

        ec, out = self._execute('tssetquota', opts)
        if ec > 0:
            self.log.raiseException("setQuota: tssetquota with opts %s failed" % (opts))


if __name__ == '__main__':
    g = GpfsOperations()

    g.listFilesystems()
    print "fs", g.gpfslocalfilesystems

    g.listQuota()
    print "quota", g.gpfslocalquotas

    g.listFilesets()
    print "filesets", g.gpfslocalfilesets

    g.listDisks()
    print "disks", g.gpfsdisks


