"""
20/08/2012 Stijn De Weirdt HPC UGent-VSC

General POSIX filesystem interaction (sort of replacement for linux_utils)
"""


import commands
import errno
import os
import sys
import subprocess

import vsc.fancylogger as fancylogger
OS_LINUX_MOUNTS = '/proc/mounts'
OS_LINUX_FILESYSTEMS = '/proc/filesystems'
# be very careful to add new ones here
OS_LINUX_IGNORE_FILESYSTEMS = ('rootfs',  # special initramfs filesystem
                               'configfs',  # kernel config
                               'debugfs',  # kernel debug
                               'usbfs',  # usb devices
                               'ipathfs',  # qlogic IB
                               'binfmt_misc',  # ?
                               'rpc_pipefs',  # NFS RPC
                               )


class PosixOperationError(Exception):
    pass


class PosixOperations(object):
    """
    Class to create objects in filesystem, with various properties
    """

    def __init__(self):
        self.log = fancylogger.getLogger(name=self.__class__.__name__)

        self.obj = None  # base object (file or directory or whatever)

        self.forceabsolutepath = True

        self.brokenlinkexists = True  # does a broken symlink count as existing file ?
        self.ignorefilesystems = True  # remove filesystems to be ignored from detected filesystem
        self.ignorerealpathmismatch = False  # ignore mismatches between the obj and realpath(obj)

        self.localfilesystems = None
        self.localfilesystemnaming = None

        self.supportedfilesystems = []  # nothing generic, mostly due to missing generalised quota settings

        self.dry_run = False

    def _execute(self, cmd, changes=False):
        """Run command cmd, return exitcode,output"""
        ec = None
        res = None

        shell = False
        if isinstance(cmd, (tuple, list,)):
            # cmd in non-shell
            shell = False
        elif isinstance(cmd, str):
            shell = True
        else:
            self.log.error("_execute unsupported type %s of cmd %s" % (type(cmd), cmd))
            return ec, res

        if changes and self.dry_run:
            self.log.info("Dry run: not really executing cmd %s" % (cmd))
            return 0, ""

        # Executes and captures the output of a succesful run.
        if sys.hexversion >= 0x020700F0:
            try:
                out = subprocess.check_output(cmd, shell=shell)
                ec = 0
            except Exception, err:
                ec = err.returncode
                out = "%s" % err
                self.log.exception("_execute command [%s] failed: ec %s" % (cmd, ec))
        else:
            if shell:
                cmdtxt = cmd
            else:
                cmdtxt = " ".join(["%s" % x for x in cmd])
                self.log.debug("_execute converted cmd %s in cmdtxt %s" % (cmd, cmdtxt))

            (ec, out) = commands.getstatusoutput("%s" % cmdtxt)
            if ec > 0:
                self.log.exception("_execute command [%s] failed: ec %s" % (cmd, ec))

        return ec, out

    def _sanity_check(self, obj=None):
        """Run sanity check on obj. E.g. force absolute path.
            @type obj: string to check
        """
        # filler for reducing lots of LOC
        if obj is None:
            if self.obj is None:
                self.log.raiseException("_sanity_check no obj passed and self.obj not set.", PosixOperationError)
                return
            else:
                obj = self.obj

        obj = obj.rstrip('/')  # remove trailing /

        if self.forceabsolutepath:
            if not os.path.isabs(obj):  # other test: obj.startswith(os.path.sep)
                self.log.raiseException("_sanity_check check absolute path: obj %s is not an absolute path" % obj, PosixOperationError)
                return

        # check if filesystem matches current class
        fs = None
        pts = obj.split(os.path.sep)  # used to determine maximum number of steps to check
        for x in range(len(pts)):
            fp = os.path.sep.join(pts[::-1][x:][::-1])
            if fs is None and self._exists(fp):
                tmpfs = self._what_filesystem(fp)
                if tmpfs is None:
                    continue

                if tmpfs[0] in self.supportedfilesystems:
                    fs = tmpfs[0]
                else:
                    self.log.raiseException("_sanity_check found filesystem %s for subpath %s of obj %s is not a supported filesystem (supported %s)" % (tmpfs[0], fp, obj, self.supportedfilesystems), PosixOperationError)

        if fs is None:
            self.log.raiseException("_sanity_check no valid filesystem found for obj %s" % obj, PosixOperationError)

        # try readlink
        if not obj == os.path.realpath(obj):
            # some part of the path is a symlink
            if self.ignorerealpathmismatch:
                self.log.debug("_sanity_check obj %s doesn't correspond with realpath %s" % (obj, os.path.realpath(obj)))
            else:
                self.log.raiseException("_sanity_check obj %s doesn't correspond with realpath %s" % (obj, os.path.realpath(obj)), PosixOperationError)
                return

        return obj

    def set_object(self, obj):
        """Set the object, apply checks if needed
            @type obj: string to set as obj
        """
        self.obj = self._sanity_check(obj)

    def exists(self, obj=None):
        """Based on obj, check if obj exists or not"""
        self.obj = self._sanity_check(obj)
        return self._exists(obj)

    def _exists(self, obj):
        """Based on obj, check if obj exists or not
            called by _sanity_check and exists  or with sanitised obj
        """
        if obj is None:
            self.log.raiseException("_exists: obj is None", PosixOperationError)

        if os.path.exists(obj):
            return True

        # os.path.exists returns False for broken links
        if self.brokenlinkexists:
            # check if obj is a link
            return self._isbrokenlink(obj)
        else:
            return False

    def isbrokenlink(self, obj=None):
        """Check is obj is borken link"""
        self.obj = self._sanity_check(obj)
        return self._isbrokenlink(obj)

    def _isbrokenlink(self, obj):
        """Check is obj is broken link. Called from _exist or with sanitised obj"""
        res = (not os.path.exists(obj)) and os.path.islink(obj)
        if res:
            self.log.error("_isbrokenlink: found broken link for %s" % obj)
        return res

    def what_filesystem(self, obj=None):
        """Based on obj, determine underlying filesystem as much as possible"""
        self.obj = self._sanity_check(obj)
        return self._what_filesystem(obj)

    def _what_filesystem(self, obj):
        if not self._exists(obj):  # obj is sanitised
            self.log.error("_whatFilesystem: obj %s does not exist" % obj)
            return

        if self.localfilesystems is None:
            self._local_filesystems()

        try:
            fsid = os.stat(obj).st_dev  # this resolves symlinks
        except:
            self.log.exception("Failed to get fsid from obj %s" % obj)
            return

        fss = [x for x in self.localfilesystems if x[self.localfilesystemnaming.index('id')] == fsid]

        if len(fss) == 0:
            self.log.raiseException("No matching filesystem found for obj %s with id %s (localfilesystems: %s)" % (obj, fsid, self.localfilesystems), PosixOperationError)
        elif len(fss) > 1:
            self.log.raiseException("More then one matching filesystem found for obj %s with id %s (matched localfilesystems: %s)" % (obj, fsid, fss), PosixOperationError)
        else:
            self.log.debug("Found filesystem for obj %s: %s" % (obj, fss[0]))
            return fss[0]

    def _local_filesystems(self):
        """What filesystems are mounted / available atm"""
        # what is currently mounted
        if not os.path.isfile(OS_LINUX_MOUNTS):
            self.log.raiseException("Missing Linux OS overview of mounts %s" % OS_LINUX_MOUNTS, PosixOperationError)
        if not os.path.isfile(OS_LINUX_FILESYSTEMS):
            self.log.raiseException("Missing Linux OS overview of filesystems %s" % OS_LINUX_FILESYSTEMS, PosixOperationError)

        try:
            currentmounts = [x.strip().split(" ") for x in open(OS_LINUX_MOUNTS).readlines()]
            # returns [('rootfs', '/', 2051L, 'rootfs'), ('ext4', '/', 2051L, '/dev/root'), ('tmpfs', '/dev', 17L, '/dev'), ...
            self.localfilesystemnaming = ['type', 'mountpoint', 'id', 'device']
            self.localfilesystems = [[y[2], y[1], os.stat(y[1]).st_dev, y[0]] for y in currentmounts]
        except:
            self.log.exception("Failed to create the list of current mounted filesystems")
            raise

        # do we need further parsing, eg of autofs types or remove pseudo filesystems ?
        if self.ignorefilesystems:
            self.localfilesystems = [x for x in self.localfilesystems if not x[self.localfilesystemnaming.index('type')] in OS_LINUX_IGNORE_FILESYSTEMS]

    def _largest_existing_path(self, obj):
        """Given obj /a/b/c/d, check which subpath exists and will determine eg filesystem type of obj.
            Start with /a/b/c/d, then /a/b/c etc
        """
        obj = self._sanity_check(obj)

        res = None
        pts = obj.split(os.path.sep)  # used to determine maximum number of steps to check
        for x in range(len(pts)):
            fp = os.path.sep.join(pts[::-1][x:][::-1])
            if res is None and self.exists(fp):
                res = fp  # could be broken symlink
                if self.isbrokenlink(fp):
                    self.log.error("_largestExistingPath found broken link %s for obj %s" % (res, obj))
                break

        return res

    def make_symlink(self, target, obj=None, force=True):
        """Create symlink from self.obj to target
            @type target: string representing path
        """
        target = self._sanity_check(target)
        obj = self._sanity_check(obj)

        if os.path.exists(target):
            if os.path.islink(target):
                if force:
                    self.log.warning("Target %s is a symlink, removing" % (target))
                    target_ = os.realpath(target)
                    os.unlink(target)
                    target = self._sanity_check(target_)
        else:
            self.log.raiseException("Target %s does not exist, cannot make symlink to it" % (target),
                                    PosixOperationError)

        self.log.info("Creating symlink from %s to %s" % (obj, target))
        try:
            os.symlink(target, obj)
        except OSError, err:
            self.log.raiseException("Cannot create symlink from %s to %s" % (obj, target), PosixOperationError)

    def is_dir(self, obj=None):
        """Check if it is a directory"""
        obj = self._sanity_check(obj)
        # do symlinks count ?

    def make_dir(self, obj=None):
        """Make a directory hierarchy.

        @type obj: string representing a path to the final directory in the hierarchy

        @raise PosixOperationError: if the directory does ot exist and cannot be created
        """
        obj = self._sanity_check(obj)
        try:
            os.makedirs(obj)
        except OSError, err:
            if err.errno == errno.EEXIST:
                pass
            else:
                self.log.raiseException("Cannot create the directory hierarchy %s" % (obj), PosixOperationError)

    def make_home_dir(self, obj=None):
        """Make a homedirectory"""
        obj = self._sanity_check(obj)
        self.make_dir(obj)
        # (re?)generate default key
        # create .ssh/authorized_keys (+default key)
        # generate ~/.bashrc / ~/.tcshrc or whatever we support

    def populate_home_dir(self, user_id, group_id, home_dir, ssh_public_keys):
        """Populate the home directory with the required files to allow the user to login.

        - (re)generate the default key
        - .ssh/authorized_keys (+default key)
        - .bashrc or whatever shell we support

        @type user_id: numerical user id
        @type group_id: numerical group id
        @type home_dir: string representing the path to the home directory (or whatever symlinks to it)
        @type ssh_public_keys: list of strings representing the public ssh keys
        """
        # ssh
        self.log.info("Populating home %s for user %s:%s" % (home_dir, user_id, group_id))
        ssh_path = os.path.join(home_dir, '.ssh')
        self.make_dir(ssh_path)

        self.log.info("Placing %d ssh public keys in the authorized keys file." % (len(ssh_public_keys)))
        fp = open(os.path.join(self.homeDirectory, '.ssh', 'authorized_keys'), 'w')
        for key in ssh_public_keys:
            fp.write(key + "\n")
        fp.close()
        self.chmod(ssh_path, 0644)

        # bash
        self.log.info('Creating .bashrc and .bash_profile')
        open(os.path.join(home_dir, '.bashrc')).close()
        fp = open(os.path.join(home_dir), '.bash_profile')
        fp.write('if [ -f ~/.bashrc ]; then\n . ~/.bashrc\nfi\n')
        fp.close()

        for f in [os.path.join(home_dir, '.ssh'),
                  os.path.join(home_dir, '.ssh', 'authorized_keys'),
                  os.path.join(home_dir, '.bashrc'),
                  os.path.join(home_dir, '.bash_profile')]:
            self.log.info("Changing ownership of %s to %s:%s" % (f, self.uidNumber, self.gidNumber))
            try:
                os.chown(f, self.uidNumber, self.gidNumber)
            except OSError, _:
                self.log.raiseException("Cannot change ownership of file %s to %s:%s" %
                                        (f, self.uidNumber, self.gidNumber), PosixOperationError)

    def list_quota(self, obj=None):
        """Report on quota"""
        obj = self._sanity_check(obj)
        self.log.error("listQuota not implemented for this class %s" % self.__class__.__name__)

    def set_quota(self, soft, who, obj=None, typ='user', hard=None, grace=None):
        """Set quota
            @type soft: int, soft limit in bytes
            @type who: identifier (eg username or userid)
            @type grace: int, grace period in seconds
        """
        obj = self._sanity_check(obj)
        self.log.error("setQuota not implemented for this class %s" % self.__class__.__name__)

    def chown(self, owner, group=None, obj=None):
        """Change ownership of the object"""
        obj = self._sanity_check(obj)

        self.log.info("Changing ownership of %s to %s:%s" % (obj, owner, group))
        try:
            os.chown(obj, owner, group)
        except OSError, _:
            self.log.raiseException("Cannot change ownership of object %s to %s:%s" % (obj, owner, group),
                                    PosixOperationError)

    def chmod(self, permissions, obj=None):
        """Change permissions on the object.

        @type permissions: octal number representing the permissions (rwxrwxrwx).
        @type obj: the object of which to checge the permissions
        """
        obj = self._sanity_check(obj)

        self.log.info("Changing access permission of %s to %o" % (obj, permissions))

        try:
            os.chmod(obj, permissions)
        except OSError, err:
            self.log.raiseException("Could not change the permissions on object %s to %o" % (obj, permissions),
                                    PosixOperationError)

    def compare_files(self, target, obj=None):
        target = self._sanity_check(target)
        obj = self._sanity_check(obj)

    def remove_obj(self, obj=None):
        """Remove obj"""
        obj = self._sanity_check(obj)
        # if backup, take backup
        # if real, remove

    def rename_obj(self, obj=None):
        """Rename obj"""
        obj = self._sanity_check(obj)
        # if backup, take backup
        # if real, rename
