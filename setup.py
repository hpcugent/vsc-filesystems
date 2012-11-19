#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
# Copyright 2009-2012 Stijn De Weirdt
# Copyright 2012 Andy Georges
#
# This file is part of VSC-tools,
# originally created by the HPC team of the University of Ghent (http://ugent.be/hpc).
#
#
# http://github.com/hpcugent/VSC-tools
#
# VSC-tools is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
#
# EasyBuild is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with VSC-tools. If not, see <http://www.gnu.org/licenses/>.
##

"""
VSC-tools distribution setup.py

Usage:
------
python setup.py [<targetname>] <usual distutils commands and options>

targetname can be:
    vsc-all : build all (default if no target is specified)
    vsc-allinone : build all as single target (ie 1 tarball/spec/rpm)
    vsc-showall : just show all possible targets


    vsc-tools ## this is the name of the allinone target
    vsc-administration
    vsc-core
    vsc-globfs
    vsc-gpfs
    vsc-icingadb
    vsc-postgres
    vsc-utils

Warning:
--------
To utilise the fake mpirun wrapper,
 - mympirun must be installed in path that resembles SOME_PREFIX/mympirun.*?/bin/
 - the fake path SOME_PREFIX/mympirun.*?/bin/fake must be added to the PATH variable
     so it is found before the real mpirun
 - this should be fine when installing with --prefix or --user

Example usage:
--------------
build all and install in user space (~/.local/lib/python2.X/site-packages)
    python setup.py build -b /tmp/vsc$USER install --user

build under /tmp/vsc, install with prefix /tmp/vsc/testinstall
(scripts in PREFIX/bin; python modules under PREFIX/lib/python2.X/site-packages)
    python setup.py vsc-mympirun clean build -b /tmp/vsc install --prefix /tmp/vsc/testinstall

make rpm for all targets at once (works for single target)
    python setup.py clean sdist -d /tmp/vsc$USER bdist_rpm --bdist-base /tmp/vsc$USER -d /tmp/vsc$USER clean ; rm -f MANIFEST

TODO:
    create http://hpcugent.github.com/VSC-tools page, short description per package ?

"""
#import distutils.command.install_scripts
import os
import shutil
import sys

from collections import defaultdict
from distutils import log

try:
    ## setuptools makes copies of the scripts, does not preserve symlinks
    #raise("a")  # to try distutils, uncomment
    from setuptools import setup
    from setuptools.command.install_scripts import install_scripts
    from setuptools.command.easy_install import easy_install
    #from setuptools.distutils.dir_util import remove_tree
except:
    from distutils.core import setup
    from distutils.command.install_scripts import install_scripts
    easy_install = object

from distutils.dir_util import remove_tree

## 0 : WARN (default), 1 : INFO, 2 : DEBUG
log.set_verbosity(2)


class vsc_easy_install(easy_install):
    def install_egg_scripts(self, dist):
        easy_install.install_egg_scripts(self, dist)


class vsc_install_scripts(install_scripts):
    """Create the (fake) links for mympirun
        also remove .sh and .py extensions from the scripts
    """
    def __init__(self, *args):
        install_scripts.__init__(self, *args)
        self.original_outfiles = None

    def run(self):
        # old-style class
        install_scripts.run(self)
        self.original_outfiles = self.get_outputs()[:]  # make a copy
        self.outfiles = []  # reset it
        for script in self.original_outfiles:
            # remove suffixes for .py and .sh
            if script.endswith(".py") or script.endswith(".sh"):
                shutil.move(script, script[:-3])
                script = script[:-3]
            self.outfiles.append(script)


# authors
sdw = ('Stijn De Weirdt', 'stijn.deweirdt@ugent.be')
jt = ('Jens Timmermans', 'jens.timmermans@ugent.be')
kh = ('Kenneth Hoste', 'kenneth.hoste@ugent.be')
ag = ('Andy Georges', 'andy.georges@ugent.be')
wdp = ('Wouter Depypere', 'wouter.depypere@ugent.be')
lm = ('Luis Fernando Munoz Meji­as', 'luis.munoz@ugent.be')

# shared target config
SHARED_TARGET = {
    'url': 'http://hpcugent.github.com/VSC-tools',
    'download_url': 'https://github.com/hpcugent/VSC-tools',
    'package_dir': {'': 'lib'},
    'cmdclass': {'install_scripts': vsc_install_scripts,
                 'easy_install': vsc_easy_install
                 }
}

# meta-package for allinone target
VSC_ALLINONE = {
    'name': 'python-vsc-tools',
    'version': '0.0.1',
}

VSC_ = {
    'name': 'vsc-',
    'version': '',
    'author': [ag],
    'maintainer': [],
    'packages': [],
    'py_modules': [
    ],
    'scripts': []
}

VSC_ADMINISTRATION = {
    'name': 'vsc-administration',
    'version': '0.1',
    'author': [ag],
    'maintainer': [ag],
    #'packages': ['vsc.administration'],
    'py_modules': [
        'vsc.__init__',
        'vsc.administration.group',
        'vsc.administration.institute',
        'vsc.administration.tools',
        'vsc.administration.user',
        'vsc.administration.vo',
    ],
    'scripts': []
}

VSC_CORE = {
    'name': 'vsc-core',
    'version': '0.3',
    'author': [sdw, ag],
    'maintainer': [sdw, ag],
    'namespace_packages': ['vsc', 'vsc.config'],
    'py_modules': [
        'vsc.__init__',
        'vsc.config.base',
        'vsc.exceptions',
        'vsc.utils.patterns',
    ],
    'scripts': []
}

VSC_FILESYSTEMS = {
    'name': 'vsc-filesystems',
    'version': '0.1',
    'author': [sdw],
    'maintainer': [sdw],
    'packages': ['vsc.filesystem'],
    'namespace_packages': ['vsc'],
    'py_modules': [
        'vsc.__init__',
        'vsc.filesystem.__init__',
        'vsc.filesystem.gpfs',
        'vsc.filesystem.posix'
    ],
    'scripts': []
}

VSC_GLOBFS = {
    'name': 'vsc-globfs',
    'version': '',
    'author': [ag, sdw],
    'maintainer': [ag, sdw],
    'packages': ['vsc.globfs'],
    'namespace_packages': ['vsc', 'vsc.globfs'],
    'py_modules': [
        'vsc.__init__',
        'vsc.globfs.hpccollector',
        'vsc.globfs.moab'
    ],
    'scripts': []
}

VSC_GPFS = {
    'name': 'vsc-gpfs',
    'version': '0.1.10',
    'author': [ag],
    'maintainer': [ag],
    'packages': ['vsc.gpfs'],
    'namespace_packages': ['vsc', 'vsc.gpfs.quota', 'vsc.gpfs.utils'],
    'py_modules': [
        'vsc.__init__',
        'vsc.gpfs.quota.entities',
        'vsc.gpfs.quota.fs_store',
        'vsc.gpfs.quota.report',
        'vsc.gpfs.utils.exceptions',
        'vsc.gpfs.utils.vo_utils',
    ],
    'scripts': []
}

VSC_ICINGADB = {
    'name': 'vsc-icingadb',
    'version': '0.10',
    'author': [wdp],
    'maintainer': [wdp],
    'packages': ['vsc.icingadb'],
    'namespace_packages': ['vsc', 'vsc.icingadb'],
    'py_modules': [
        'vsc.__init__',
        '.vsc.icingadb.icingadb',
        '.vsc.icingadb.showdb'
    ],
    'scripts': []
}

VSC_LDAP_CONFIGURATION = {
    'name': 'vsc-ldap-configuration',
    'version': '0.1',
    'author': [ag],
    'maintainer': [ag],
    'packages': ['vsc.ldap'],
    'namespace_packages': ['vsc', 'vsc.ldap'],
    'py_modules': [
        'vsc.__init__',
        'vsc.ldap.configuration'
    ],
    'scripts': []
}

# FIXME: Check is this can be downloaded directly from PyPi
VSC_LOCKFILE = {
    'name': 'vsc-lockfile',
    'version': '0.9.1.1',
    'author': [ag],
    'maintainer': [ag],
    'packages': ['lockfile'],
    'py_modules': [
        'lockfile.linklockfile',
        'lockfile.mkdirlockfile',
        'lockfile.pidlockfile',
        'lockfile.sqlitelockfile',
    ],
    'scripts': []
}

VSC_POSTGRES = {
    'name': 'vsc-postgres',
    'version': '0.1.1',
    'author': [wdp],
    'maintainer': [],
    'namespace_packages': ['vsc'],
    'py_modules': [
        'vsc.__init__',
        'vsc.pg'
    ],
    'scripts': []
}

VSC_UTILS = {
    'name': 'vsc-utils',
    'version': '0.10',
    'author': [ag, sdw],
    'maintainer': [ag, sdw],
    'install_requires': ['lockfile>=0.9.1.1']
    'packages': ['vsc.utils'],
    'namespace_packages': ['vsc'],
    'py_modules': [
        'vsc.__init__',
        'vsc.utils.crypt',
        'vsc.utils.filesystem',
        'vsc.utils.fs_store',
        'vsc.utils.pickle_files',
        'vsc.utils.timestamp_pid_lockfile'
    ],
    'scripts': []
}


def get_all_targets():
    return [
        VSC_ALLINONE,
        VSC_ADMINISTRATION,
        VSC_CORE,
        VSC_FILESYSTEMS,
        VSC_GLOBFS,
        VSC_GPFS,
        VSC_ICINGADB,
        VSC_LDAP_CONFIGURATION,
        VSC_POSTGRES,
        VSC_UTILS,
    ]

############################################################################################
###
### THE BELOW SHOULD NOT BE TOUCHED
###
### BUILDING
###


def parse_target(target):
    """Add some fields"""
    new_target = {}
    new_target.update(SHARED_TARGET)
    for k, v in target.items():
        if k in ('author', 'maintainer'):
            if not isinstance(v, list):
                print "ERROR: %s of config %s needs to be a list (not tuple or string)" % (k, target['name'])
                sys.exit(1)
            new_target[k] = ";".join([x[0] for x in v])
            new_target["%s_email" % k] = ";".join([x[1] for x in v])
        else:
            new_target[k] = v
    return new_target


def create_all_in_one_target(all_targets):
    """Creates the complete target set, creating a sictionary to install all targets in a single go."""

    all_in_one_target = defaultdict(list)
    all_in_one_target.update(VSC_ALLINONE)  # default

    for target in all_targets:
        for k, v in target.items():
            if k in ['name', 'version']:
                continue
            if isinstance(v, list):
                all_in_one_target[k] += v
            else:
                print 'ERROR: unsupported type cfgname %s key %s value %s' % (target['name'], k, v)
                sys.exit(1)
    ## sanitize allinone/vsc-tools
    for k, v in all_in_one_target.items():
        if isinstance(v, list):
            all_in_one_target[k] = list(set(all_in_one_target[k]))
            all_in_one_target[k].sort()

    return all_in_one_target


def main(args):

    all_targets = get_all_targets()
    registered_names = ['vsc-all', 'vsc-allinone'] + [x['name'] for x in all_targets]

    envname = 'VSC_TOOLS_SETUP_TARGET'
    tobuild = os.environ.get(envname, 'vsc-all')  # default all

    if args[1] == 'vsc-showall':
        print "Valid targets: %s" % " ".join(registered_names)
        sys.exit(0)
    elif args[1] in registered_names:
        tobuild = args[1]
        args.pop(1)

    log.info("main: going to build %s (set through env: %s)" % (tobuild, envname in os.environ))

    all_in_one_target = create_all_in_one_target(all_targets)

    if tobuild == 'vsc-allinone':
        # reset all_targets
        all_targets = [all_in_one_target]

    log.info("main: all targets are: %s" % (all_targets))

    # build what ?
    for target in all_targets:
        target_name = target['name']

        log.info("main: Checking if we should build target with name %s" % (target_name))
        log.debug("main: tobuild = %s; target_name = %s" % (tobuild, target_name))

        if (tobuild is not None) and not (tobuild in ('vsc-all', 'vsc-allinone', target_name,)):
            print "continuing from 1"
            continue
        if tobuild == 'vsc-all' and target_name == 'python-vsc-tools':
            # vsc-tools / allinone is not a default when vsc-all is selected
            continue
            print "continuing from 2"

        ## from now on, build the exact targets.
        os.environ[envname] = target_name
        os.putenv(envname, target_name)

        x = parse_target(target)

        log.info("Target information: %s" % (x))

        try:
            remove_tree('build')
        except OSError, _:
            pass
        setup(**x)


if __name__ == '__main__':
    main(sys.argv)
