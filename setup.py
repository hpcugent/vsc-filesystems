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
    vsc-base
    vsc-mympirun
    ...

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
import distutils.command.install_scripts
import shutil
import os


class vsc_install_scripts(distutils.command.install_scripts.install_scripts):
    """Create the (fake) links for mympirun
        also remove .sh and .py extensions from the scripts
    """
    def __init__(self, *args):
        distutils.command.install_scripts.install_scripts.__init__(self, *args)
        self.original_outfiles = None

    def run(self):
        ## old-style class
        distutils.command.install_scripts.install_scripts.run(self)
        self.original_outfiles = self.get_outputs()[:] ## make a copy
        self.outfiles = [] ## reset it
        for script in self.original_outfiles:
            ## remove suffixes for .py and .sh
            if script.endswith(".py") or script.endswith(".sh"):
                shutil.move(script, script[:-3])
                script = script[:-3]
            self.outfiles.append(script)


## authors
sdw = ('Stijn De Weirdt', 'stijn.deweirdt@ugent.be')
jt = ('Jens Timmermans', 'jens.timmermans@ugent.be')
kh = ('Kenneth Hoste', 'kenneth.hoste@ugent.be')
ag = ('Andy Georges', 'andy.georges@ugent.be')
wdp = ('Wouter Depypere', 'wouter.depypere@ugent.be')
lm = ('Luis Fernando Munoz Meji­as', 'luis.munoz@ugent.be')

## shared target config
SHARED_TARGET = {'url': 'http://hpcugent.github.com/VSC-tools',
                 'download_url': 'https://github.com/hpcugent/VSC-tools',
                 'package_dir': {'': 'lib'},
                 'cmdclass': {"install_scripts": vsc_install_scripts}
                 }

## meta-package for allinone target
VSC_ALLINONE = {'name': 'python-vsc-tools',
                'version': '0.0.1',
                }

VSC_FILESYSTEMS = {'name': 'vsc-filesystems',
                   'version': '0.1',
                   'author': [sdw],
                   'maintainer': [sdw],
                   'packages': ['vsc/filesystems'],
                   'py_modules': ['vsc/filesystems/gpfs',
                                  'vsc/filesystems/posix'],
                   'scripts': []
                   }


############################################################################################
###
### THE BELOW SHOULD NOT BE TOUCHED
###
### BUILDING
###
from distutils.core import setup
import sys


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


all_targets = [VSC_ALLINONE]
registered_names = ['vsc-all', 'vsc-allinone'] + [x['name'] for x in all_targets]

envname = 'VSC_TOOLS_SETUP_TARGET'
tobuild = os.environ.get(envname, 'vsc-all')  ## default all
if sys.argv[1] == 'vsc-showall':
    print "Valid targets: %s" % " ".join(registered_names)
    sys.exit(0)
elif sys.argv[1] in registered_names:
    tobuild = sys.argv[1]
    sys.argv.pop(1)

## create allinone / vsc-tools target
for target in all_targets:
    for k, v in target.items():
        if k in ('name', 'version',):
            continue
        if not k in VSC_ALLINONE:
            VSC_ALLINONE[k] = v
        else:
            if isinstance(v, list):
                VSC_ALLINONE[k] += v
            else:
                print 'ERROR: unsupported type cfgname %s key %s value %s' % (target['name'], k, v)
                sys.exit(1)
## sanitize allinone/vsc-tools
for k, v in VSC_ALLINONE.items():
    if isinstance(v, list):
        VSC_ALLINONE[k] = list(set(VSC_ALLINONE[k]))
        VSC_ALLINONE[k].sort()


if tobuild == 'vsc-allinone':
    ## reset all_targets
    all_targets = [VSC_ALLINONE]

## build what ?
for target in all_targets:
    target_name = target['name']
    if (tobuild is not None) and not (tobuild in ('vsc-all', 'vsc-allinone' , target_name,)):
        continue
    if tobuild == 'vsc-all' and target_name == 'vsc-tools':
        ## vsc-tools / allinone is not a default when vsc-all is selected
        continue

    ## froim now on, build the exact targets.
    os.environ[envname] = target_name
    os.putenv(envname, target_name)

    x = parse_target(target)
    setup(**x)
