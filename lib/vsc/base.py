#!/usr/bin/env python
##
#
# Copyright 2010-2012 Stijn De Weirdt
# Copyright 2012 Andy Georges
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://hpc.ugent.be).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
#!/usr/bin/env python
"""
library with vsc utils

30/01/2010 SDW UGent-VSC

most functionality similar to hpc-user-code tools

"""
## FIXME: - use a decent configuration file setup.
##        - should be used by the administration part?
import re
import os

import vsc.fancylogger as fancylogger

from vsc.utils.patterns import Singleton


class VSC:

    __metaclass__ = Singleton

    def __init__(self, read_cfg=True):
        self.log = fancylogger.getLogger(name=self.__class__.__name__)

        self.cfgname = self.__get_config_name()
        self.cfg = {}
        self.pwd = {}

        self.group_dn_base = "ou=central,ou=groups,dc=vscentrum,dc=be"
        self.user_dn_base = "ou=central,ou=people,dc=vscentrum,dc=be"
        self.project_dn_base = "ou=central,ou=projects,dc=vscentrum,dc=be"

        self.user_uid_range = 10000
        self.user_extra_gid_range = 10000

        # lists of minimum ids
        self.user_uid_institute_map = { "vsc" : [2500000],
                                        "brussel" : [2510000],
                                        "antwerpen" : [2520000],
                                        "leuven" : [2530000],
                                        "gent" :  [2540000],
                                      }
        self.user_extra_gid_institute_map = { "vsc" : [2600000],
                                              "brussel" : [2610000],
                                              "antwerpen" : [2620000],
                                              "leuven" : [2630000],
                                              "gent" :  [2640000],
                                            }

        self.defaults = {"new_user_status" : "new",
                         "notify_user_status" : "notify",
                         "modify_user_status" : "modify",
                         "active_user_status" : "active",
                         "inactive_user_status" : "inactive",
                         "default_project_gold" : "default_project"
                        }

        self.institutes = ["gent", "leuven", "brussel", "antwerpen"]

        # FIXME: cvan we get these from the LDAP schema?
        self.user_multi_value_attributes = ['pubkey', 'mailList', 'researchField']
        self.group_multi_value_attributes = ['memberUid', 'moderator', 'autogroup']
        self.project_multi_value_attributes = []

        self.user_shell = '/bin/bash'
        self.user_quota_home = 3145728
        self.user_quota_data = 26214400
        self.user_quota_default_scratch = 26214400

        if read_cfg:
            self.__read_config()
            self.pwdname = self.__get_password_file_name()
            if self.pwdname:
                self.__read_password_file()

        self.log.debug('Init completed')

    def __get_config_name(self):
        """Acquire the path to the VSC configuration file.

        Default is /etc/vsc.conf. However, should the environment
        in which the script using the VSC class contain a VSC_CONF
        variable, that value is preferred.

        If the configuration filename turns out to be 'NOCONF', we're
        not returning anything.

        @returns: string representing said path.
        """
        fn = "/etc/vsc.conf"
        if os.environ.has_key('VSC_CONF'):
            fn = os.environ['VSC_CONF']

        ## set this to bypass any vsc config file
        if fn == 'NOCONF':
            return

        if not os.path.isfile(fn):
            self.log.error("VSC cfg file %s not found" % fn)

        return fn

    def __get_password_file_name(self):
        """Acquire the path to the password file.

        If the configuration holds a 'passwdfile' entry and the value
        thereof corresponds to an existing file in the system, this
        value is the path. Otherwise, the path is None.

        @returns: string representing said path.
        """
        fn = None
        if self.cfg.has_key('passwdfile'):
            fn = self.cfg['passwdfile']
            if not os.path.isfile(fn):
                self.log.error("VSC pwd file %s not found" % fn)
                return None
        return fn

    def __read_config(self):
        if not self.cfgname:
            return

        f = None
        try:
            f = open(self.cfgname)
        except Exception, err:
            self.log.error("Can't open VSC cfg file %s" % self.cfgname)
            return

        ## current syntax is limited to single line with quoted text
        reg = re.compile(r"^\s*(?P<name>\w+)=(?:\")?(?P<value>.*?)(?:\")?;")
        for l in f.readlines():
            r = reg.search(l)
            if not r:
                continue
            self.cfg[r.group('name')] = r.group('value')

        self.log.debug("Found cfg %s: %s" % (self.cfgname, self.cfg))

    def __read_password_file(self):
        try:
            f = open(self.pwdname)
        except Exception, err:
            self.log.error("Can't open VSC pwd file %s" % self.pwdname)

        ## current syntax is limited to single line with quoted text

        reg = re.compile(r"^\s*(?P<name>\w(\S*?\w)?)=(?P<value>.*?)\s+")
        for l in f.readlines():
            r = reg.search(l)
            if not r:
                continue
            self.pwd[r.group('name')] = r.group('value')

        ## not in prod sys
        ## tested and working!
        ##self.log.debug("Found pwd %s: %s"%(self.pwdname,self.pwd))
        self.log.debug("Found pwd %s: %s" % (self.pwdname, self.pwd.keys()))

    def is_institute(self, insti="NOVSCINSTITUTE"):
        res = False
        if insti in self.institutes:
            res = True
        self.log.debug("Institute %s in vsc? %s" % (insti, res))
        return res

    def get_default(self, req=None):
        defa = "DOESNOTEXISTS"
        if not req:
            req = defa
        res = "%s.%s" % (defa, req)
        if self.defaults.has_key(req):
            res = self.defaults[req]

        self.log.debug("get_default %s : %s" % (req, res))
        return res

    def user_id_to_institute(self, id):
        """Determine the institute for a given user id.

        @type id: numerical user id for a VSC user.

        @returns: the name of the institute the user belongs to.
        """
        idrange = int(id / self.user_uid_range) * self.user_uid_range
        for inst, idmins in self.user_uid_institute_map.items():
            for idmin in idmins:
                if idmin == idrange:
                    return inst

        idrange = int(id / self.user_extra_gid_range) * self.user_extra_gid_range
        for inst, idmins in self.user_extra_gid_institute_map.items():
            for idmin in idmins:
                if idmin == idrange:
                    return inst

    def institute_to_regex(self, institutes=None, mode='user'):
        """Build a regular expression to match entities from a given list of institutes.

        @type institutes: a list of institute names (strings)

        @returns: a compiler regular expression.
        """
        ## FIXME: we are going to enforce this to be a list by usage!
        if type(institutes) == list:
            insts = institutes
        else:
            ## assume string
            insts = [institutes]

        if mode == 'user':
            ids = []
            for inst in insts:
                ## lookup integers
                for idmin in self.user_uid_institute_map[inst]:
                    tmp = int(idmin / self.user_uid_range)
                    res = tmp % 10

                    ids.append(str(res))

            txt = "^vsc(%s)\d{4}$" % ('|'.join(ids))
        elif mode in ['vo', 'group', 'anygroup']:
            ids = []
            for inst in institutes:
                ids.append(inst[0])

            suffix = '\w+$'
            vosuffix = 'vo\d{5}$'
            if mode == 'group':
                ## this means not VO
                suffix = "(?!%s)" % vosuffix
            elif mode == 'vo':
                suffix = vosuffix

            txt = "^(%s)%s" % ('|'.join(ids), suffix)
        else:
            self.log.error('unsupported mode %s' % mode)

        self.log.debug("Regular expression for mode %s insti %s: %s" % (mode, institutes, txt))
        reg = re.compile(r"%s" % txt)

        return reg

    def machine(self):
        """
        Get values from environment
        """
        site = 'UNKNOWN'
        cluster = 'UNKNOWN'

        if os.environ.has_key('VSC_INSTITUTE_CLUSTER'):
            site = os.environ['VSC_INSTITUTE_LOCAL']
            cluster = os.environ['VSC_INSTITUTE_CLUSTER']

        return "%s_%s" % (site, cluster)

    def vo_pathnames(vo_name, institute):
        """Get the paths for the VO owned directories.

        @type vo_name: string representing the name of the VO
        @type institute: string representing the institute

        @returns: dictionary with the (name, path) key-value pairs for the
                  directories
        """
        vo_group = vo_name[0:6]

        if institute == "gent":
            return {
                'data': "/user/data/%s/%s/%s" % (institute, vo_group, vo_name),
                'scratch': "/user/scratch/%s/%s/%s" % (institute, vo_group, vo_name)
            }
        else:
            return {
                'data': "/data/%s/%s/%s" % (institute, vo_group, vo_name),
                'scratch': "/scratch/%s/%s/%s" % (institute, vo_group, vo_name)
            }

    def user_pathnames(self, user_login, institute):
        """Get the paths to the various user owned directories.

        @type user_login: string representing the login name of the user
        @type institute: string representing the institute the user belongs to

        @returns: dictionary with the (name, path) key-value pairs for the
                  directories
        """
        if institute == "gent":
            user_group_id = user_login[0:6]
            return {
                'home': "/user/home/gent/%s/%s" % (user_group_id, user_login),
                'data': "/user/data/gent/%s/%s" % (user_group_id, user_login),
                'scratch': "/user/scratch/gent/%s/%s" % (user_group_id, user_login)
            }
        else:
            user_group_id = user_login[3:6]
            return {
                'home': "/user/%s/%s/%s" % (institute, user_group_id, user_login),
                'data': "/data/%s/%s/%s" % (institute, user_group_id, user_login),
                'scratch': "/scratch/%s/%s/%s" % (institute, user_group_id, user_login)
            }

