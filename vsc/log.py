#!/usr/bin/env python
'''
Created on Oct 13, 2010

@author: wdpypere
'''


import logging
import logconfig


def getLog(name=None,type=None,rethand=False):
    """
    Will initialise a logger or add a sublogger.
    type and rethand are for backwards compatibility and should no longer be used.
    """
       
    if logconfig.initDone == None:
        if name == None:
            name = "UNKNOWN"
        if not type:
            type=name    
                         
        if rethand:
            logger, hand = initLog(name, type, rethand)
        else:
            logger = initLog(name, type, rethand)
    else:
        if name != None:
            name = "%s.%s" % (logconfig.logName, name)
        else:
            name = logconfig.logName + ".UNKNOWN"
        if not type:
                type=name
        
        
        logger = logging.getLogger(name)
        #creation of unused handler just to satisfy unadapted scripts :s
        hand = logging.StreamHandler()
        
        debug = logconfig.debug
        if debug:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)  
        
    if rethand:
        return logger,hand
    else:
        return logger        
    
    
def setdebugloglevel(debug):
    """Used to enable/disable debugging."""
    logconfig.debug = debug
    
def setlogtofile(file):
    logconfig.file = file

def setlogfilelocation(location):
    logconfig.filelocation = location    

def initLog(name=None,type=None,rethand=False):
    """
    Will initialise a logger.
    type and rethand are for backwards compatibility and should no longer be used.
    """
    if logconfig.initDone == None:
        if name == None:
            name = "UNKNOWN"
        if not type:
            type=name
            
        logger = logging.getLogger(name)
        
        debug = logconfig.debug
        if debug:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO) 
        
        if not logconfig.logtofile and not logconfig.logtoscreen: logconfig.logtoscreen = True
        
        if logconfig.logtofile:    
            outputfilehandler = logging.FileHandler(logconfig.filelocation)
            outputfilehandler.setLevel(logger.getEffectiveLevel())
            formatterTime = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            outputfilehandler.setFormatter(formatterTime)
            logger.addHandler(outputfilehandler)
            hand = outputfilehandler
        if logconfig.logtoscreen:
            outputconsolehandler = logging.StreamHandler()
            outputconsolehandler.setLevel(logger.getEffectiveLevel())        
            formatterNotime = logging.Formatter("%(name)s - %(levelname)s - %(message)s")    
            outputconsolehandler.setFormatter(formatterNotime)
            logger.addHandler(outputconsolehandler)
            hand = outputconsolehandler
            
        
        logconfig.initDone = True
        logconfig.logName = name
        
        from socket import gethostname
        logger.debug("Log initialised with name %s on host %s (debug set to %s)."%(name,gethostname(),debug))
    
    else:
        if rethand:
            logger, hand = getLog(name, type, rethand)
        else:
            logger = getLog(name, type, rethand)
            
            
    if rethand:
        return logger,hand
    else:
        return logger

