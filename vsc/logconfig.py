'''
Created on Oct 13, 2010

@author: wdpypere
'''
"""
debug:        True for debugging. None for info. If levels are set or switched
              in the application with setdebugloglevel(), this option is ignored.    
logtoscreen:  True for logging to stdout. If both stdout and file logging are disabled, stdout will be enabled.
logtofile:    True for logging to file.
filelocation: location and filename of logfile.
initDone:     should always be None.
logName:      should always be None.
"""
debug = True
logtoscreen = None              
logtofile = True               
filelocation = "/tmp/default.log"    
initDone = None                 
logName = None                 
