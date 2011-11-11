'''
Created on Oct 14, 2011

@author: jens
This module implements a fancy logger on top of python logging

it adds custom specifiers, rotating file handler, and a default formatter

f.ex the threadname specifier which will insert the name of the thread 

usage:
import fancylogger
#will log to screen by default

fancylogger.logToFile('dir/filename') 
fancylogger.setLogLevelDebug() #set global loglevel to debug
logger = fancylogger.getLogger(name) #get a logger with a specific name
logger.setLevel(level) #set local debugging level

#you can now even use the handler to set a different formatter by using
handler = fancylogger.logToFile('dir/filename')
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-10s %(funcname)-15s %(threadname)-10s %(message)s))
'''
from logging import Logger
import logging.handlers
import threading
import inspect


#constants  
LOGGER_NAME = "fancylogger"
DEFAULT_LOGGING_FORMAT= '%(asctime)-15s %(levelname)-10s %(name)-15s %(threadname)-10s  %(message)s'
#DEFAULT_LOGGING_FORMAT= '%(asctime)-15s %(levelname)-10s %(module)-15s %(threadname)-10s %(message)s'
MAX_BYTES = 100*1024*1024 #max bytes in a file with rotating file handler
BACKUPCOUNT = 10 #number of rotating log files to save

# Adding extra specifiers is as simple as adding attributes to the log record
# Custom log record
class FancyLogRecord(logging.LogRecord):
    def __init__(self, *args, **kwargs):
        logging.LogRecord.__init__(self,*args, **kwargs)
        #modify custom specifiers here
        self.threadname = thread_name() #actually threadName already exists?
        self.name= self.name.replace(LOGGER_NAME + ".","",1) #remove LOGGER_NAME prefix from view


# Custom logger that uses our log record
class NamedLogger(logging.getLoggerClass()):
    _thread_aware = True #this attribute can be checked to know if the logger is thread aware
    
    def __init__(self,name):
        """
        constructor
        This function is typically called before any
        loggers are instantiated by applications which need to use custom
        logger behavior.
                """
        Logger.__init__(self,name)
        self.log_To_Screen = False
        self.log_To_File = False
        
    def makeRecord(self, name, level, pathname, lineno, msg, args, exc_info, func=None, extra=None):
        """
        overwrite make record to use a fancy record (with more options)
        """
        return FancyLogRecord(name, level, pathname, lineno, msg, args, exc_info)
    
    def raiseException(self,message,exception=Exception):
        """
        logs an exception (as warning, since it can be caught higher up and handled)
        and raises it afterwards
        """
        self.warning(message)
        raise exception(message)
        
def thread_name():
    """
    returns the current threads name
    """
    return threading.currentThread().getName()


def getLogger(name=None):
    """
    returns a fancylogger
    """
    fullname =getRootLoggerName()
    if name:
        fullname =  fullname + "." + name
        
    #print "creating logger for %s"%fullname
    return logging.getLogger(fullname)
      
def getRootLoggerName():
    """
    returns the name for the root logger for the particular instance
    """
    ret = _getRootModuleName()  
    if ret:
        #return LOGGER_NAME + "." + ret
        return ret
    else:  
        return LOGGER_NAME           

def _getRootModuleName():
    """
    returns the name of the root module
    this is the module that is actually running everything and so doing the logging
    """
    try:
        return inspect.stack()[-1][1].split('/')[-1].split('.')[0]
    except:
        return None

def logToScreen(boolean=True,handler=None,name=None):
    """
    enable (or disable) logging to screen
    returns the screenhandler (this can be used to later disable logging to screen)
    
    if you want to disable logging to screen, pass the earlier obtained screenhandler
    
    you can also pass the name of the logger for which to log to the screen
    otherwise you'll get all logs on the screen 
    """
    logger= getLogger(name)
    if boolean and not logger.log_To_Screen:
        if not handler:
            formatter = logging.Formatter(DEFAULT_LOGGING_FORMAT)
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.log_To_Screen = True
    elif not boolean:
        if handler:
            logger.removeHandler(handler)
        else:
            #removing the standard stdout logger doesn't work
            #it will be readded if only one handler is present
            lhStdout = logger.handlers[0]  # stdout is the first handler
            lhStdout.setLevel(101)#50 is critical, so 101 should be nothing
        logger.log_To_Screen = False
    return handler
        
def logToFile(filename,boolean=True,filehandler=None,name=None):
    """
    enable (or disable) logging to file
    given filename
    will log to a file with the given name using a rotatingfilehandler
    this will let the file grow to MAX_BYTES and then rotate it
    saving the last BACKUPCOUNT files. 
    
    returns the filehandler (this can be used to later disable logging to screen)
    
    if you want to disable logging to screen, pass the earlier obtained filehandler 
    """
    logger= getLogger(name)
    if boolean and not logger.log_To_File:
        if not filehandler:
            formatter = logging.Formatter(DEFAULT_LOGGING_FORMAT)
            filehandler = logging.handlers.RotatingFileHandler(filename, 'a', maxBytes=MAX_BYTES, backupCount=BACKUPCOUNT )
            filehandler.setFormatter(formatter)
        logger.addHandler(filehandler)
        logger.log_To_File = True
    elif not boolean: #stop logging to file (needs the handler, so fail if it's not specified)
        logger.removehandler(filehandler)
        logger.log_To_File = False
    return filehandler
            


def setLogLevel(level):
    """
    set a global log level (for this root logger)
    """
    getLogger().setLevel(level)
    
def setLogLevelDebug():
    """
    shorthand for setting debug level
    """
    setLogLevel(logging.DEBUG)

def setLogLevelInfo():
    """
    shorthand for setting loglevel to Info
    """
    setLogLevel(logging.INFO)
    
def setLogLevelWarning():
    """
    shorthand for setting loglevel to Info
    """
    setLogLevel(logging.WARNING)

# Register our logger
logging.setLoggerClass(NamedLogger)
#log to screen by default
logToScreen(boolean=True)
#print "getting logger"
#getLogger().critical( "created logger for %s"%getRootLoggerName())
#create a root logger 
#getLogger()