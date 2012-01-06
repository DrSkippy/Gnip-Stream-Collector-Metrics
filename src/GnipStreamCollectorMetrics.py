#!/usr/bin/env python
#
#   scott hendrickson (shendrickson@gnipcentral.com)
#      2012-01-03 
#
#########################

__author__ = 'scott hendrickson'

import pycurl

from SaveThread import SaveThread
from CountRules import CountRules
from Redis import Redis
from Latency import Latency

import ConfigParser
import io
import sys
import time
import logging
import logging.handlers
from cStringIO import StringIO

#########################

class SClient(object):

    def __init__(self, _streamURL,
                       _streamName,
                       _userName,
                       _password,
                       _filePath='./',
                       _rollDuration=60,
		       _procThread = None,
                       _maxFileSize=1024*1000,
                       _encoding='gzip' ):
        logging.info('SClient started')
        #
	if _procThread is None:
		self.procThreadOjb = SaveThread
	else:
		self.procThreadObj = _procThread
        #
	# see for example http://www.skymind.com/~ocrow/python_string/
        self.buffer = StringIO()
        self.bufferSize = 0
        self.saveThread = None
        #
	self.userName = _userName
        self.password = _password
        self.rollDuration = _rollDuration
        self.maxFileSize = _maxFileSize
        self.streamName = _streamName
        self.streamURL = _streamURL
        self.encoding = _encoding
        self.filePath = _filePath
        #
	self.time_start = time.time()
	self.time_rate_start = time.time()
	self.time_rate_end = None
	#
        self.conn = pycurl.Curl()
        self.conn.setopt(pycurl.USERPWD, "%s:%s" % (_userName, _password))
        self.conn.setopt(pycurl.ENCODING, _encoding)
        self.conn.setopt(pycurl.URL, _streamURL)
        self.conn.setopt(pycurl.WRITEFUNCTION, self.on_receive)
        self.conn.setopt(pycurl.FOLLOWLOCATION, 1)
        self.conn.setopt(pycurl.MAXREDIRS, 5)
        self.conn.setopt(pycurl.COOKIEFILE, "cookie.txt")
        try:
            self.conn.perform()
	    logging.info('pycurl connections established')
        except Exception, e:
            logging.error("Pycurl connection failure: %s. Exiting." % (e.message))
            sys.exit()

    # filePath getters and setters
    def getFilePath(self):
        return self.filePath

    def setFilePath(self, _filePath):
        self.filePath = _filePath

    # encoding getters and setters
    def getEncoding(self):
        return self.encoding

    def setEncoding(self, _encoding):
        self.encoding = _encoding

    # streamURL getters and setters
    def getStreamURL(self):
        return self.streamURL

    def setStreamURL(self, _streamURL):
        self.streamURL = _streamURL

    # conn getters and setters
    def getConn(self):
        return self.conn

    # buffer getters and setters
    def getBuffer(self):
        return self.buffer.getvalue()

    # userName getters and setters
    def getUsername(self):
        return self.userName

    def setUsername(self, _userName):
        self.userName = _userName

    # password getters and setters
    def setPassword(self, _password):
        self.password = _password

    # rollDuration getters and setters
    def getRollduration(self):
        return self.rollDuration

    def setRollduration(self, _rollDuration):
        self.rollDuration = _rollDuration

    # maxFileSize getters and setters
    def getMaxfilesize(self):
        return self.maxFileSize

    def setMaxfilesize(self, _maxFileSize):
        self.maxFileSize = _maxFileSize

    # streamName getters and setters
    def getStreamName(self):
        return self.streamName

    def setStreamName(self, _streamName):
        self.streamName = _streamName

    def on_receive(self, data):
        #print data
        self.buffer.write(data)
        self.bufferSize += len(data)
        logging.debug("(size=%dbytes)"%self.bufferSize)
        if data.endswith("\n"):   # so we have complete records
            if self.triggered():
                #Start the save thread...
                if self.bufferSize != 0:
                    try:
			self.time_rate_end = time.time()
			timeSpan = self.time_rate_end - self.time_rate_start
			self.procThreadObj(self.getBuffer(), self.getStreamName(), self.getFilePath(), logging, _timeSpan=timeSpan).start()
			self.time_rate_start = time.time()
		    except Exception, e:  # Beware, catches all exceptions! Can hide silly errors.
                        logging.error("Thread problem: %s"%(e))
                        # sys.exit(1)
                self.buffer.close()
                self.buffer = StringIO()
                self.bufferSize = 0

    def triggered(self):
        logging.info("started (size=%s)"%self.bufferSize)
        # First trigger based on size then based on time..
        if self.bufferSize > self.getMaxfilesize():
            logging.debug("buffer trigger")
            return True
        time_end = time.time()
        if (time_end - self.time_start) > self.getRollduration():  #for the time frame
            logging.debug("time trigger")
            self.time_start = time.time()
            return True
        return False

    def __repr__(self):
        resList = []
        resList.append("<<<<String representation of Sclient>>>>\n")
        resList.append("{0:.<20}: {1}\n".format(
            'userName',self.getUsername()))
        resList.append("{0:.<20}: {1}\n".format(
            'rollDuration',self.getRollduration()))
        resList.append("{0:.<20}: {1}\n".format(
            'maxFileSize',self.getMaxfilesize()))
        resList.append("{0:.<20}: {1}\n".format(
            'encoding',self.getEncoding()))
        return ''.join(resList)


if __name__ == '__main__':

    config = ConfigParser.ConfigParser()
    config.read('gnip.cfg')

    # logging  level=logging.DEBUG for lots of detail
    logfilepath = config.get('sys','logfilepath')
    logging.basicConfig(filename=logfilepath + "/gnip-log",
                    format='%(asctime)s %(levelname)s %(funcName)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
    		    level=logging.ERROR)
                    #level = logging.DEBUG)
    
    # set up authentication
    username = config.get('auth','username')
    password = config.get('auth','password')
    # stream
    streamurl = config.get('stream', 'streamurl')
    streamname = config.get('stream', 'streamname')
    filepath = config.get('stream', 'filepath')
    
    # Determine processing method
    rollduration = int(config.get('proc', 'rollduration'))
    processtype = config.get('proc', 'processtype')
    if processtype == "latency":
    	proc = Latency
    elif processtype == "files":
    	proc = SaveThread
    elif processtype == "rules":
        proc = CountRules
    elif processtype == "redis":
    	proc = Redis
    else:
    	print "Please select a valid processtype.  Exiting."
	sys.exit(-1)

    sclientObj = SClient(streamurl, streamname, username, password, _filePath=filepath, _rollDuration=rollduration, _procThread=proc)
    # print sclientObj
