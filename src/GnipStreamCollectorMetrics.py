#!/usr/bin/env python
__author__ = 'scott hendrickson'

import ConfigParser
import time
import logging
import logging.handlers
import urllib2
import base64
import zlib
import sys
import ssl

# stream processing strategies
from SaveThread import SaveThread
from CountRules import CountRules
from Redis import Redis
from Latency import Latency

# Tune CHUNKSIZE as needed.  The CHUNKSIZE is the size of compressed data read
# For high volume streams, use large chuck sizes, for low volume streams, decrease
# CHUNKSIZE.  Minimum practical is about 1K.
CHUNKSIZE = 2**16
GNIPKEEPALIVE = 30  # seconds
MAX_BUF_SIZE = 2**27 # records
NEWLINE = '\r\n'

class GnipStreamClient(object):
    def __init__(self, 
                    _streamURL,
                    _streamName,
                    _userName,
                    _password,
                    _filePath='./',
                    _rollDuration=60,
                    _procThread = None,
                    _encoding='gzip',
                    _maxFileSize = MAX_BUF_SIZE ):
        logging.info('GnipStreamClient started')
        self.rollDuration = _rollDuration
        self.streamName = _streamName
        self.streamURL = _streamURL
        self.encoding = _encoding
        self.filePath = _filePath
        self.procThread = _procThread
        self.maxFileSize = _maxFileSize
        #
        self.time_start = time.time()
        self.time_roll_start = self.time_start
        self.time_roll_end = None
        self.headers = { 'Accept': 'application/json',
            'Connection': 'Keep-Alive',
            'Accept-Encoding' : 'gzip',
            'Authorization' : 'Basic %s'%base64.encodestring(
                '%s:%s'%(_userName, _password))  }
    
    def run(self):
        while True:
            if self.procThread is None:
                # Default processing strategy is SaveThread
                self.procThreadOjb = SaveThread
            else:
                self.procThreadObj = self.procThread
            try:
                self.getStream()
                logging.error("Forced disconnect: %s\n"%(str(e)))
            except ssl.SSLError, e:
                logging.error("Connection failed: %s\n"%(str(e)))

    def getStream(self):
        req = urllib2.Request(self.streamURL, headers=self.headers)
        response = urllib2.urlopen(req, timeout=(1+GNIPKEEPALIVE))
        decompressor = zlib.decompressobj(16+zlib.MAX_WBITS)
        remainder = ''
        self.string_buffer = ''
        while True:
            tmp_buf = ''
            while NEWLINE not in tmp_buf:
                tmp = decompressor.decompress(response.read(CHUNKSIZE))
                if tmp == '':
                    return
                tmp_buf += tmp
            [records, remainder] = ''.join([remainder, tmp_buf]).rsplit(NEWLINE,1)
            self.string_buffer += records
            test_time = time.time()
            try:
                if self.trigger(test_time):
                    timeSpan = test_time - self.time_roll_start
                    self.procThreadObj(
                        self.string_buffer, 
                        self.streamName, 
                        self.filePath,
                        logging, 
                        test_time, 
                        timeSpan).start()
                    self.time_roll_start = test_time
                    self.string_buffer = ''
            except Exception, e:
                raise e

    def trigger(self, ts):
        if len(self.string_buffer) > self.maxFileSize:
            logging.debug("Triggered: Buffer size (%d)"%self.maxFileSize)
            return True
        if ts - self.time_roll_start >= self.rollDuration:
            logging.debug("Triggered: Roll duration (%d)"%self.rollDuration)
            return True
        return False

if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read('gnip.cfg')
    # logging  level=logging.DEBUG for lots of detail
    logfilepath = config.get('sys','logfilepath')
    logging.basicConfig(filename=logfilepath + "/gnip-stream-log",
                    format='%(asctime)s %(levelname)s %(funcName)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    #level=logging.ERROR)
                    level = logging.DEBUG)
    # set up authentication
    username = config.get('auth','username')
    password = config.get('auth','password')
    # stream
    streamurl = config.get('stream', 'streamurl')
    streamname = config.get('stream', 'streamname')
    filepath = config.get('stream', 'filepath')
    logging.info("Collection starting for stream %s"%(streamurl))
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
        logging.error("No valid processing strategy selected (%s). Aborting."%processtype)
        sys.exit(-1)
    logging.info("Storing data in path %s"%(filepath))

    client = GnipStreamClient(streamurl, 
            streamname, 
            username, 
            password, 
            filepath, 
            rollduration, 
            proc)
    client.run()
