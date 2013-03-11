#!/usr/bin/env python
__author__ = 'scott hendrickson'
import threading
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

CHUNK_SIZE = 2**16        # decrease for v. low volume streams, > max record size
GNIP_KEEP_ALIVE = 30      # 30 sec gnip timeout
MAX_BUF_SIZE = 2**22      # bytes records to hold in memory
MAX_ROLL_SIZE = 2**36     # force time-period to roll forward 
NEW_LINE = '\r\n'

class GnipStreamClient(object):
    def __init__(self, _streamURL, _streamName, _userName, _password,
            _filePath, _rollDuration, _procThread):
        logging.info('GnipStreamClient started')
        self.rollDuration = _rollDuration
        self.streamName = _streamName
        self.streamURL = _streamURL
        self.filePath = _filePath
        self.procThreadObj = _procThread
        #
        self.headers = { 'Accept': 'application/json',
            'Connection': 'Keep-Alive',
            'Accept-Encoding' : 'gzip',
            'Authorization' : 'Basic %s'%base64.encodestring(
                '%s:%s'%(_userName, _password))  }
    
    def run(self):
        self.time_start = time.time()
        self.time_roll_start = self.time_start
        while True:
            try:
                self.getStream()
                logging.error("Forced disconnect: %s"%e)
            except ssl.SSLError, e:
                logging.error("Connection failed: %s"%e)

    def getStream(self):
        req = urllib2.Request(self.streamURL, headers=self.headers)
        response = urllib2.urlopen(req, timeout=(1+GNIP_KEEP_ALIVE))
        decompressor = zlib.decompressobj(16+zlib.MAX_WBITS)
        self.string_buffer = ''
        roll_size = 0
        while True:
            chunk = decompressor.decompress(response.read(CHUNK_SIZE))
            # if chunk is zero length, no longer connected to gnip
            if chunk == '':
                return
            self.string_buffer += chunk
            test_time = time.time()
            test_size = roll_size + len(self.string_buffer)
            if self.triggerProcess(test_time, test_size):
                try:
                    [records, self.string_buffer] = self.string_buffer.rsplit(NEW_LINE,1)
                    timeSpan = test_time - self.time_roll_start
                    logging.debug("recsize=%d, %s, %s, ts=%d, dur=%d"%
                            (len(records), self.streamName, self.filePath, 
                                test_time, timeSpan))
                    self.procThreadObj(records, self.streamName, self.filePath,
                            logging, self.time_roll_start, timeSpan).start()
                    if self.rollForward(test_time, test_size):
                        self.time_roll_start = test_time
                        roll_size = 0
                    else:
                        roll_size += len(records)
                except Exception, e:
                    raise e

    def rollForward(self, ttime, tsize):
        # these trigger both processing and roll forward
        if ttime - self.time_roll_start >= self.rollDuration:
            logging.debug("Roll: duration (%d>=%d)"%
                    (ttime-self.time_roll_start, self.rollDuration))
            return True
        if tsize >= MAX_ROLL_SIZE:
            logging.debug("Roll: size (%d>=%d)"%
                    (tsize, MAX_ROLL_SIZE))
            return True
        return False

    def triggerProcess(self, ttime, tsize):
        # trigger process?
        # no trigger if no NEW_LINE
        if NEW_LINE not in self.string_buffer:
            return False
        if len(self.string_buffer) > MAX_BUF_SIZE:
            logging.debug("Triggered: buffer size (%d>%d)"%
                    (len(self.string_buffer), MAX_BUF_SIZE))
            return True
        return self.rollForward(ttime, tsize)

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
    logging.info("processing strategy %s"%processtype)
    proc = None
    if processtype == "latency":
        proc = Latency
    elif processtype == "files":
        proc = SaveThread
    elif processtype == "rules":
        proc = CountRules
    elif processtype == "redis":
        proc = Redis
    else:
        logging.error("No valid processing strategy selected (%s), aborting"%processtype)
        sys.exit(-1)
    logging.info("Storing data in path %s"%(filepath))
    client = GnipStreamClient(streamurl, streamname, username, password, 
            filepath, rollduration, proc)
    client.run()
