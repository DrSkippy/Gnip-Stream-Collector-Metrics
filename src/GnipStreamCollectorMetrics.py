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
MAX_ROLL_SIZE = 2**30     # force time-period to roll forward 
NEW_LINE = '\r\n'

class GnipStreamClient(object):
    def __init__(self, _streamURL, _streamName, _userName, _password,
            _filePath, _rollDuration, _procThread):
        logr.info('GnipStreamClient started')
        self.rollDuration = _rollDuration
        self.streamName = _streamName
        self.streamURL = _streamURL
        self.filePath = _filePath
        self.procThread = _procThread
        #
        self.headers = { 'Accept': 'application/json',
            'Connection': 'Keep-Alive',
            'Accept-Encoding' : 'gzip',
            'Authorization' : 'Basic %s'%base64.encodestring(
                '%s:%s'%(_userName, _password))  }
    
    def run(self):
        self.time_roll_start = time.time()
        #delay = 0.01
        #fail_time = 0.0
        while True:
            reset_time = time.time()
            try:
                self.getStream()
                logr.error("Forced disconnect: %s"%e)
            except ssl.SSLError, e:
                logr.error("Connection failed: %s"%e)
            #finally:
            #    time.sleep(delay)
            #    fail_time = time.time()
            #    if fail_time - reset_time > 1.1*delay:
            #        reset_time = time.time()
            #        delay = 0.01
            #    else:
            #        if delay < 120:
            #            delay *= 2
            #        else:
            #            delay = 120

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
            test_roll_size = roll_size + len(self.string_buffer)
            if self.triggerProcess(test_time, test_roll_size):
                try:
                    [records, self.string_buffer] = self.string_buffer.rsplit(NEW_LINE,1)
                    timeSpan = test_time - self.time_roll_start
                    logr.debug("recsize=%d, %s, %s, ts=%d, dur=%d"%
                            (len(records), self.streamName, self.filePath, 
                                test_time, timeSpan))
                    self.procThread(records, self.streamName, self.filePath,
                            logr, self.time_roll_start, timeSpan).start()
                    if self.rollForward(test_time, test_roll_size):
                        self.time_roll_start = test_time
                        roll_size = 0
                    else:
                        roll_size += len(records)
                except Exception, e:
                    raise e

    def rollForward(self, ttime, tsize):
        # these trigger both processing and roll forward
        if ttime - self.time_roll_start >= self.rollDuration:
            logr.debug("Roll: duration (%d>=%d)"%
                    (ttime-self.time_roll_start, self.rollDuration))
            return True
        if tsize >= MAX_ROLL_SIZE:
            logr.debug("Roll: size (%d>=%d)"%
                    (tsize, MAX_ROLL_SIZE))
            return True
        return False

    def triggerProcess(self, ttime, tsize):
        # trigger process?
        # no trigger if no NEW_LINE
        if NEW_LINE not in self.string_buffer:
            return False
        if len(self.string_buffer) > MAX_BUF_SIZE:
            logr.debug("Trigger: buffer size (%d>%d)"%
                    (len(self.string_buffer), MAX_BUF_SIZE))
            return True
        return self.rollForward(ttime, tsize)

if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read('gnip.cfg')
    streamname = config.get('stream', 'streamname')
    # logger 
    logfilepath = config.get('sys','logfilepath')
    logr = logging.getLogger('GnipStreamLogger')
    rotating_handler = logging.handlers.RotatingFileHandler(
            filename=logfilepath + "/%s-log"%streamname,
            mode='a', maxBytes=2**24, backupCount=5)
    rotating_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(funcName)s %(message)s"))
    logr.setLevel(logging.DEBUG)
    #logr.setLevel(logging.ERROR)
    logr.addHandler(rotating_handler)
    # set up authentication
    username = config.get('auth','username')
    password = config.get('auth','password')
    # stream
    streamurl = config.get('stream', 'streamurl')
    filepath = config.get('stream', 'filepath')
    logr.info("Collection starting for stream %s"%(streamurl))
    logr.info("Storing data in path %s"%(filepath))
    # Determine processing method
    rollduration = int(config.get('proc', 'rollduration'))
    processtype = config.get('proc', 'processtype')
    logr.info("processing strategy %s"%processtype)
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
        logr.error("No valid processing strategy selected (%s), aborting"%processtype)
        sys.exit(-1)
    client = GnipStreamClient(streamurl, streamname, username, password, 
            filepath, rollduration, proc)
    client.run()
