#!/usr/bin/env python
__author__ = 'scott hendrickson'

from threading import RLock
import threading
import time
import gzip
import os

write_lock = RLock()

class SaveThread(threading.Thread):
    def __init__(self, _buffer, _feedname, _savepath, _rootLogger, _startTs, _spanTs, **kwargs):
        self.logger =  _rootLogger
        self.savepath = _savepath
        self.string_buffer = _buffer
        self.feedName = _feedname
        self.timeEnd = time.gmtime(_startTs + _spanTs)
        self.timeSpan = _spanTs
        self.timeStart = time.gmtime(_startTs)
        threading.Thread.__init__(self)

    def run(self):
        try:
            # store by start date, name by start date
            self.logger.debug("started")
            file_path = "/".join([
                self.savepath,
                "%d"%self.timeStart.tm_year,
                "%02d"%self.timeStart.tm_mon,
                "%02d"%self.timeStart.tm_mday,
                "%02d"%self.timeStart.tm_hour ])
            try:
                os.makedirs(file_path)
                self.logger.info("directory created (%s)"%file_path)
            except OSError, e:
                self.logger.info("directory exists (%s)"%file_path)
            name = self.feedName + "_"
            name += "-".join([
                    "%d"%self.timeStart.tm_year,
                    "%02d"%self.timeStart.tm_mon,
                    "%02d"%self.timeStart.tm_mday])
            name += "_%02d%02d"%(self.timeStart.tm_hour, self.timeStart.tm_min)
            name += ".gz"
            file_name = file_path + "/" + name
            with write_lock:
                fp = gzip.open(file_name, "a")
                fp.write(self.string_buffer)
                fp.close()
                self.logger.info("saved file %s"%file_name)
        except Exception, e:
            self.logger.error("saveAs failed, exiting thread (%s). Exiting."%e)
            raise e
