#!/usr/bin/env python
#
#   scott hendrickson (shendrickson@gnipcentral.com)
#      2011-08-23 09:53:47.873347
#
#####################
__author__ = 'scott hendrickson'

from pprint import pprint
from cStringIO import StringIO
import threading
import datetime
import time
import gzip
import os
import sys

#####################

class SaveThread(threading.Thread):
    
    """ The base class for procession a stream of data from the pycurl process.  The saveAs 
	implementation creates gzip'd data files containing, e.g., JSON formatted tweets from
	GNIP.  Files are organizaed by year, month, date directories. Overwrite 
	saveAs to implement other functions like parsing and counting."""

    def __init__(self, _buffer, _feedname, _savepath, _rootLogger, _timeSpan=None):
        _rootLogger.info('SaveThread started')
        self.savepath = _savepath
        self.buffer = _buffer      # string
        self.feedName = _feedname
        self.logger =  _rootLogger
	self.timeSpan = _timeSpan
        threading.Thread.__init__(self)

    # timespan getter
    def getTimeSpan(self):
	return self.timeSpan

    # savepath getters and setters
    def getSavepath(self):
        return self.savepath

    # buffer getters and setters
    def getBuffer(self):
        return self.buffer

    # feedName getters and setters
    def getFeedname(self):
        return self.feedName.replace(" ","_").replace(":","")

    def run(self):
        self.logger.debug("started")
        try:
            self.saveAs(self.getBuffer())
        except Exception, e: # Beware, this catches aeverythign so hides some dumb mistakes!
	    self.logger.error("saveAs failed, exiting thread (%s). Exiting." % (e))
            #sys.exit(1)

    def saveAs(self, buffer):
        self.logger.debug("started")
        date_time = datetime.datetime.today()
        year, month, day, hour = date_time.year, date_time.month, date_time.day, date_time.hour
        minute, microsecond = date_time.minute, date_time.microsecond
        if hour < 10:
            hour = "0" + str(hour)
        if day < 10:
            day = "0" + str(day)
        if month < 10:
            month = "0" + str(month)

        base_path = self.getSavepath()
        suffix_path = str(year) + "/" + str(month) + "/" + str(day) + "/" + str(hour)
        file_path = base_path + "/" + suffix_path

        try:
            os.makedirs(file_path)
        except OSError, e:
            self.logger.info("directory already exists (%s)"%file_path)

        name = self.getFeedname() + "_" + str(year) + str(month) + str(day) + str(hour) + str(minute) + str(
            microsecond)
        file_name = file_path + "/" + name + ".gz"
        self.logger.debug("file_name=%s"%file_name)
        fp = gzip.open(file_name, "w")
        fp.write(buffer)
        fp.close()
        self.logger.info("saved file %s" % ( file_name))
        # Exit thread now.
        sys.exit(0)

    def __repr__(self):
        resList = []
        resList.append("<<<<String representation of Savethread>>>>\n")
        resList.append("{0:.<20}: {1}\n".format(
            'savepath',self.getSavepath()))
        resList.append("{0:.<20}: {1}\n".format(
            'buffer',self.getBuffer()))
        return ''.join(resList)
