#!/usr/bin/env python
#
#   scott hendrickson (shendrickson@gnipcentral.com)
#      2011-08-23 09:53:47.873347
#
#####################
__author__ = 'scott hendrickson'

import sys
from SaveThread import SaveThread
import json
import redis
import time
import datetime

# Quick and dirty for dealing with timezones--set this to yours
tzOffset = datetime.timedelta(seconds=3600*7)

#####################

class Latency(SaveThread):

    def saveAs(self, buffer):
        self.logger.debug("Latency started")
	countMap = {}
	count = 0
	for act in buffer.split("\n"):
		self.logger.debug(str(act))
		if act.strip() is None or act.strip() == '':
			continue
		count +=1
		actJson = json.loads(act.strip())
		# should work for both Gnip Wordpress and Gnip Twitter normalized streams 
		if "postedTime" in actJson:
			pt = actJson["postedTime"]
			now = datetime.datetime.now() + tzOffset
			lat = now - datetime.datetime.strptime(pt, "%Y-%m-%dT%H:%M:%S.000Z")
			self.logger.debug("%s"%(lat))
			latSec = (lat.microseconds + (lat.seconds + lat.days * 86400.) * 10.**6) / 10.**6
			sys.stdout.write("%s, %f\n"%(now,latSec))
		elif "created_at"in actJson:
			pt = actJson["created_at"]
			now = datetime.datetime.now() + tzOffset
			# example date: Thu Dec 15 20:56:00 +0000 2011
			lat = now - datetime.datetime.strptime(pt, "%a %b %d %H:%M:%S +0000 %Y")
			self.logger.debug("%s"%(lat))
		        latSec = (lat.microseconds + (lat.seconds + lat.days * 86400.) * 10.**6) / 10.**6
			sys.stdout.write("%s, %f\n"%(now,latSec))
		else:
			self.logger.debug("postedTime missing")
        sys.exit(0)
