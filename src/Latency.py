#!/usr/bin/env python
__author__ = 'scott hendrickson'

from threading import RLock
import threading
import sys
import json
import time
import datetime

wrt_lock = RLock()

# Quick and dirty for dealing with timezones--set this to yours
tzOffset = datetime.timedelta(seconds=3600*7)
tzOffset = datetime.timedelta(seconds=0)

class Latency(threading.Thread):
    def __init__(self, _buffer, _feedname, _savepath, _rootLogger, _endTs, _spanTs):
        threading.Thread.__init__(self)
        with wrt_lock:
            self.logger = _rootLogger
            self.string_buffer = _buffer

    def run(self):
        with wrt_lock:
            self.logger.debug("started")
            for act in self.string_buffer.split("\n"):
                if act.strip() is None or act.strip() == '':
                    continue
                actJson = json.loads(act.strip())
                now = datetime.datetime.now() + tzOffset
                if "postedTime" in actJson:
                    # for twitter, postedTime is at root
                    pt = actJson["postedTime"]
                    try:
                        lat = now - datetime.datetime.strptime(pt, "%Y-%m-%dT%H:%M:%S.000Z")
                    except ValueError:
                        lat = now - datetime.datetime.strptime(pt, "%Y-%m-%dT%H:%M:%S+00:00")
                elif "created_at"in actJson:
                    # for wp, created_at is at root
                    pt = actJson["created_at"]
                    # example date: Thu Dec 15 20:56:00 +0000 2011
                    lat = now - datetime.datetime.strptime(pt, "%a %b %d %H:%M:%S +0000 %Y")
                elif "object" in actJson:
                    # for stocktwits
                    if "postedTime" in actJson["object"]:
                        pt = actJson["object"]["postedTime"]
                        lat = now - datetime.datetime.strptime(pt, "%Y-%m-%dT%H:%M:%SZ")
                    else:
                        self.logger.debug("%s"%"object found but postedTime missing")
                        continue
                else:
                    self.logger.debug("postedTime, created_at, and object missing")
                    continue
                self.logger.debug("%s"%(lat))
                latSec = (lat.microseconds + (lat.seconds + lat.days * 86400.) * 10.**6) / 10.**6
                sys.stdout.write("%s, %f\n"%(now,latSec))
