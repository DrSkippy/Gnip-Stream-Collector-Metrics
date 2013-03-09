#!/usr/bin/env python
__author__ = 'scott hendrickson'

import sys
from SaveThread import SaveThread
from threading import RLock
import json
import redis
import datetime

wrt_lock = RLock()

class CountRules(SaveThread):
    def run(self):
        with wrt_lock:
            self.logger.debug("CountRules started")
            countMap = {}
            count = 0
            for act in self.string_buffer.split("\n"):
                self.logger.debug(str(act))
                if act.strip() is None or act.strip() == '':
                    continue
                count +=1
                actJson = json.loads(act.strip())
                if "gnip" in actJson:
                    if "matching_rules" in actJson["gnip"]:
                        for mr in actJson["gnip"]["matching_rules"]:
                            rule = mr["value"]
                            self.logger.debug(str(rule))
                            if rule in countMap:
                                countMap[rule] += 1
                            else:
                                countMap[rule] = 1
                    else:
                        self.logger.debug("matching_rules missing")
                else:
                    self.logger.debug("gnip missing")
            # If calls to saveThreads are overlapping, replace this with thread safe prints
            sys.stdout.write("(%s) sample %d tweets (%d seconds)\n"%
                    (datetime.datetime.now(), count, self.timeSpan))
            firstCol = max([len(x) for x in countMap.keys()]) + 3
            countMapKeys = sorted(countMap.keys(), key=countMap.__getitem__)
            countMapKeys.reverse()
            for r in countMapKeys:
                sys.stdout.write("%s: %s (%4d tweets matched)   %3.4f tweets/second\n"%
                (r, '.'*(firstCol-len(r)), countMap[r], float(countMap[r])/self.getTimeSpan()))
