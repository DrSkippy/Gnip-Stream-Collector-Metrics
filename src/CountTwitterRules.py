#!/usr/bin/env python
__author__ = 'scott hendrickson'

import sys
from SaveThread import SaveThread
from threading import RLock
import json
import datetime
import gzip

wrt_lock = RLock()

class CountTwitterRules(SaveThread):
    def write(self, file_name):
        self.logger.debug("CountTwitterRules is writing.")
        count_map = {}
        count = 0
        for act in self.string_buffer.split("\n"):
            #self.logger.debug(str(act))
            if act.strip() is None or act.strip() == '':
                continue
            count +=1
            act_json = json.loads(act.strip())
            if "gnip" in act_json:
                if "matching_rules" in act_json["gnip"]:
                    for mr in act_json["gnip"]["matching_rules"]:
                        rule = mr["value"]
                        #self.logger.debug(str(rule))
                        if rule in count_map:
                            count_map[rule] += 1
                        else:
                            count_map[rule] = 1
                else:
                    self.logger.error("matching_rules missing")
            else:
                self.logger.error("gnip missing")
        
        try:
            now = datetime.datetime.now()
            file_name = file_name.replace("gz","counts")
            fp = open(file_name, "a")
            #sys.stdout.write("(%s) sample %d tweets (%d seconds)\n"%
            #        (datetime.datetime.now(), count, self.timeSpan))
            first_col = max([len(x) for x in count_map.keys()]) + 3
            count_mapKeys = sorted(count_map.keys(), key=count_map.__getitem__)
            count_mapKeys.reverse()
            for r in count_mapKeys:
                # for dump to file
                r_no_comma = r.replace(",","COMMA")
                write_str = "{}, {}, {}, {}, {}\n".format(now, r_no_comma, count_map[r], count, self.timeSpan)
                fp.write(write_str)
                # for dump to stdout
                #sys.stdout.write("%s: %s (%4d tweets matched)   %3.4f tweets/second\n"%
                #(r, '.'*(first_col-len(r)), count_map[r], float(count_map[r])/self.timeSpan)) 
            fp.close()
            self.logger.info("saved file %s"%file_name)
        except Exception, e:
            self.logger.error("write failed: %s"%e)
            raise e

