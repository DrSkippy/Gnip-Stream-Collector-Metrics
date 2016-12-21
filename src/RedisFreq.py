#!/usr/bin/env python
__author__ = 'scott hendrickson'

import sys
import redis
import re
import datetime

limit = 150
#####################

class RedisFreq(object):
    def __init__(self):
        rs = redis.Redis("localhost")
        keys = rs.keys()
        self.valMap = {}
        self.ruleMap = {}
        for key in keys:
            try:
                if key[0] != "[":
                    self.valMap[key] = int(rs.get(key))
                else:
                    self.ruleMap[key] = int(rs.get(key))
            except ValueError:
                sys.stderr.write("valMap or ruleMap Key value error\n") 
            except redis.exceptions.ResponseError as e:
                sys.stderr.write("Redis response errer (%s)\n"%e)
            except IndexError:
                sys.stderr.write("List index error (%s)\n"%key)
            except TypeError:
                sys.stderr.write("Redis returned non-int value for key=%s\n"%key)
        self.ordKeys = sorted(self.valMap.keys(), key=self.valMap.__getitem__)
        self.ruleKeys = sorted(self.ruleMap.keys(), key=self.ruleMap.__getitem__)
        c1 = rs.get("TotalRuleMatchCount")
        try:
            c2 = int(rs.get("TotalRuleMatchCountTmp"))
        except TypeError:
            c2 = 0
        key = "NewRuleMatchesAdded"
        self.ordKeys.append(key)
        self.valMap[key] = int(c1) - c2
        rs.set("TotalRuleMatchCountTmp", c1)
        #
        c1 = rs.get("TotalTokensCount")
        try:
            c2 = int(rs.get("TotalTokensCountTmp"))
        except TypeError:
            c2 = 0
        key = "NewTermsAdded"
        self.ordKeys.append(key)
        self.valMap[key] = int(c1) - c2
        rs.set("TotalTokensCountTmp", c1)
        #
        try:
            self.lasttime = datetime.datetime.strptime(
                    rs.get("LastDate").split(".")[0],"%Y-%m-%d %H:%M:%S")
        except TypeError:
            self.lasttime = ""
            sys.stderr.write("LastDate not present?\n")
        except AttributeError:
            self.lasttime = ""
            sys.stderr.write("LastDate not present?\n")
        rs.set("LastDate", str(datetime.datetime.now()))
        #
        self.ordKeys.reverse()
        self.ruleKeys.reverse()

    def __repr__(self):
        res = '%s\n'%datetime.datetime.now()
        res += 'New... items since: %s\n'%self.lasttime
        cnt = 0
        for key in self.ordKeys:
            if not key.endswith("Tmp"):
                cnt += 1
                tmp = 25 - len(key)
                res += "%s %s %5d (%2.5f)\n"%(key, "."*tmp,  
                    self.valMap[key], self.valMap[key]/float(self.valMap["TotalTokensCount"]))
                if cnt >= limit+4:
                    break
        cnt = 0
        for key in self.ruleKeys:
            cnt += 1
            tmp = 25 - len(key)
            res += "%s %s %5d (%2.5f)\n"%(key, "."*tmp,  
                    self.ruleMap[key], self.ruleMap[key]/float(self.valMap["TotalRuleMatchCount"]))
            if cnt >= limit+2:
                break
        return res

if __name__ == '__main__':
    print RedisFreq()

