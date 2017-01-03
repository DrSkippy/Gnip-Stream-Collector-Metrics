#!/usr/bin/env python
__author__ = 'scott hendrickson'

import sys
import redis
import re
import datetime

limit = 150
#####################

class EmptyRedisCacheError(ValueError):
    def __init__(self, arg):
        self.strerror = arg
        self.args = {arg}

class RedisFreq(object):
    def __init__(self):
        rs = redis.Redis("localhost")
        keys = rs.keys()
        if len(keys) == 0:
            raise EmptyRedisCacheError("There are no keys in your Redis store, quitting")
        self.valMap = {}
        self.ruleMap = {}
        for key in keys:
            try:
                if key[0] != "[".encode("utf-8"):
                    self.valMap[key] = int(rs.get(key))
                else:
                    self.ruleMap[key] = int(rs.get(key))
            except ValueError as ve:
                # 'LastDate' shouldn't be an integer value
                if key == "LastDate".encode("utf-8"):
                    pass
                else:
                    sys.stderr.write("ValueError raised: "+
                            "{}\n--valMap or ruleMap Key value error on key '{}'\n".format(ve,key)) 
            except redis.exceptions.ResponseError as e:
                sys.stderr.write("Redis response errer (%s)\n"%e)
            except IndexError as ie:
                sys.stderr.write("IndexError raised: {}--\nList index error ({})\n".format(ie,key))
            except TypeError:
                sys.stderr.write("Redis returned non-int value for key=%s\n"%key)
        self.ordKeys = sorted(self.valMap.keys(), key=self.valMap.__getitem__)
        self.ruleKeys = sorted(self.ruleMap.keys(), key=self.ruleMap.__getitem__)
        c1 = rs.get("TotalRuleMatchCount".encode("utf-8"))
        try:
            c2 = int(rs.get("TotalRuleMatchCountTmp".encode("utf-8")))
        except TypeError:
            c2 = 0
        key = "NewRuleMatchesAdded".encode("utf-8")
        self.ordKeys.append(key)
        self.valMap[key] = int(c1) - c2
        rs.set("TotalRuleMatchCountTmp".encode("utf-8"), c1)
        #
        c1 = rs.get("TotalTokensCount".encode("utf-8"))
        try:
            c2 = int(rs.get("TotalTokensCountTmp".encode("utf-8")))
        except TypeError:
            c2 = 0
        key = "NewTermsAdded".encode("utf-8")
        self.ordKeys.append(key)
        self.valMap[key] = int(c1) - c2
        rs.set("TotalTokensCountTmp".encode("utf-8"), c1)
        #
        try:
            self.lasttime = datetime.datetime.strptime(
                    rs.get("LastDate".encode("utf-8")).split(".")[0],"%Y-%m-%d %H:%M:%S") #.encode("utf-8")
        except TypeError as te:
            self.lasttime = ""
            sys.stderr.write("TypeError was raised: {} \n--Is the key 'LastDate' not present?\n".format(te))
        except AttributeError as ae:
            self.lasttime = ""
            sys.stderr.write("AttributeError was raised: {} \n--Is the key 'LastDate' not present?\n".format(ae))
            sys.stderr.write("If this is the first time running RedisFreq on a Redis db ignore this AttributeError,"+
                    " as LastDate will not be present.\n")
        rs.set("LastDate".encode("utf-8"), str(datetime.datetime.now()).encode("utf-8"))
        #
        self.ordKeys.reverse()
        self.ruleKeys.reverse()

    def __repr__(self):
        res = '%s\n'%datetime.datetime.now()
        res += 'New... items since: %s\n'%self.lasttime
        cnt = 0
        for key in self.ordKeys:
            if not key.endswith("Tmp".encode("utf-8")):
                cnt += 1
                tmp = 25 - len(key)
                res += "%s %s %5d (%2.5f)\n"%(key.decode("utf-8"), "."*tmp,  
                    self.valMap[key], self.valMap[key]/float(self.valMap["TotalTokensCount".encode("utf-8")]))
                if cnt >= limit+4:
                    break
        cnt = 0
        for key in self.ruleKeys:
            cnt += 1
            tmp = 25 - len(key)
            res += "%s %s %5d (%2.5f)\n"%(key, "."*tmp,  
                    self.ruleMap[key], self.ruleMap[key]/float(self.valMap["TotalRuleMatchCount".encode("utf-8")]))
            if cnt >= limit+2:
                break
        return res

if __name__ == '__main__':
    print(RedisFreq())

