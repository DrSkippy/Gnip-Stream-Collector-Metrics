#!/usr/bin/env python
#
#   scott hendrickson (shendrickson@gnipcentral.com)
#      2011-08-23 09:53:47.873347
#
#####################
__author__ = 'scott hendrickson'

import sys
import redis
import re

limit = 100
#####################

class RedisFreq(object):
    def __init__(self):
	rs = redis.Redis("localhost")
	keys = rs.keys()
	self.valMap = {}
	for key in keys:
		try:
			if key[0] != "[":
				self.valMap[key] = int(rs.get(key))
		except ValueError:
			pass
	self.ordKeys = sorted(self.valMap.keys(), key=self.valMap.__getitem__)
        self.ordKeys.reverse()

    def __repr__(self):
	    res = ''
	    cnt = 0
	    for key in self.ordKeys:
		    cnt += 1
		    tmp = 25 - len(key)
		    res += "%s %s %d (%2.5f)\n"%(key, "."*tmp,  
				    self.valMap[key], self.valMap[key]/float(self.valMap["TotalTokensCount"]))
		    if cnt >= limit:
			    break
	    return res

if __name__ == '__main__':
	print RedisFreq()

