#!/usr/bin/env python
__author__ = 'scott hendrickson'

import sys
import json
#import MySQLdb
import datetime
from threading import RLock

from SaveThread import SaveThread

write_lock = RLock()

# minutes
BUCKET_SIZE = 15

fmt = "%Y-%m-%dT%H:%M:%S.000Z"

class Metrics(SaveThread):

    def run(self):
        self.logger.debug("Metrics started")
        data = {}
        for act in self.string_buffer.split("\n"):
            if act.strip() is None or act.strip() == '':
                continue
            try:
                activity = json.loads(act)
            except ValueError, e:
                self.logger.error("Invalid JSON record (%s)"%e)
                continue
            if "postedTime" in activity:
                posted_time = datetime.datetime.strptime(activity["postedTime"],fmt)
            else:
                posted_time = datetime.datetime.utcnow()
            bucket_minute = BUCKET_SIZE * int(posted_time.minute/BUCKET_SIZE) 
            bucket_time = datetime.datetime(posted_time.year, 
                                       posted_time.month, 
                                       posted_time.day, 
                                       posted_time.hour, 
                                       bucket_minute, 
                                       0)
            bucket = str(bucket_time)
            if bucket not in data:
                data[bucket] = {"count": 0, "langs": {}, "verbs":{}}
            data[bucket]["count"] += 1
            verb = activity["verb"]
            data[bucket]["verbs"][verb] = 1 + data[bucket]["verbs"].get(verb, 0)       
            if "gnip" in activity and "language" in activity["gnip"]:
                lang = activity["gnip"]["language"]["value"]
            else:
                lang = "None"
            data[bucket]["langs"][lang] = 1 + data[bucket]["langs"].get(lang, 0)
        with write_lock:
            for t in data:
                sql = """INSERT INTO table (a, datetime, verb, count) VALUES (%s,%s,%s) 
                         ON DUPLICATE KEY UPDATE count=count + %s;"""
                for x in data[t]["verbs"].items():
                    print x
                for x in data[t]["langs"].items():
                    print x
        """
        # connect to the db
        db=MySQLdb.connect(
                user="shendrickson", 
                passwd="ss_merploft",
                host="gnipdatasciencemedium.cq5kbhkpogrx.us-west-1.rds.amazonaws.com",
                db="twitter_volume"
                )
        try:
            c = db.cursor()
            c.execute("""LOCK TABLES key_queue WRITE;""")
            db.commit()
        except Exception, e:
            print >>sys.stderr,"A key_queue table lock or locker flag error occured (%s)"%str(e)
            db.rollback()
        """

        sys.exit(0)
