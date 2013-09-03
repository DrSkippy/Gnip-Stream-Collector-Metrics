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
    def __init__(self, _buffer, _feedname, _savepath, _rootLogger, _startTs, _spanTs, **kwargs):
        self.sql_instance = kwargs["sql_instance"]
        self.sql_user_name = kwargs["sql_user_name"]
        self.sql_password = kwargs["sql_password"]
        self.sql_db = kwargs["sql_db"]
        SaveThread.__init__(self, _buffer, _feedname, _savepath, _rootLogger, _startTs, _spanTs, **kwargs) 

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
            #
            verb_list = []
            lang_list = []
            for t in data:
                verb_count_list =  data[t]["verbs"].items()
                dates = [ t for i in range(len(verb_count_list))]
                counts = [ i[1] for i in verb_count_list]
                verb_list.extend(zip(dates, verb_count_list, counts))
                #
                lang_count_list =  data[t]["langs"].items()
                dates = [ t for i in range(len(lang_count_list))]
                counts = [ i[1] for i in lang_count_list]
                verb_list.extend(zip(dates, lang_count_list, counts))
        """
        # connect to the db
        db=MySQLdb.connect(
                user=self.sql_user_name), 
                passwd=self.sql_password,
                host=self.sql_instance,
                db=self.sql_db
                )
        try:
            c = db.cursor()
            sql = """INSERT INTO table (datetime, verb, count) VALUES (%s,%s,%s) 
                     ON DUPLICATE KEY UPDATE count=count + %s;"""
            c.execute(sql, zip(
            db.commit()
        except Exception, e:
            print >>sys.stderr,"A key_queue table lock or locker flag error occured (%s)"%str(e)
            db.rollback()
        """

        sys.exit(0)
