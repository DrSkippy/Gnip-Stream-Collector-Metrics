#!/usr/bin/env python
__author__ = 'scott hendrickson'

import sys
import MySQLdb
import datetime
from threading import RLock
import operator
import re
try:
    import ujson as json
except ImportError:
    import json

from SaveThread import SaveThread

write_lock = RLock()

# minutes
BUCKET_SIZE = 15

fmt = "%Y-%m-%dT%H:%M:%S"
datetime_re = re.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}.[0-9]{2}:[0-9]{2}:[0-9]{2}")

i0 = operator.itemgetter(0)
i1 = operator.itemgetter(1)

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
            except ValueError as e:
                self.logger.error("Invalid JSON record (%s)"%e)
                continue
            posted_time = datetime.datetime.utcnow()
            if "postedTime" in activity:
                short_datetime_re = datetime_re.search(activity["postedTime"])
                if short_datetime_re:
                    posted_time = datetime.datetime.strptime(short_datetime_re.group(0).replace(" ","T"),fmt)
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
            count_list = []
            for t in data:
                count_list.append((t, data[t]["count"], data[t]["count"]))
                #
                verb_count_list =  data[t]["verbs"].items()
                dates = [ t for i in range(len(verb_count_list))]
                counts = [ i[1] for i in verb_count_list]
                verb_list.extend(zip(dates, map(i0, verb_count_list), map(i1, verb_count_list), counts))
                #
                lang_count_list =  data[t]["langs"].items()
                dates = [ t for i in range(len(lang_count_list))]
                counts = [ i[1] for i in lang_count_list]
                lang_list.extend(zip(dates, map(i0, lang_count_list), map(i1, lang_count_list), counts))
        self.logger.info("Preparing to insert (counts %d, languages %d, verbs %d) records to db %s."%
                (len(count_list), len(lang_list), len(verb_list), self.sql_db))

        # connect to the db
        db=MySQLdb.connect(
                user=self.sql_user_name, 
                passwd=self.sql_password,
                host=self.sql_instance,
                db=self.sql_db
                )
        try:
            c = db.cursor()
            sql_count = """INSERT INTO counts (datetime, count) VALUES (%s,%s) 
                           ON DUPLICATE KEY UPDATE count=count + %s;"""
            c.executemany(sql_count, count_list)
            sql_verb = """INSERT INTO verb (datetime, verb, count) VALUES (%s,%s,%s)
                           ON DUPLICATE KEY UPDATE count=count + %s;"""
            c.executemany(sql_verb, verb_list)
            sql_lang = """INSERT INTO language (datetime, language, count) VALUES (%s,%s,%s) 
                           ON DUPLICATE KEY UPDATE count=count + %s;"""
            c.executemany(sql_lang, lang_list)
            db.commit()
        except Exception as e:
            sys.stderr.write("A MySQL insert error occured (%s)"%str(e))
            db.rollback()
        sys.exit(0)
