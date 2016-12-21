#!/usr/bin/env python
__author__ = 'Fiona Pigott'

from SaveThread import SaveThread
import gzip
import json
import datetime 


# Twitter Snowflake ID to timestamp (and back)
# https://github.com/client9/snowflake2time/
# Nick Galbreath @ngalbreath nickg@client9.com
# Public Domain -- No Copyright -- Cut-n-Paste!
def snowflake2utc(sf):
    sf_int = int(sf)
    return int(((sf_int >> 22) + 1288834974657) / 1000.0)
def make_utc_timestamp(timestamp):
    date_obj = datetime.datetime.strptime(timestamp.split(".")[0], "%Y-%m-%dT%H:%M:%S")
    return str(int((date_obj - datetime.datetime(1970,1,1)).total_seconds()))

class SaveThreadGnacs(SaveThread):
    def __init__(self, _buffer, _feedname, _savepath, _rootLogger, _startTs, _spanTs, **kwargs):
        SaveThread.__init__(self, _buffer, _feedname, _savepath, _rootLogger, _startTs, _spanTs, **kwargs)

    def write(self, file_name):
        try:
            # create CSV output and write it to an output to file
            fp = gzip.open(file_name, "a")
            buffer_formated = ''
            for act in self.string_buffer.split("\n"):
                if act.strip() is None or act.strip() == '':
                    continue
                act_json = json.loads(act)
                try:
                    # tweetid, time of tweet, time of like, user tweeting, user liking,
                    time_of_tweet = str(snowflake2utc(act_json["object"]["id"].split(":")[-1]))
                    time_of_like = make_utc_timestamp(act_json["postedTime"]) 
                    act_formated = (
                        act_json["object"]["id"].split(":")[-1] + "," +
                        time_of_tweet + "," + time_of_like + "," +
                        act_json["actor"]["id"].split(":")[-1] + "," +
                        act_json["object"]["actor"]["id"].split(":")[-1]
                        )
                except Exception as e:
                    self.logger.error("Custom JSON->list->str formatting failed: {0}".format(e))
                    raise e
                buffer_formated += str(act_formated) + '\n'
            fp.write(buffer_formated)
            fp.close()
            self.logger.info("saved file %s"%file_name)
        except Exception, e:
            self.logger.error("write failed: %s"%e)
            raise e

# add more custom outputs to this file
