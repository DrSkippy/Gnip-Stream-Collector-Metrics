#!/usr/bin/env python
__author__ = 'Jeff Kolb'

from SaveThread import SaveThread
from twacscsv import TwacsCSV
import gzip
import json

class SaveThreadGnacs(SaveThread):
    def __init__(self, _buffer, _feedname, _savepath, _rootLogger, _startTs, _spanTs, **kwargs):
        self.geo = 'g' in kwargs['options']
        self.user = 'u' in kwargs['options']
        self.rules = 'r' in kwargs['options']
        self.urls = 's' in kwargs['options']
        self.lang = 'l' in kwargs['options']
        self.influence = 'i' in kwargs['options']
        self.struct = 't' in kwargs['options']
        self.delim = kwargs['delim']
        SaveThread.__init__(self, _buffer, _feedname, _savepath, _rootLogger, _startTs, _spanTs, **kwargs)

    def write(self, file_name):
        try:
            # set up Gnacs object
            gnacs = TwacsCSV(self.delim, self.geo, self.user, self.rules, self.urls, self.lang, self.influence, self.struct)
            
            # write gnacs-ified output to file
            fp = gzip.open(file_name, "a")
            buffer_formated = ''
            for act in self.string_buffer.split("\n"):
                if act.strip() is None or act.strip() == '':
                    continue
                act_json = json.loads(act)
                try:
                    act_formated = gnacs.asString(gnacs.procRecordToList(act_json)).encode('utf-8')
                except Exception, e:
                    self.logger.error("gnacs JSON->list->str formatting failed: {0}".format(e))
                    raise e
                buffer_formated += str(act_formated) + '\n'
            fp.write(buffer_formated)
            fp.close()
            self.logger.info("saved file %s"%file_name)
        except Exception, e:
            self.logger.error("write failed: %s"%e)
            raise e
