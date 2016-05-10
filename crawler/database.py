'''
database.py

Class for standard database operations

'''

import json
import pandas as pd
import db_settings as settings
from pymongo import MongoClient

class Database(object):

    def __init__(self):
        self.db_host = "mongodb://" +  settings.host + ":" + settings.port
        self.db_client = MongoClient(self.db_host)
        self.db = self.db_client[settings.db_name]

    def db_insert(self, collection_name, df_items):
        records = json.loads(df_items.to_json(orient='records'))
        result = self.db[collection_name].insert_many(records)
        return result.acknowledged

    def db_findall(self, collection_name):
        df_results = pd.DataFrame(list(self.db[collection_name].find()))    
        return df_results

    def db_find(self, collection_name, query):
        df_results = pd.DataFrame(list(self.db[collection_name].find(query)))
        return df_results
