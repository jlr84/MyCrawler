'''
scheduler.py

Execute reguarly scheduled scrape/parse jobs

'''

import scheduler_settings as settings
import database
import pandas as pd
import dateutil.parser as dp
import schedule
import time
import raw_parser as raw


class Scheduler(object):

    def __init__(self):
        pass

    def process_raw(self, source_id):
        db = database.Database()
        df_rawsource = db.db_find("sources", {"_id" : source_id})
        # Define values to pass to raw_parse()
        file_path = df_rawsource.iloc[0]['url']
        column_structure = df_rawsource.iloc[0]['columnStructure']
        discard_columns = df_rawsource.iloc[0]['discardColumns']
        comment_delimiter = df_rawsource.iloc[0]['commentDelimiter']
        item_delimiter = df_rawsource.iloc[0]['itemDelimiter']
        skip_num_lines = df_rawsource.iloc[0]['skipLines']

        rp = raw.RAWParser()
        df_result = rp.raw_parse(file_path, column_structure, discard_columns, comment_delimiter, item_delimiter, skip_num_lines)
        db.db_insert("items", df_result)


    def schedule_raw(self, df_rawsources):
        # Iterate through all sources with 'raw' type
        for index, source in df_rawsources.iterrows():
            print "[SCHEDULER] Working with raw source: ",source['name']
            updateFrequency = source['updateFrequency']
            print "[SCHEDULER] Update frequency is <",updateFrequency,">"
            updates = source['updates']
            if len(updates) > 0:
                # Get the most recent update
                lastUpdate = dp.parse(updates[0]['createdAt'])
                # Get the current time in seconds
                now = int(round(time.time()))
                # If time between now and the last update is greater than the
                # update interval, schedule the event
                if(now - int(lastUpdate.strftime('%s')) > updateFrequency):
                    source_id = source['_id']
                    print "[SCHEDULER] Scheduling source <",source['name'],"> with id <",source_id,">"
                    schedule.every(updateFrequency).seconds.do(process_raw, source_id)
            source_id = source['_id']
            print "[SCHEDULER] Scheduling source <",source['name'],"> with id <",source_id,">"
            schedule.every(updateFrequency).seconds.do(self.process_raw, source_id)
        # Process all scheduled items
        while True:
            schedule.run_pending()
            time.sleep(1)


    def load_sources(self):
        db = database.Database()

        # Schedule raw sources
        print "[SCHEDULER] Retrieveing raw sources via db_find()..."
        df_rawsources = db.db_find("sources", {"format" : "raw"})
        print "[SCHEDULER] Calling schedule_raw()"
        self.schedule_raw(df_rawsources)
