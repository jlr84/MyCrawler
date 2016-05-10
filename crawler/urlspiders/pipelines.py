# -*- coding: utf-8 -*-

import pymongo
from datetime import datetime
from scrapy.conf import settings
from scrapy.exceptions import DropItem
from scrapy import log

class UrlspidersPipeline(object):
    def __init__(self):
        connection = pymongo.MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        db = connection[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]


    # Process each item and add to DB if good
    def process_item(self, item, spider):
        valid = True

	# Ensure data is correct type as defined in items.py; if valid, process for DB
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            # Add to DB if not present; update DB if already present (based on url)

            timestamp = str(datetime.now())
            datasource = spider.start_urls[0]
	    srcID = item['sourceID']

	    # If data type is URL, process based on URL Logic. 
	    if item['dataType'] == "URL":

                # Check to see if present in DB; if not add; if so, update
    	        if self.collection.count({'url': item['url']}) == 0:
		    self.collection.insert_one({'url': item['url'], 
                            'sourceID': srcID,
		            'createdAt': timestamp,
		            'lastUpdate': timestamp}) 
		    log.msg("URL ADDED to database!", level=log.DEBUG, spider=spider)
	        else:
		    self.collection.update_one({'url': item['url']
		    	    }, {'$set': {
			    'lastUpdate': timestamp,
			    'source': datasource}}, upsert=False)
                    log.msg("URL updated!",
                            level=log.DEBUG, spider=spider)

	    # If data type is MD5, process based on MD5 logic.
	    elif item['dataType'] == "MD5":
		#TODO: Add this logic when MD5 is implemented.
		log.msg("ERROR: MD5 logic not implemented",
			level=log.DEBUG, spider=spider)

	    else:
		log.msg("ERROR: Data source type NOT FOUND", 
			level=log.DEBUG, spider=spider)

        return item
