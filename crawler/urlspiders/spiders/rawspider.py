import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
from items import UrlspidersItem
import StringIO
import csv
import urllib
import re

class RawSpider(scrapy.Spider):
    name = 'Raw-Spider-Base'
    sourceID = "999999999"
    # DEFAULT Parameters:
    # TODO: Change this to reference parser_setings.py file for default value
    column_structure = ['fqdn', 'ip', 'url', 'hash', 'description',\
                            'lastseen', 'confidence']
    discard_columns = []
    comment_delimiter = "#"
    item_delimiter = ","
    skip_num_lines = 0    


    def parse(self, response):
        hxs = Selector(response)
        myquery = hxs.xpath('//body').extract()[0]

	# NOTE: myquery now contains entire "raw" body web page; at beginning
	# of string is '<body><p>' and at end of string is '</p></body>'.

	# TODO: Change this parsing logic to use pandas instead
	f = StringIO.StringIO(myquery)
	querylist = list(csv.reader(f, delimiter=self.item_delimiter))

	items = []  # NOTE - KEEP this (I believe)

	for i in range(self.skip_num_lines, len(querylist)):
	    if len(querylist[i]) >= 2:

        	item = UrlspidersItem()  # NOTE - KEEP this (I believe); define this in ../items.py
		item['dataType'] = "URL"
		item['sourceID'] = self.sourceID
		host = querylist[i][2] 
		item['url'] = host

        	items.append(item)  # NOTE - KEEP this (I believe); except add each row of data as an item and then append that item to 'items' array.

        return items  # NOTE - return 'items' array, which will push through pipeline (I believe), which contains the logic for adding to database


