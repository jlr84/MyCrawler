#!/usr/bin/env python
import sys
import pymongo
from pymongo import MongoClient
import urllib
import mechanize
from bs4 import BeautifulSoup
import re

#Store The Search Results
def process_item(keyword,results):
    client = MongoClient()
    db = client.collector
    collection = db.discoveredlinks

    for item in results:
        num = collection.find({'url':item}).count()
        if num > 0:
            print 'The record exist'
        else:
            #Every item is valid for crawl by default
            collection.insert({'url':item,'processed':False,'keyword':keyword})

#Print All Valid Record for Crawl
def print_item():
    client = MongoClient()
    db = client.collector
    collection = db.url_collection

    records = list(collection.find({'processed':False}))
    print "The Valid URL for Crawl is:"
    for item in records:
        dic = item
        print "dict['url']: ", dic['url']

#Search links for recording in DB
def getGoogleLinks(link):

    br = mechanize.Browser()
    br.set_handle_robots(False)
    br.addheaders = [('User-agent','chrome')]

    term = link.replace(" ","+")
    query = "http://www.google.com/search?num=10&q="+term
    htmltext = br.open(query).read()
    soup = BeautifulSoup(htmltext,'html.parser')
    search = soup.findAll('div',attrs={'id':'search'})
    searchtext = str(search[0])
    soup1 = BeautifulSoup(searchtext,'html.parser')
    list_items = soup1.findAll('a')

    regex = "q=http(?!.*webcache).*?&amp"
    pattern = re.compile(regex)

    results_array = []

    for li in list_items:
        soup2 = BeautifulSoup(str(li),'html.parser')
        links = soup2.findAll('a')
        source_link = links[0]
        source_url = re.findall(pattern,str(source_link))
        if len(source_url)>0:
            results_array.append(str(source_url[0].replace("q=","").replace("&amp","")))
    return results_array

#Verify Parameters
number = len(sys.argv)
if number != 2:
    print "Usage: ./getlinks keyword"
else:
    search = sys.argv[1]
    #print search
    results_array = []
    results_array = getGoogleLinks(search)
    #print results_array
    process_item(search,results_array)

