import scrapy
import json
from scrapy.crawler import CrawlerProcess
from scrapy.conf import settings
from spiders.rawspider import RawSpider
from jsonschema import validate
from jsonschema import ValidationError
from urlparse import urlparse


# Define anticipated schema for source object
SOURCE_SCHEMA = {
    "type" : "object",
    "properties" : {
        "name": {"type" : "string"},
        "url" : {"type" : "string"},
        "updateFrequency" : {"type" : "number"},
        "type" : {"type" : "string"},
        "format" : {"type" : "string"},
	"columnStructure": {
	    "type" : "array",
	    "items": { 
		"type": "string"
	    }
	},
	"discardColumns": {
	    "type" : "array",
	    "items": {
		"type": "string"
	    }
	},
	"commentDelimiter": {"type" : "string"},
	"itemDelimiter": {"type" : "string"},
	"skipLines": {"type" : "number"},
	"html": {
	    "type" : "array", 
	    "items" : {
		"type" : "object", 
		"properties" : {
		    "name": {"type" : "string"},
		    "xpath" : {"type" : "string"}
		}
	    }
	}
    },
    "required" : ["name"]
}


# Simple Exception class for handling Parse Errors
class ParseError(Exception):
    def __str__(self):
	print "\nERROR WITHIN PARSE:"
	print self.message
	print self.context
	print self.cause
	return


# Function for calling a RAW format Spider
def CallSpiderRAW( src ): 
    print "Running RAW Spider for '{0}'...\n".format(src['name'])
    
    # Set Source Data Fields Locally 
    name = src['name']
    idnum = src['_id']

    # FYI: Parsed URL result has these indexes: 0=scheme; 1=netloc; 2=path; 3=params
    myurl = urlparse( src['url'] ) 
    if myurl[1] == '':
	ad = myurl[2]
    else:
	ad = myurl[1]
    sd = src['url']  
    hdrs = src['columnStructure']
    discol = src['discardColumns']
    cdel = src['commentDelimiter']
    delimtemp = src['itemDelimiter'] 
    if delimtemp == "TAB":
	delim = "\t"
	delimtemp = "\"\\t\" [TAB]"
    else:
	delim = delimtemp
    sline = src['skipLines']

    print """Configuration: 
	Name:              {0}
	Allowed Domain:    {1}
	Start URL:         {2}
	Column Structure:  {3}
	Discard Columns:   {4}
	Comment Delimiter: {5}
	Item Delimiter:    {6}
	Skip # Lines:      {7}
""".format(name, ad, sd, hdrs, discol, cdel, delimtemp, sline)

    print "\nExecuting spider now:\n"
    process = CrawlerProcess(settings)
    process.crawl(RawSpider, allowed_domains=[ad], start_urls=[sd], sourceID=idnum, column_structure=hdrs, discard_columns=discol, comment_delimiter=cdel, item_delimiter=delim, skip_num_lines=sline)
    # TODO: Improve 'RawSpider' to utilize additional stored attributed as 
    # shown in this commented-out function call:
    # process.crawl(RawSpider, allowed_domains=ad, start_urls=sd, delimiter=delim, quotechar=qc, headers=hdrs)
    process.start()

    ## TODO: Figure out how to use 'stats' to verify a good crawl before returning 0
    return 0;


# Function for calling an HTML format Spider
def CallSpiderHTML( src ):
    print "Spider for HTML Format not implemented yet. Nice try.\n"
    return 1;


# Main function for calling spiders based on dictionary input
def RunSpider( mysource ):
    rc = 2;
    try:
	# Validate 'mysource' passed in based on above schema 
        validate(mysource, SOURCE_SCHEMA)
    except ValidationError as error:
	print "\n\n\nPARSE ERROR....\n\n\nEXITING NOW.\n\n\n"
        raise ParseError(error)

    if mysource['format']=='raw':
        rc = CallSpiderRAW( mysource )

    elif mysource['format']=='html':
	rc = CallSpiderHTML( mysource )

    else:
	print "Functionality '{0}' NOT Implemented.".format(mysource['format'])
	rc = 1;

    print "Return code: {0}\n".format(rc)


# UNCOMMNET the following to test running this file (instead of calling 
# 'RunSpider()' from a separate script):

# SAMPLE DATA
#json_string = '''
#{
#        "name": "MalwareDomains.com",
#        "url": "http://mirror1.malwaredomains.com/files/domains.txt",
#        "updateFrequency": 86400,
#        "type": "domain",
#        "format": "raw",
#        "columnStructure": ["nextvalidation", "url", "type", "source", "noitce"],
#        "discardColumns": ["none"],
#        "commentDelimiter": "##",
#        "itemDelimiter": "TAB",
#        "skipLines": 1,
#        "html": [{
#                "name": "ValueX",
#                "xpath": "//base/@href"
#        }],
#        "createdAt": "2016-02-23T17:29:14.864Z",
#        "updates": [{
#                "createdAt": "2016-01-23T17:29:14.864Z",
#                "status": "success"
#        }, {
#                "createdAt": "2016-02-23T17:29:14.864Z",
#                "status": "failure"
#        }]
#}
#'''
#
#d=json.loads(json_string)
#
#print "JSON TEST - Parsed Data: "
#print "Name: {0}\nURL: {1}\nFormat: {2}\nDelimiter: {3}".format(d['name'], d['url'], d['format'], d['itemDelimiter'])
#
#RunSpider( d );

