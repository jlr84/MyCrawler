import json
from bson import json_util 
import BlackSpider
from BlackSpider import RunSpider

# This script provides a sample 'source data' entry to the RunSpider
# function within BlackSpider.py for testing purposes.

# TODO: Verify the object ID here is in the correct format!
# SAMPLE DATA
json_string = '''
{
	"_id": {
		"$oid": "56cc96ea882de3d81c0f0a99"
	},
        "name": "MalwareDomains.com",
        "url": "http://mirror1.malwaredomains.com/files/domains.txt",
        "updateFrequency": 86400,
        "type": "domain",
        "format": "raw",
        "columnStructure": ["nextvalidation", "url", "type", "source", "noitce"],
        "discardColumns": ["none"],
        "commentDelimiter": "##",
        "itemDelimiter": "TAB",
        "skipLines": 4,
        "html": [{
                "name": "ValueX",
                "xpath": "//base/@href"
        }],
        "createdAt": "2016-02-23T17:29:14.864Z",
        "updates": [{
                "createdAt": "2016-01-23T17:29:14.864Z",
                "status": "success"
        }, {
                "createdAt": "2016-02-23T17:29:14.864Z",
                "status": "failure"
        }]
}
'''

d=json.loads(json_string, object_hook=json_util.object_hook)

print "JSON TEST - Parsed Data: "
print "My object ID is: {0}".format(d["_id"])
print "Name: {0}\nURL: {1}\nFormat: {2}\nDelimiter: {3}".format(d['name'], d['url'], d['format'], d['itemDelimiter'])

RunSpider( d );

print "\nEND OF TEST\n"
