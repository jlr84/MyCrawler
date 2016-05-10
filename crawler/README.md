# Crawler

## Info

This directory contains files for running the crawler portion of the Collector project.
Library dependencies are specified in the "requirements.txt" file and can be installed with:
"pip install -r requirements.txt"

## Modules

### Blacklist Crawler
- ./urlspiders/ -- contains functionality for crawling/parsing sources 
- ./urlspiders/BlackSpider.py -- contains the main calling function 'RunSpider()' which takes a source-data dictionary object and calls specified spider function to create/run appropriate spider
- ./urlspiders/settings.py -- contains database settings
- ./urlspiders/items.py -- specifies items anticipated during crawl/parse of webpages/files
- ./urlspiders/piplines.py -- defines actions taken to verify each data item and add to DB
- ./urlspiders/spiders/ -- contains generic spiders for each type of data crawled/parsed
         **Need help making this more "generic"... still working on that part**

### New Blacklist Search / Recommendation
- ./googlesearch/ -- contains search functionality designed to find new possible source lists


### Other Notes:
Environment Used 

1. Ubuntu-14.04.3-desktop-i386 
2. intsall MongoDB 
 * sudo apt-get install mongodb 
3. install Python-pip 
 * sudo apt-get install python-pip 
4. install pymongo
 * sudo pip install pymongo 
5. install python-dev 
 * sudo apt-get install python-dev 
6. install scrapy 
 * sudo pip install scrapy 
7. Create user account 
 * use my_database 
 * db.addUser("admin", "admin") 
 * Note: my_database is the name of a database 


MongoDB Reference 

1. Access to MongoDB 
 * Mongo 
2. Display Database 
 * show dbs 
3. Reference Database 
 * use test_database 
4. Disply Collections 
 * show collections 
5. Query 
 * db.test_collections.find() 
 * (select * from test_collections) 
6. Query on condition 
 * db.test_collections.find({"name": Georgia}); 
 * (select * from test_collections where age = 22;) 



## Files In This Directory

#### database.py

Class for generic database operations (e.g. create, find)

Usage:

````python
import database

# Create new database object
db = database.Database()

# Set collection that will be searched
collection = "sources"

# Find all items in "sources" collection
# Results returned as a Pandas Dataframe
df_allsources = db.find(collection)

# Find documents in "sources" collection based on
# specific search criteria
# Results returned as Pandas Dataframe
query = {"format" : "raw"}
df_rawsources = db.db_find(collection, query)

# Insert items from a Panadas Dataframe into a collection
db.db_insert("items", df_items)
```

#### raw_parser.py

Parses datasources of raw type (e.g. CSV, TSV, etc)

Usage:

```python

import raw_parser as raw

rp = raw.RAWParser()
# URL of the list to be parsed
file_path = "http://example.url/domain_list.csv"
# Layout of columns in the list to be parsed
column_structure = ['fqdn', 'description', 'info']
# Columns to discard after reading the list (i.e. columns that will not be saved/inserted into DB)
discard_columns = ['info']
# Character that precedes a commment line
comment_delimiter = "#"
# Character that indicates separation between items on a line
item_delimiter = ","
# Skip the first n lines from the file before parsing starts
skip_num_lines = 0

df_result = rp.raw_parse(file_path, column_structure, discard_columns, comment_delimiter, item_delimiter, skip_num_lines)
```

#### scheduler.py

Class containing functions used to schedule events.
Pulls sources/scheduling info from database and schedules appropriate parsing job

Functions:
```
process_raw(String source_id) - Retrieve RAW type source with object ID "source_id" from "sources" collection, parse info, and store results in database

schedule_raw(Dataframe df_rawsources) - Takes as input a Pandas Dataframe containing all info about a RAW source. Schedules a job to parse that source as appropriate.

load_sources() - Loads all sources from database "source" collection
```

#### Configuration Files

Miscellaneous configuration files that are needed by the different classes are also present.

These files include:

```
db_settings.py - Generic settings for database operations (i.e. hostname/port of database server, database name)

parser_settings.py - Default values for parser variables

scheduler_settings.py - Default values for scheduler variables
```
