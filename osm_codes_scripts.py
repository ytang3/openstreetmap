
# coding: utf-8

# ## OpenStreetMap Data Case - Codes Scripts

# #### Downloading a Sample Part of the Area

# In[3]:


import xml.etree.ElementTree as ET  
OSM_FILE = "new-orleans_louisiana.osm"  
SAMPLE_FILE = "sample.osm"

k = 20 

def get_element(osm_file, tags=('node', 'way', 'relation')):

    
    context = iter(ET.iterparse(osm_file, events=('start', 'end')))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


with open(SAMPLE_FILE, 'wb') as output:
    output.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    output.write('<osm>\n  ')

    # Write every kth top level element
    for i, element in enumerate(get_element(OSM_FILE)):
        if i % k == 0:
            output.write(ET.tostring(element, encoding='utf-8'))

    output.write('</osm>')


# #### Iterative Parsing  

# Through the count_tags function, we could get a dictionary called "tag_freq". It shows tag names as the key and number of times they showed in the dataset as the value.

# In[4]:


#count_tags function returns a dictionary with the 
#tag name as the key and number of times this tag can be encountered in 
#the map as value.

import pprint

def count_tags(filename):
    tag_freq={}
    
    for event, elem in ET.iterparse(filename):
        if elem.tag not in tag_freq:
            tag_freq[elem.tag]=1
        else:
            tag_freq[elem.tag]+=1
             
    return tag_freq


# In[5]:

count_tags(SAMPLE_FILE)


# ##### Checking the "k" value

# In[6]:

import re

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')


def key_type(element, keys):
    if element.tag == "tag":
        
        k_value=element.get("k")
        
        l=re.search(lower,k_value)
        lc=re.search(lower_colon,k_value)
        p=re.search(problemchars,k_value)
        
        if l:
            keys['lower']+=1
            
        elif lc:
            keys['lower_colon']+=1
            
        elif p: 
            keys['problemchars']+=1
            
        else:
            keys['other']+=1
        
        pass
  
    return keys



def process_map(filename):
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    for _, element in ET.iterparse(filename):
        keys = key_type(element, keys)

    return keys


# In[7]:

process_map(SAMPLE_FILE)


# #### Checking street names

# In[22]:

from collections import defaultdict

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

#I firstly input normal street types like "Street","Avenue" and etc. into the expected list. Then 
#run the audit(SAMPLE_FILE) function to see the results. After that, I adjusted the expected list 
#manually by adding other street types into the list. Also, I updated the mapping dictionary after 
#looking through following results.

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Lane", "Road", 
            "Heights","Parkway","Terrace","Alley","Corner","Cove","Circle","Highway","Park",
            "Trace","Trail","View","Village","Randch","Way","Walk","Loop","Bayou","Hollow","Hill","Ridge",
           "North","West","East"]


mapping = { "St": "Street",
            "St.": "Street",
            "Ave": "Avenue",
            "Rd.": "Road",
           "Pky":"Parkway",
           "Villa":"Village",
           "Lp":"Loop"
            }


def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)


def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")


def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
    osm_file.close()
    
    return street_types


def update_name(name, mapping):

    m = street_type_re.search(name)
    #print m.group()
    if m.group() not in expected:
        if m.group() in mapping.keys():
            name = re.sub(m.group(), mapping[m.group()], name)
    
    return name

#audit(SAMPLE_FILE)


def test():
    st_types=audit(SAMPLE_FILE)
    for st_type, ways in st_types.iteritems():
        for name in ways:
            better_name = update_name(name, mapping)
            print name, "=>", better_name


if __name__ == '__main__':
    test()


# ### Prepare Dataset for SQL

# In[16]:


import csv
import codecs

import cerberus

import schema


# In[17]:



OSM_PATH = "sample.osm"

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"


SCHEMA = schema.schema



# In[29]:

#-Shape each element into several data structures using a custom function

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

#The shape_element function transforms each element into the correct format. 
#It takes as input an iterparse Element object and return a dictionary organizing 
#all the information into the correct format. 

#For example, for elements whose top level tag is "node", the "shape_element" function returns 
#a dictionary whose format is {"node": .., "node_tags": ...}.
#To be specific, the "node" key holds attributes like "id","user","uid","version","lat","lon","timestamp" and
#"changeset" for its top level "node". And "node_tags" key holds a list of dictionaries which includes
#attributes like "id","key","value" and "type" for its secondary tag.


def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements

   
    if element.tag == 'node':
        for attrib in element.attrib:
            if attrib in NODE_FIELDS:
                node_attribs[attrib]=element.attrib[attrib]
            
        for child in element:
            node_tag={}

            if LOWER_COLON.search(child.attrib['k']):
                if child.attrib['k'] == 'addr:street':   
                    node_tag['value'] = update_name(child.attrib['v'], mapping)
                    node_tag['id'] = element.attrib['id']
                    node_tag['key'] = child.attrib['k'].split(':',1)[1]
                    node_tag['type'] = child.attrib['k'].split(':',1)[0]
                    tags.append(node_tag)
                else:   
                    node_tag['id'] = element.attrib['id']
                    node_tag['key'] = child.attrib['k'].split(':',1)[1]
                    node_tag['type'] = child.attrib['k'].split(':',1)[0]
                    node_tag['value'] = child.attrib['v']
                    tags.append(node_tag)
                    
            elif PROBLEMCHARS.search(child.attrib['k']):
                continue
                
            else:
                node_tag['id'] = element.attrib['id']
                node_tag['key'] = child.attrib['k']
                node_tag['type'] = 'regular'
                node_tag['value'] = child.attrib['v']
                tags.append(node_tag)
        
        return {'node': node_attribs, 'node_tags': tags}
        
    elif element.tag == 'way':
        for attrib in element.attrib:
            if attrib in WAY_FIELDS:
                way_attribs[attrib]=element.attrib[attrib]
                
        position=0    
        for child in element:
            way_tag={}
            way_node={}
            
            if child.tag=='tag':
                if LOWER_COLON.search(child.attrib['k']):
                    if child.attrib['k'] == 'addr:street':   
                        way_tag['value'] = update_name(child.attrib['v'], mapping)
                        way_tag['id'] = element.attrib['id']
                        way_tag['key'] = child.attrib['k'].split(':',1)[1]
                        way_tag['type'] = child.attrib['k'].split(':',1)[0]
                        tags.append(way_tag)
                    else:
                        way_tag['id'] = element.attrib['id']
                        way_tag['key'] = child.attrib['k'].split(':',1)[1]
                        way_tag['type'] = child.attrib['k'].split(':',1)[0]
                        way_tag['value'] = child.attrib['v']
                        tags.append(way_tag)
                    
                elif PROBLEMCHARS.search(child.attrib['k']):
                    continue
                
                else:
                    way_tag['id'] = element.attrib['id']
                    way_tag['key'] = child.attrib['k']
                    way_tag['type'] = 'regular'
                    way_tag['value'] = child.attrib['v']
                    tags.append(way_tag)
                    
            #print tags
            elif child.tag=='nd':
                way_node['id']=element.attrib['id']
                way_node['node_id']=child.attrib['ref']
                way_node['position']=position
                position+=1
                way_nodes.append(way_node)
            
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}


# In[30]:



# ================================================== #
#               Helper Functions                     #
# ================================================== #

#- Use iterparse to iteratively step through each top level element in the XML
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()

#- Utilize a schema and validation library to ensure the transformed data is in the correct format
#Using the cerberus library can validate the output against this schema to ensure it is correct.

def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        
        raise Exception(message_string.format(field, error_string))

#- Write each data structure to the appropriate .csv files
class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file,          codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file,          codecs.open(WAYS_PATH, 'w') as ways_file,          codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file,          codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])


if __name__ == '__main__':
    process_map(OSM_PATH, validate=False)


# ### Statistical Overview of the Dataset

# #### File sizes

# In[35]:

import os

folder = '/Users/tangyiyi/Desktop/Data Analyst/Data Wrangling/Project'
folder_size = 0


for (path, dirs, files) in os.walk(folder):
    for file in files:
        if '.ipynb' not in file and '.py' not in file and '.DS_Store' not in file and '.jpg' not in file and '.png' not in file:
            filename = os.path.join(path, file)
            folder_size = os.path.getsize(filename)
            print file, " = %0.1f MB" % (folder_size/(1024*1024.0))


# #### Number of nodes and ways

# In[38]:

import sqlite3

con = sqlite3.connect('osm.db')
cursor = con.cursor()
cursor.execute("SELECT count(*) FROM nodes;")

print(cursor.fetchall())


# In[39]:

cursor.execute("SELECT count(*) FROM ways;")

print(cursor.fetchall())


# As above, the number of nodes is 320614, and the number of ways is 18914.

# #### Number of unique users

# In[40]:

cursor.execute("SELECT count(distinct(u.uid)) FROM (Select uid FROM nodes UNION ALL SELECT uid FROM ways) u;")

print(cursor.fetchall())


# I used "union all" function to combine nodes and ways tables accroding to their uid, and named the combiend table "u". Then I counted distinct uid numbers. So as above showed, there's 482 unique users in the dataset.

# #### Number of cafes  in the dataset & Who contributed to the cafe data

# In[41]:

cursor.execute("SELECT count(*) FROM nodes_tags where value='cafe';")

print(cursor.fetchall())


# In[47]:

cursor.execute("SELECT nodes.user FROM nodes INNER JOIN nodes_tags ON nodes.id=nodes_tags.id where nodes_tags.value='cafe';")

pprint.pprint(cursor.fetchall())


# There's 7 cafe in the dataset. And user named "Matt Toups", "wheelmap_visitor", "wegavision", "lokejul", "bhelx",  "anna2233" contributed to the cafe data.

# #### How many times did user "Matt Toups" contribute to this dataset

# In[43]:

cursor.execute("SELECT count(*) FROM (SELECT user from nodes UNION ALL SELECT user from ways) u where user='Matt Toups';")

print(cursor.fetchall())


# As results, user "Matt Toups"contributed 172024 times.

# #### Sum of contributions (posted by contributors)

# In[72]:

cursor.execute("SELECT count(*) FROM (SELECT user from nodes UNION ALL SELECT user from ways) u;")

print(cursor.fetchall())


# #### Sum of top 10 contributors' contributions

# In[74]:

cursor.execute("SELECT sum(num) FROM (SELECT u.user, count(*) as num FROM (SELECT user FROM nodes UNION ALL SELECT user FROM ways) u group by u.user order by num desc limit 10) e;")

pprint.pprint(cursor.fetchall())


# #### Top 20 contributors

# In[50]:

cursor.execute("SELECT u.user, count(*) as num FROM (SELECT user FROM nodes UNION ALL SELECT user FROM ways) u group by u.user order by num desc limit 20;")

pprint.pprint(cursor.fetchall())


# #### How many contributors only contribute one time

# In[56]:

cursor.execute("SELECT count(*) FROM (SELECT u.user,count(*) as num FROM (SELECT user FROM nodes UNION ALL SELECT user FROM ways) u group by u.user HAVING num=1) i;")

pprint.pprint(cursor.fetchall())


# #### Additional data exploration using SQL

# #### Top 5 amenities

# In[75]:

cursor.execute("SELECT value, count(*) as num FROM nodes_tags WHERE key='amenity' group by value order by num desc limit 5;")

pprint.pprint(cursor.fetchall())


# #### Count amenity bar 

# In[76]:

cursor.execute("SELECT count(*) FROM nodes_tags WHERE key='amenity'and value='bar';")
pprint.pprint(cursor.fetchall())

