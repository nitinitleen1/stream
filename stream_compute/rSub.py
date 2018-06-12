import argparse
import logging
#import webapp2, json, logging, os, time, uuid, hashlib, cgi , sys
import urllib
import json,time
from google.cloud import pubsub
from collections import OrderedDict
from google.cloud import bigquery
from datetime import date, timedelta,datetime
from google.cloud import pubsub


def subscriber():
    pubsub_client = pubsub.Client()
    topic_name='vuukle-messages'
    topic = pubsub_client.topic(topic_name)
    subscription_name='sub3'
    subscription = topic.subscription(subscription_name)

    while True:
        temp1=[]
        #try:
        results = subscription.pull(return_immediately=True,max_messages=15)
        print('Received {} messages.'.format(len(results)))
        for ack_id, message in results:
            #print('* {}: {}, {}'.format(message.message_id, message.data, message.attributes))
            temp1.append(str(message.data))
                 
        try:
                   
            dataset_name = 'vuukle_dataset'
            table_name   = 'page_impression_logs'
            #today = date.today().strftime("%Y%m%d")
            data1 = json.loads(temp1[0],object_pairs_hook=OrderedDict)
            loc = datetime.strptime(data1['PAGE_VIEW_TIMESTAMP'],"%Y-%m-%d %H:%M:%S")

            today=loc.strftime("%Y%m%d")
            table_name = "%s$%s"%(table_name, today)

            #putting data into bigquery
            bigquery_client = bigquery.Client()
            dataset = bigquery_client.dataset(dataset_name)
            table = dataset.table(table_name)
            table.reload()
            schema = table.schema
            schema_names = [o.name for o in schema]
            records = []
            for i in range(0,len(temp1)):
                data = json.loads(temp1[i],object_pairs_hook=OrderedDict)
                        
                ## get the names of schema
                temp=list()
                for key in schema_names:
                     if data.has_key(key):
			   if key == "referrer":
                                if type(data[key]) is list:
                                        data[key]= " "
                                else:
                                        data[key]= data[key]
		           temp.append(data[key])
		           #rows = c[key]
		#print rows
	             else:
		           temp.append(" ") 
                records.append(temp)
                
    
            try:
               errors = table.insert_data(records)
            except:
               print "not done"
            if not errors:
                logging.debug('Loaded 1 row into {}:{}'.format(dataset_name, table_name))
            else:
                logging.error(errors)
            if results:
                subscription.acknowledge([ack_id for ack_id, message in results])
        except: 
            print "done doing"
            #i=i+1
        #if results:
        #    subscription.acknowledge([ack_id for ack_id, message in results])
            #print( "one acknowledged")
        #except:
        #    print("next")
            #continue

while True:
    try:
        subscriber()
    except:
        print "service unavailable"
