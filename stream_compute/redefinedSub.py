import argparse
import logging
#import webapp2, json, logging, os, time, uuid, hashlib, cgi , sys
import urllib
import json,time
from google.cloud import pubsub
from collections import OrderedDict
from google.cloud import bigquery
from datetime import date, timedelta
from google.cloud import pubsub


def subscriber():
    pubsub_client = pubsub.Client()
    topic_name='my-new-topic'
    topic = pubsub_client.topic(topic_name)
    subscription_name='sub2'
    subscription = topic.subscription(subscription_name)

    while True:
        temp1=[]
        #try:
        results = subscription.pull(return_immediately=True,max_messages=1000)
        print('Received {} messages.'.format(len(results)))
        for ack_id, message in results:
            #print('* {}: {}, {}'.format(message.message_id, message.data, message.attributes))
            temp1.append(str(message.data))
                 
        try:
                   
            dataset_name = 'searce_poc_vuukle'
            table_name   = 'page_impressions'
            today = date.today().strftime("%Y%m%d")
            table_name = "%s$%s"%(table_name, today)

            #putting data into bigquery
            bigquery_client = bigquery.Client()
            dataset = bigquery_client.dataset(dataset_name)
            table = dataset.table(table_name)
            table.reload()
            records = []
            for i in range(0,len(temp1)):
                data = json.loads(temp1[i],object_pairs_hook=OrderedDict)
                    
                        
                ## get the names of schema
                temp=list()
                for key in data:
                    temp.append(data[key])
                    #rows = [data]
                    #print rows
                records.append(temp)
            errors = table.insert_data(records)
            if not errors:
                logging.debug('Loaded 1 row into {}:{}'.format(dataset_name, table_name))
            else:
                logging.error(errors)
        except:
            print "could not load"
            i=i+1
        if results:
            subscription.acknowledge([ack_id for ack_id, message in results])
            #print( "one acknowledged")
        #except:
        #    print("next")
            #continue

subscriber()

