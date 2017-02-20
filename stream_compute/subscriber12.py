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

''' This function listens on topic having subscription named
	sub2 and fetches it and put it into biqquery'''
def subscriber():
	while True:
		counter=10
		temp1=[]
		while (counter!=-1):
			
			pubsub_client = pubsub.Client()
			topic_name='my-new-topic'
			topic = pubsub_client.topic(topic_name)
			subscription_name='sub2'
			subscription = topic.subscription(subscription_name)

			#subscription.create()

			#for subscription in topic.list_subscriptions():
		    #	   print(subscription.name)

			#logging.debug('Subscription {} created on topic {}.'.format(
		   	#	subscription.name, topic.name))
			
			try:
				results = subscription.pull(return_immediately=True)
				if counter!=0:
					#print counter
					print('Received {} messages.'.format(len(results)))

					for ack_id, message in results:
			 			#print('* {}: {}, {}'.format(
			 			        #message.message_id, message.data, message.attributes))
			 			temp1.append(str(message.data))
						#print temp1[counter]
						#var=var+1
					#print results
					#print counter
					#print b1
					#ts = self.request.get("ts")
				else:
					i=0
					while (i!=10):	
						#print len(temp1)
						#print i
						#print "hello"
						try:
							#print "loop ke ander"
							b = json.loads(temp1[i],object_pairs_hook=OrderedDict)
							#print('json load: {}'.format(b)) 			
							#defing dataset variables
							dataset_name = 'searce_poc_vuukle'
							table_name   = 'page_impressions'
							#print "date ke pehle"
							loc = datetime.strptime(b['timestamp'],"%Y-%m-%d %H:%M:%S")
							print loc
							today=loc.strftime("%Y%m%d")
							table_name = "%s$%s"%(table_name, today)

							#print "hello1"
							#putting data into bigquery
							bigquery_client = bigquery.Client()
							dataset = bigquery_client.dataset(dataset_name)
							table = dataset.table(table_name)
							data = b
							#time_stamp1=time.strftime("%c")

							#data['timestamp'] = time_stamp1

							# Reload the table to get the schema.
							table.reload()

							## get the names of schema
							temp=list()
							for key in data:
								temp.append(data[key])
								#rows = [data]
								#print rows
							errors = table.insert_data([temp])
							if not errors:
								logging.debug('Loaded 1 row into {}:{}'.format(dataset_name, table_name))
							else:
								logging.error(errors)
						except:
							print "could not load"		
						i=i+1	
					

					# # # Acknowledge received messages. If you do not acknowledge, Pub/Sub will
					# # # redeliver the message.
				if results:
					subscription.acknowledge([ack_id for ack_id, message in results])
					#print( "one acknowledged")
			except:
				print("next")
				#continue
			counter=counter-1
			


subscriber()


