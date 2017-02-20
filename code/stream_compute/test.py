import argparse
import logging
# import webapp2, json, logging, os, time, uuid, hashlib, cgi , sys
import urllib 
import json,time
from google.cloud import pubsub
from collections import OrderedDict
from google.cloud import bigquery
from datetime import date, timedelta
from google.cloud import pubsub


def subscriber():
	while True:
		counter=10
		while 1:
			pubsub_client = pubsub.Client()
			topic_name='my-new-topic'
			topic = pubsub_client.topic(topic_name)
			subscription_name='sub1'
			subscription = topic.subscription(subscription_name)
			#subscription.create()

			#for subscription in topic.list_subscriptions():
		    #	   print(subscription.name)

			#logging.debug('Subscription {} created on topic {}.'.format(
		   	#	subscription.name, topic.name))
			try:
				results = subscription.pull(return_immediately=True)

				print('Received {} messages.'.format(len(results)))

				for ack_id, message in results:
		 			#print('* {}: {}, {}'.format(
		 			#        message.message_id, message.data, message.attributes))
		 			b1=message.data
				#print results
				#print b1
				#ts = self.request.get("ts")
				try:
					b = json.loads(b1)
					#print('json load: {}'.format(b)) 
				except:
					print('Cannot able to load')
					#return
				
				#defing dataset variables
				dataset_name = 'searce_poc_vuukle'
				table_name   = 'page_impressions'
				today = date.today().strftime("%Y%m%d")
				table_name = "%s$%s"%(table_name, today)


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
			

					# # # Acknowledge received messages. If you do not acknowledge, Pub/Sub will
					# # # redeliver the message.
				if results:
					subscription.acknowledge([ack_id for ack_id, message in results])
					print( "one acknowledged")
			except:
				print("next")
				continue
			counter=counter-1
			if len(results)==0:
				break
			

subscriber()

