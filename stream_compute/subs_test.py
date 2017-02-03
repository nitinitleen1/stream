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
	topic_name='my-new-topic'
	subscription_name='sub1'		
    pubsub_client = pubsub.Client()
    topic = pubsub_client.topic(topic_name)
    subscription = topic.subscription(subscription_name)

    # Change return_immediately=False to block until messages are
    # received.
    results = subscription.pull(return_immediately=True)

    print('Received {} messages.'.format(len(results)))

    for ack_id, message in results:
        print('* {}: {}, {}'.format(
            message.message_id, message.data, message.attributes))

    # Acknowledge received messages. If you do not acknowledge, Pub/Sub will
    # redeliver the message.
    if results:
        subscription.acknowledge([ack_id for ack_id, message in results])

subscriber()