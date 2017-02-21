import webapp2, json, logging, os, time, uuid, hashlib, cgi , sys
import urllib 
from google.cloud import pubsub
from collections import OrderedDict
from google.cloud import bigquery
from google.appengine.api import memcache, taskqueue
from datetime import date, timedelta
from google.appengine.api import urlfetch



pubsub_client = pubsub.Client()
topic_name = 'my-new-topic'
topic = pubsub_client.topic(topic_name)

#fuction to test the query parameters i.e just for testing
def sync_query(query):
    client = bigquery.Client()
    query_results = client.run_sync_query(query)

    
    query_results.use_legacy_sql = False
    query_results.use_query_cache = False

    query_results.run()

    # Drain the query results by requesting a page at a time.
    page_token = None
    bqdata = []

    while True:
        rows, total_rows, page_token = query_results.fetch_data(
            max_results=10,
            page_token=page_token)

        bqdata.extend(rows)

        for row in rows:
            logging.debug(row)

        if not page_token:
            break

    return bqdata

''' function to actually stream the json payload into the 
	biqquery table to by using the global function insert_all'''
def stream_data(dataset_name, table_name, json_data, ip,time_stamp = time.strftime("%c")):
    bigquery_client = bigquery.Client()
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)
    data = json_data
    time_stamp1=time.strftime("%c")

    data['timestamp'] = time_stamp1
    data['Ipaddress']=ip

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


''' This handler is called when the given request is /bqstreamer 
	Uses taskqueue to call another handler as post request'''
class MainHandler(webapp2.RequestHandler):
    def get(self):
        self.response.headers.add_header("Access-Control-Allow-Origin", "*")
        s=self.request.url
        s=urllib.unquote(s).decode('utf8')
        #self.response.write(s)
        
        start = s.index('&') + len('&')
        end = s.index('&_', start )
        b1=s[start:end]
        #bq=parse.unquotes(b1)
        b1=b1.replace("'",'"')
        ip=self.request.remote_addr                    
        
        task = taskqueue.add(url='/bq-task', params={'bq': b1, 'ts': str(time.time()), 'Ip':ip})
        
        



    def post(self):
        self.response.headers.add_header("Access-Control-Allow-Origin", "*")

        
        s=self.request.url
        s=urllib.unquote(s).decode('utf8')
        #self.response.write(s)
        try:
            start = s.index('&') + len('&')
            end = s.index('&_', start )
            b1=s[start:end]
        #bq=parse.unquotes(b1)
            b1=b1.replace("'",'"')
        except:
            return
        
        
        #ts=str(time.time())
        #b = json.loads(b1)
        #b.replace("'",'"')
        #self.response.write(b)
        task = taskqueue.add(url='/bq-task', params={'bq': b1, 'ts': str(time.time())})


        
'''Handler gets post reequest from the MainHandler and call stream
   data to insert the data into the biqquery''' 
class BqHandler(webapp2.RequestHandler):
    def post(self):

        ## get example.com/bq-task?bq=blah
        b1 = self.request.get("bq")
        ts = self.request.get("ts")
        ip= self.request.get("Ip")
        try:
            b = json.loads(b1,object_pairs_hook=OrderedDict)
            logging.debug('json load: {}'.format(b)) 
        except:
            logging.debug('Cannot able to load')
            return    
        

        if len(b) > 0:
            datasetId = os.environ['DATASET_ID']
            tableId   = os.environ['TABLE_ID']

            today = date.today().strftime("%Y%m%d")

            tableId = "%s$%s"%(tableId, today)

            stream_data(datasetId, tableId, b,ip, ts)



class PublishHandler(webapp2.RequestHandler):
    urlfetch.set_default_fetch_deadline(15)
    def get(self):
        #flag=1
        self.response.headers.add_header("Access-Control-Allow-Origin", "*")
        s=self.request.url
        s=urllib.unquote(s).decode('utf8')
        #self.response.write(s)

        
        start = s.index('&') + len('&')
        end = s.index('&_', start )
        b1=s[start:end]
        #bq=parse.unquotes(b1)
        b1=b1.replace("'",'"')
        #b1=json.dumps(b1)
        b=json.loads(b1,object_pairs_hook=OrderedDict,strict=False)
        timestamp=time.strftime("%Y-%m-%d %H:%M:%S")
        b['timestamp']=timestamp
        Ip= self.request.remote_addr
        b['Ipaddress']=Ip
        a= json.dumps(b)
        c=str(a)
        logging.debug('json decoded')
            
        #logging.debug(b1)                            
        #for topic in pubsub_client.list_topics():
        #    if topic.name=='my-new-topic':
        #        flag=0        

        #topic.create()

        # if flag==0:                                        
        #     topic.create()
        #     print('Topic {} created.'.format(topic.name))
        #     b1 = b1.encode('utf-8')
        #     message_id = topic.publish(b1)
        #     logging.debug('Message {} published.'.format(message_id))        
        
        
        # else:            
        #     # Data must be a bytestring
        c = c.encode('utf-8')

        message_id = topic.publish(c)

        logging.debug('Message {} published outside loop.'.format(message_id))


    def post(self):
        self.response.headers.add_header("Access-Control-Allow-Origin", "*")
        pubsub_client = pubsub.Client()
        #flag=1
        s=self.request.url
        s=urllib.unquote(s).decode('utf8')
        #self.response.write(s)
        
        start = s.index('&') + len('&')
        end = s.index('&_', start )
        b1=s[start:end]
        #bq=parse.unquotes(b1)
        b1=b1.replace("'",'"')
        #b1=json.dumps(b1)
        b=json.loads(b1,object_pairs_hook=OrderedDict,strict=False)
        timestamp=time.strftime("%Y-%m-%d %H:%M:%S")
        b['PAGE_VIEW_TIMESTAMP']=timestamp
        a= json.dumps(b)
        c=str(a)
        
        
        #for topic in pubsub_client.list_topics():
        #    if topic.name=='my-new-topic':
        #        flag=0        
        topic_name = 'my-new-topic'
        topic = pubsub_client.topic(topic_name)
        # if flag==0:                
        #     topic.create()
        #     #self.response.write('Topic {} created.'.format(topic.name))
        #     b1 = b1.encode('utf-8')
        #     message_id = topic.publish(b1)
        #     #self.response.write('Message {} published.'.format(message_id))        
        
        
        # else:            
        #     # Data must be a bytestring
        c = c.encode('utf-8')

        message_id = topic.publish(c)

        logging.debug('Message {} published.'.format(message_id))        
        


    		
app = webapp2.WSGIApplication([
    ('/bq-streamer', MainHandler),
    ('/bq-task', BqHandler),
    ('/bq-publish', PublishHandler),
], debug=True)
