import webapp2, json, logging, os, time, uuid, hashlib, cgi , sys
import urllib 
from collections import OrderedDict
from google.cloud import bigquery
from google.appengine.api import memcache, taskqueue
from datetime import date, timedelta


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



def stream_data(dataset_name, table_name, json_data, time_stamp = time.strftime("%c")):
    bigquery_client = bigquery.Client()
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)
    data = json_data
    time_stamp1=time.strftime("%c")

    data['timestamp'] = time_stamp1

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
        
        task = taskqueue.add(url='/bq-task', params={'bq': b1, 'ts': str(time.time())})
        
        



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

class BqHandler(webapp2.RequestHandler):
    def post(self):

        ## get example.com/bq-task?bq=blah
        b1 = self.request.get("bq")
        ts = self.request.get("ts")
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

            stream_data(datasetId, tableId, b, ts)

    		
app = webapp2.WSGIApplication([
    ('/bq-streamer', MainHandler),
    ('/bq-task', BqHandler),
], debug=True)
