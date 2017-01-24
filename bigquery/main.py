import webapp2, json, logging, os, time, uuid, hashlib, cgi , sys
import urllib 
from pprint import pprint
from collections import OrderedDict

from google.cloud import bigquery
from google.appengine.api import memcache, taskqueue
from datetime import date, timedelta





class MainHandler(webapp2.RequestHandler):

    def get(self):
        time1=0
        for i in range(0,50):
            timestamp=time.time()
            b1='{"power":"high","temperature":"hot"}'
            ts=str(time.time())
            b = json.loads(b1)
            logging.debug('json load: {}'.format(b))
            if len(b) > 0:
                bigquery_client = bigquery.Client()
                datasetId = os.environ['DATASET_ID']
                tableId   = os.environ['TABLE_ID']
                #today = date.today().strftime("%Y%m%d")
                #tableId = "%s$%s"%(tableId, today)
                dataset = bigquery_client.dataset(datasetId)
                table = dataset.table(tableId)
                data = json.loads(b1,object_pairs_hook=OrderedDict)
                table.reload()
                temp=list()
                for key in data:
                    temp.append(data[key])
                    rows = [data]
                #print rows
                errors = table.insert_data([temp])
                if not errors:
                    print('Loaded 1 row into {}:{}\n'.format(datasetId, tableId))
                else:
                    print('Errors:')
                    pprint(errors)
                 
            #self.response.write('row added successfully')
            timestamp1=time.time()
            time1=time1+(timestamp1-timestamp)
            #self.response.write(time1) 
        
        
        time1=time1/50
        self.response.write(time1)
        
        

		
app = webapp2.WSGIApplication([
    ('/', MainHandler),
    ], debug=True)
    