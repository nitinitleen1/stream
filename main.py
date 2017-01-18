import webapp2, json, logging, os, time, uuid, hashlib, cgi

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



def stream_data(dataset_name, table_name, json_data, time_stamp = time.time()):
    bigquery_client = bigquery.Client()
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)
    data = json_data

    data['ts'] = time_stamp

    # Reload the table to get the schema.
    table.reload()

    ## get the names of schema
    schema = table.schema
    schema_names = [o.name for o in schema]

    logging.debug('BQ Schema: {}'.format(schema_names))

    
    rows = [(data[x] for x in schema_names)]

    
    errors = table.insert_data(rows, row_ids = str(uuid.uuid4()))

    if not errors:
    	logging.debug('Loaded 1 row into {}:{}'.format(dataset_name, table_name))
    else:
        logging.error(errors)

class MainHandler(webapp2.RequestHandler):
    def get(self):
        self.response.headers.add_header("Access-Control-Allow-Origin", "*")
        s='http://345.34.5.34.5/insert?callback=jQuery21402542612166112883_1484128195262&{"hashed_article_url":"indianexpress.com/elections/punjab-assembly-elections-2017/kejriwal-rubbishes-rumours-says-cm-of-punjab-will-be-from-punjab-4469232/","hostname":"2e5a47ef-15f6-4eec-a685-65a6d0ed00d0","pubdomain":"indianexpress.com","article_id":"4469232","hashed_email":"$485ebce-af23-4d88-9163-5fa86b7227ca","tags":"elections,%20punjab-assembly-elections-2017","action":"view_page","os":"Linux","device":"Desktop","browser":"Chrome","refDomain":"indianexpresscom","articleTitle":"Kejriwal%20rubbishes%20rumours,%20says%20CM%20of%20Punjab%20will%20be%20from%20Punjab","articleImg":"http%3A//images.indianexpress.com/2017/01/kejriwal_4801.jpg%3Fw%3D100","referrer":"name=Online%20IE","sessionId":"3ec60a44-bd22-4040-bf78-1d719f6ed73e","version":"4.20"}&_=1484128195263'
        start = s.index('&') + len('&')
        end = s.index('&', start )
        bq=s[start:end]
        #print form_fields
        ## get example.com?bq=blah
        
        ts=str(time.time())
        b = json.loads(bq)
        logging.debug('json load: {}'.format(b))

        if len(b) > 0:
            datasetId = os.environ['DATASET_ID']
            tableId   = os.environ['TABLE_ID']

            today = date.today().strftime("%Y%m%d")

            tableId = "%s$%s"%(tableId, today)

            stream_data(datasetId, tableId, b, ts)



		
		
		
app = webapp2.WSGIApplication([
    ('/', MainHandler),
    ], debug=True)
    