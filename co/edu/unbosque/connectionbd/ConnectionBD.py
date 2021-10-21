import time
from datetime import datetime as dt
from google.cloud import bigtable
from google.cloud.bigtable import column_family

INSTANCE_ID = 'table-1'
TABLE_ID = 'orders_{}'.format(time.time())

client = bigtable.Client.from_service_account_json('../../unbosque-service-account.json', admin = True)
instance = client.instance(INSTANCE_ID)