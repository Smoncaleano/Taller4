import time
from datetime import datetime as dt
from google.cloud import bigtable
from google.cloud.bigtable import column_family

def createTable():
    INSTANCE_ID = 'table-1'
    TABLE_ID = '124500'

    client = bigtable.Client.from_service_account_json('unbosque-service-account.json', admin=True)
    instance = client.instance(INSTANCE_ID)

    table = instance.table(TABLE_ID)

    if not table.exists():
        table.create()
        print('Se creo la tabla')
    else:
        print("ERROR: Table {} already exists".format(TABLE_ID))

    return table