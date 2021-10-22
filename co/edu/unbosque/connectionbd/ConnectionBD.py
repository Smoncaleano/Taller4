import time
from datetime import datetime as dt
from google.cloud import bigtable
from google.cloud.bigtable import column_family

"""Método que crea la tabla que se usa para insertar los registros de PLant_Generation"""
def createTable():
    INSTANCE_ID = 'table-1'
    TABLE_ID = '124500'

    """Se carga el json con el service account de la base de datos"""
    client = bigtable.Client.from_service_account_json('unbosque-service-account.json', admin=True)
    instance = client.instance(INSTANCE_ID)

    table = instance.table(TABLE_ID)
    """Si la tabla no existe, se crea"""
    if not table.exists():
        table.create()
        print('Se creo la tabla')
    else:
        print("ERROR: Table {} already exists".format(TABLE_ID))

    return table

"""Método que crea la tabla que se usa para insertar los registros de PLant_Weather_Sensor"""

def createTable2():
    INSTANCE_ID = 'table-1'
    TABLE_ID = 'atomPunto2'

    """Se carga el json con el service account de la base de datos"""
    client = bigtable.Client.from_service_account_json('unbosque-service-account.json', admin=True)
    instance = client.instance(INSTANCE_ID)

    table = instance.table(TABLE_ID)
    """Si la tabla no existe, se crea"""
    if not table.exists():
        table.create()
        print('Se creo la tabla')
    else:
        print("ERROR: Table {} already exists".format(TABLE_ID))

    return table