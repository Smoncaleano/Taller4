from google.cloud.bigtable import row_filters
from google.cloud.bigtable.row_set import RowSet

from co.edu.unbosque.connectionbd.ConnectionBD import *
from csv import reader

"""Se crea un objeto tabla, y su valor es asignado con el método createTable2 que pertenece a nuestra clase connectionBD """
table = createTable2()

"""Se crean las columnas de familias en nuestra tabla"""
datetime_cfId = 'DATE_TIME'
datetime_cf = table.column_family(datetime_cfId)
datetime_cf.create()

plant_cfId = 'PLANT_ID'
plant_cf = table.column_family(plant_cfId)
plant_cf.create()

sourcekey_cfId = 'SOURCE_KEY'
sourcekey_cf = table.column_family(sourcekey_cfId)
sourcekey_cf.create()

ambientTemperature_cfId = 'AMBIENT_TEMPERATURE'
ambientTemperature_cf = table.column_family(ambientTemperature_cfId)
ambientTemperature_cf.create()

moduleTemperature_cfId = 'MODULE_TEMPERATURE'
moduleTemperature_cf = table.column_family(moduleTemperature_cfId)
moduleTemperature_cf.create()

irradiation_cfId = 'IRRADIATION'
irradiation_cf = table.column_family(irradiation_cfId)
irradiation_cf.create()


dt = dt.utcnow()
rows = []
listado = []

"""Se lee el csv respectivo para el punto 1, y se añade línea a línea a un listado"""
with open('../connectionbd/Plant_2_Weather_Sensor_Data.csv', 'r') as csv_file:
    csv_reader = reader(csv_file)
    # Passing the cav_reader object to list() to get a list of lists
    listado = list(csv_reader)

print('Writing orders to the table')

"""Se itera ese listado desde la posición 1650, hasta la 1682, para subir más rápido los registros a la tabla. En ese rango más o menos se encuentra 
    nuestra respuesta para este requerimiento"""
for i in range(1650, 1682):
    dateTime = listado[i][0]
    plantId = listado[i][1]
    sourceKey = listado[i][2]
    ambientTemperature = listado[i][3]
    moduleTemperature = listado[i][4]
    irradiation = listado[i][5]
    """Se crea el apuntador row_key"""
    row_key = 'plant#{}#{}'.format(plantId, dateTime).encode()
    row = table.direct_row(row_key)
    """Se setean los valores de las celdas"""
    row.set_cell(datetime_cfId, 'DATE_TIME'.encode(), dateTime, timestamp=dt)
    row.set_cell(plant_cfId, 'PLANT_ID'.encode(), plantId, timestamp=dt)
    row.set_cell(sourcekey_cfId, 'SOURCE_KEY'.encode(), sourceKey, timestamp=dt)
    row.set_cell(ambientTemperature_cfId, 'DC_POWER'.encode(), ambientTemperature, timestamp=dt)
    row.set_cell(moduleTemperature_cfId, 'AC_POWER'.encode(), moduleTemperature, timestamp=dt)
    row.set_cell(irradiation_cfId, 'DAILY_YIELD'.encode(), irradiation, timestamp=dt)
    rows.append(row)
"""Se insertan los datos en la tabla"""
table.mutate_rows(rows)


"""Método que imprime nuestras columnas"""
def print_row(row):
    print("Reading data for {}:".format(row.row_key.decode('utf-8')))
    for cf, cols in sorted(row.cells.items()):
        print("Column Family {}".format(cf))
        for col, cells in sorted(cols.items()):
            for cell in cells:
                labels = " [{}]".format(",".join(cell.labels)) \
                    if len(cell.labels) else ""
                print(
                    "\t{}: {} @{}{}".format(col.decode('utf-8'),
                                            cell.value.decode('utf-8'),
                                            cell.timestamp, labels))
    print("")

"""Se hace uso de rowSet para obtener la respuesta entre 2 rangos"""
row_set = RowSet()
row_set.add_row_range_from_keys(
    start_key=b"plant#4136001#2020-06-01 06:00:00",
    end_key=b"plant#4136001#2020-06-01 12:00:00")
"""Se construye nuestro query, que además se filtra para que sólo nos muestre la familia de columnas llamada AMBIENT_TEMPERATURE"""
rows = table.read_rows(row_set=row_set, filter_=row_filters.FamilyNameRegexFilter("AMBIENT_TEMPERATURE.*$".encode("utf-8")))

"""Se imprime para probar su funcionamiento"""
for row in rows:
    print_row(row)
