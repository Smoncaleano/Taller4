from google.cloud.bigtable import row_filters

from co.edu.unbosque.connectionbd.ConnectionBD import *
from csv import reader

table = createTable()
"""
datetime_cfId = 'DATE_TIME'
datetime_cf = table.column_family(datetime_cfId)
datetime_cf.create()

plant_cfId = 'PLANT_ID'
plant_cf = table.column_family(plant_cfId)
plant_cf.create()

sourcekey_cfId = 'SOURCE_KEY'
sourcekey_cf = table.column_family(sourcekey_cfId)
sourcekey_cf.create()

dcpower_cfId = 'DC_POWER'
dcpower_cf = table.column_family(dcpower_cfId)
dcpower_cf.create()

acpower_cfId = 'AC_POWER'
acpower_cf = table.column_family(acpower_cfId)
acpower_cf.create()

dailyYield_cfId = 'DAILY_YIELD'
dailyYield_cf = table.column_family(dailyYield_cfId)
dailyYield_cf.create()

totalyield_cfId = 'TOTAL_YIELD'
totalyield_cf = table.column_family(totalyield_cfId)
totalyield_cf.create()

dt = dt.utcnow()
rows = []
listado = []

with open('../connectionbd/Plant_1_Generation_Data.csv', 'r') as csv_file:
    csv_reader = reader(csv_file)
    # Passing the cav_reader object to list() to get a list of lists
    listado = list(csv_reader)

print('Writing orders to the table')


for i in range(1200, 1260):
    dateTime = listado[i][0]
    plantId = listado[i][1]
    sourceKey = listado[i][2]
    dcPower = listado[i][3]
    acPower = listado[i][4]
    dailyYield = listado[i][5]
    totalYield = listado[i][6]
    row_key = 'plant#{}#{}'.format(sourceKey, dateTime).encode()
    row = table.direct_row(row_key)
    row.set_cell(datetime_cfId, 'DATE_TIME'.encode(), dateTime, timestamp=dt)
    row.set_cell(plant_cfId, 'PLANT_ID'.encode(), plantId, timestamp=dt)
    row.set_cell(sourcekey_cfId, 'SOURCE_KEY'.encode(), sourceKey, timestamp=dt)
    row.set_cell(dcpower_cfId, 'DC_POWER'.encode(), dcPower, timestamp=dt)
    row.set_cell(acpower_cfId, 'AC_POWER'.encode(), acPower, timestamp=dt)
    row.set_cell(dailyYield_cfId, 'DAILY_YIELD'.encode(), dailyYield, timestamp=dt)
    row.set_cell(totalyield_cfId, 'TOTAL_YIELD'.encode(), totalYield, timestamp=dt)
    rows.append(row)

table.mutate_rows(rows)

"""
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


key = 'plant#adLQvlD726eNBSB#15-05-2020 14:15'.encode()
row = table.read_row(key)
print_row(row)