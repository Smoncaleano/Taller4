from co.edu.unbosque.connectionbd.ConnectionBD import *

table = createTable()

datetime_cfId = 'DATE_TIME'
#datetime_cf = table.column_family(datetime_cfId)
#datetime_cf.create()

plant_cfId = 'PLANT_ID'
#plant_cf = table.column_family(plant_cfId)
#plant_cf.create()

sourcekey_cfId = 'SOURCE_KEY'
#sourcekey_cf = table.column_family(sourcekey_cfId)
#sourcekey_cf.create()

dcpower_cfId = 'DC_POWER'
#dcpower_cf = table.column_family(dcpower_cfId)
#dcpower_cf.create()

acpower_cfId = 'AC_POWER'
#acpower_cf = table.column_family(acpower_cfId)
#acpower_cf.create()

dailyYield_cfId = 'DAILY_YIELD'
#dailyYield_cf = table.column_family(dailyYield_cfId)
#dailyYield_cf.create()

totalyield_cfId = 'TOTAL_YIELD'
#totalyield_cf = table.column_family(totalyield_cfId)
#totalyield_cf.create()

orders = [
    {
        'DATE_TIME': '2021/10/21',
        'PLANT_ID': '002',
        'SOURCE_KEY': 'asdasd',
        'DC_POWER': '20.4',
        'AC_POWER': '10.5',
        'DAILY_YIELD': '1.0',
        'TOTAL_YIELD': '300'
    }
]

dt = dt.utcnow()
rows = []

print('Writing orders to the table')
for order in orders:

    row_key = 'order#{}'.format(order['PLANT_ID']).encode()
    row = table.direct_row(row_key)

    row.set_cell(datetime_cfId, 'DATE_TIME'.encode(), order['DATE_TIME'], timestamp=dt)
    row.set_cell(plant_cfId, 'PLANT_ID'.encode(), order['PLANT_ID'], timestamp=dt)
    row.set_cell(sourcekey_cfId, 'SOURCE_KEY'.encode(), order['SOURCE_KEY'], timestamp=dt)
    row.set_cell(dcpower_cfId, 'DC_POWER'.encode(), order['DC_POWER'], timestamp=dt)
    row.set_cell(acpower_cfId, 'AC_POWER'.encode(), order['AC_POWER'], timestamp=dt)
    row.set_cell(dailyYield_cfId, 'DAILY_YIELD'.encode(), order['DAILY_YIELD'], timestamp=dt)
    row.set_cell(totalyield_cfId, 'TOTAL_YIELD'.encode(), order['TOTAL_YIELD'], timestamp=dt)

    rows.append(row)

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

table.mutate_rows(rows)

key = 'order#001'.encode()

row = table.read_row(key)
print_row(row)