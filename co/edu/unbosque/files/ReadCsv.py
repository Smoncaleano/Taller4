import pandas as pd
from io import StringIO
from csv import reader

with open('../connectionbd/Plant_1_Generation_Data.csv', 'r') as csv_file:
    csv_reader = reader(csv_file)
    # Passing the cav_reader object to list() to get a list of lists
    lista = list(csv_reader)

    for i in range(1, 10):
        dateTime = lista[i][0]
        plantId = lista[i][1]
        sourceKey = lista[i][2]
        dcPower = lista[i][3]
        acPower = lista[i][4]
        dailyYield = lista[i][5]
        totalYield = lista[i][6]
        print(dateTime + " " + plantId)









