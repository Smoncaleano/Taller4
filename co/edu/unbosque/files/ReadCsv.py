import pandas as pd
from io import StringIO

df = pd.read_csv('C:\\Users\\Janfep2020\Desktop\\archive\\Plant_1_Generation_Data.csv', usecols=[0])
print(df)