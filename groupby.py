from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

df = spark.createDataFrame([
    ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
    ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
    ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'v1', 'v2'])

print("                                    ")
print("                                    ")
print("Data Frame ")
print("########################")

print("                                    ")  
df.show()

print("                                    ")
print("                                    ")
print("Data Frame Group By Color and then Avg of all Variables")
print("########################")

print("                                    ")  
df.groupby('color').avg().show()

