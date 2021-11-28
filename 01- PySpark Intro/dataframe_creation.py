from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row
print("                                    ")
print("                                    ")
print("Data Frame Creation WITHOUT A Schema")
print("########################")
print("########################")
print("                                    ")
df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])
print(df)


print("########################")
print("########################")
print("                                    ")
print("                                    ")
print("Data Frame Creation WITH A Schema")
print("########################")
print("########################")
print("                                    ")


df = spark.createDataFrame([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
], schema='a long, b double, c string, d date, e timestamp')
print(df)


print("########################")
print("########################")
print("                                    ")
print("                                    ")
print("Data Frame Creation WITH RDD")
print("########################")
print("########################")
print("                                    ")

rdd = spark.sparkContext.parallelize([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
])

df = spark.createDataFrame(rdd, schema=['a', 'b', 'c', 'd', 'e'])
df.show()
