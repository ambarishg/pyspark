from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])

print("Show the rows vertically")
print("########################")

print("                                    ")

df.show(1, vertical=True)

print("########################")

print("                                    ")

print("Show the columns")
print("########################")
print(df.columns)

print("########################")

print("                                    ")

print("Print the schema")
print("########################")
df.printSchema()

print("                                    ")

print("Select the columns and also describe")
print("########################")
df.select("a", "b", "c").describe().show()

print("                                    ")
print("########################")

print("                                    ")
print("Collect and Show")
print("########################")

print(df.collect())
print("                                    ")
print("########################")


print("                                    ")
print("Show a single Row")
print("########################")
print(df.take(1))
print("                                    ")
print("########################")

print("                                    ")


print("Convert to Pandas and Show")
print("########################")
print(df.toPandas())

print("########################")

print("                                    ")
print("Assign a NEW Column")
print("########################")
print("                                    ")
from pyspark.sql import Column
from pyspark.sql.functions import upper

df.withColumn('upper_c', upper(df.c)).show()

print("########################")

print("                                    ")
print("Filter on a  Column")
print("########################")
print("                                    ")
df.filter(df.a == 1).show()