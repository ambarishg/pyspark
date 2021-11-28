from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

print("########################")
print("########################")
print("                                    ")
print("                                    ")
print("Register as a Table")
print("########################")
print("########################")
print("                                    ")

df = spark.createDataFrame([
    ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
    ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
    ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'v1', 'v2'])


df.createOrReplaceTempView("tableA")
spark.sql("SELECT count(*) from tableA").show()

print("########################")
print("########################")
print("                                    ")
print("                                    ")
print("UDFs can be registered and invoked in SQL out of the box")
print("########################")
print("########################")
print("                                    ")


from pyspark.sql.functions import pandas_udf

@pandas_udf("integer")
def add_one(s: pd.Series) -> pd.Series:
    return s + 1

spark.udf.register("add_one", add_one)
spark.sql("SELECT add_one(v1) FROM tableA").show()

print("########################")
print("########################")
print("                                    ")
print("                                    ")
print("Expressions")
print("########################")
print("########################")
print("                                    ")

from pyspark.sql.functions import expr

df.selectExpr('add_one(v1)').show()
df.select(expr('count(*)') > 0).show()
