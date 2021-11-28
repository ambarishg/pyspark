from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

df = spark.createDataFrame([
    ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
    ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
    ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'v1', 'v2'])
df.show()

def plus_mean(pandas_df):
    return pandas_df.assign(v1=pandas_df.v1 - pandas_df.v1.mean())

# GroupedData.applyInPandas(func, schema)
# Maps each group of the current DataFrame using a pandas udf 
# and returns the result as a DataFrame.

# The function should take a pandas.DataFrame 
# and return another pandas.DataFrame. 
# For each group, all columns are passed together as a pandas.
# DataFrame to the user-function and the returned pandas.
# DataFrame are combined as a DataFrame.

df.groupby('color').applyInPandas(plus_mean, schema=df.schema).show()