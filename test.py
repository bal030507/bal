from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType
import yaml
import sys; 
import load 
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

import functools

def verify_null(field, df:DataFrame):
        return df.filter(col(field).isNull())
 
# explicit function
def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)
 
 
file_path = ["C:\\Users\\636371\\Downloads\\json\\mrn.json", "C:\\Users\\636371\\Downloads\\json\\mrn1.json", "C:\\Users\\636371\\Downloads\\json\\mrn2.json"]
df1 = spark.read.json(file_path)
df3 = verify_null("name", df1)

df2 = spark.createDataFrame([], df1.schema)
unioned_df = unionAll([df3, df2])
unioned_df.show()

           
      