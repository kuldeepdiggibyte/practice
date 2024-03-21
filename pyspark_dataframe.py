import pyspark
import pandas as pd
read = pd.read_csv('expirence.csv')
print(type(read))
print(read)
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('practice').getOrCreate()
df_pyspark = spark.read.csv('expirence.csv')
print(type(spark))
spark.read.option('header','true').csv('expirence.csv').show()
