from pyspark.sql import SparkSession
from pyspark.sql.functions import array

spark  = SparkSession.builder.appName("array").getOrCreate()
data = [(1,'a'),(2,'b'),(3,'c')]
df = spark.createDataFrame(data,['id','value'])
df.withColumn("array_col",array('id','value')).show()