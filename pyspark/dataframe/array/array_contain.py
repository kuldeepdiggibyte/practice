from pyspark.sql import SparkSession
from pyspark.sql.functions import array, array_size,size, array_contains

spark = SparkSession.builder.appName("array").getOrCreate()
data = [([1,2,3],),([4,5,6],),([7,8,9],)]
df = spark.createDataFrame(data,['arr'])

df.withColumn("contain",array_contains("arr",6)).show()



