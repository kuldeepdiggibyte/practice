from pyspark.sql import SparkSession
from pyspark.sql.functions import array_size
spark = SparkSession.builder.appName("array_length").getOrCreate()
data = [([1,2,3],),([4,5],),([],)]
df = spark.createDataFrame(data,['arr'])
df.withColumn("arr_length",array_size('arr')).show()