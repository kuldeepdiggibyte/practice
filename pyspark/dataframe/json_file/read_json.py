from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("json_read").getOrCreate()
df = spark.read.json("../../resource/zipcodes.json")
df.cache()
df.printSchema()
df.show()
