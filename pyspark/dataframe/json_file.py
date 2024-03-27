from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("JSON example") \
    .getOrCreate()


df = spark.read.json("file1.json")


df.printSchema()


df.show()



spark.stop()
