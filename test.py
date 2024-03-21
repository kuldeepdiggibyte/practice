from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
datalist1 = [("Kuldeep", 19400),("Krishna",21343),("pratibha",1230),("Manvi",150)]
firstrdd = spark.sparkContext.parallelize(datalist1)
firstrdd.toDF().show()