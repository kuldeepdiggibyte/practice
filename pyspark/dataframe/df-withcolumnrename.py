from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("rename").getOrCreate()
data = [(1,"kuldeep",23),(2,"kartik",25),(3,"pralhad",60)]
column = ['id','name','age']
df1 = spark.createDataFrame(data=data,schema=column)
df1.show()
df2 = df1.withColumnRenamed('id','no.')
df2.show()