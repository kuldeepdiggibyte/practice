
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("rename1").getOrCreate()
data = [(1,"Kuldeep",23),(2,"kartik",25),(3,"pralhad",60)]
column = ['id','name','age']
df = spark.createDataFrame(data=data,schema=column)
df.show()

# Dataframe are immutable, so it create another column
df1= df.withColumnRenamed('id','no.')
df1.show()

# lets try to manipulate age by adding 2
df3 = df.withColumn('age after 2 years',df['age']+2)
df3.show()


