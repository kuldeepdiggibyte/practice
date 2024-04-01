from pyspark.sql import SparkSession
from pyspark.sql.functions import split

spark = SparkSession.builder.appName("split").getOrCreate()
data = [("kuldeep,managoli",),("kartik, managoli",),("pralhad,managoli",)]
data_schema = ["fullname"]
df = spark.createDataFrame(data=data,schema=data_schema)

split_column = split(df["fullname"],",")

df_with_split = df.withColumn("first_name",split_column[0]).withColumn("last_name",split_column[1])
df_with_split.show()