from pyspark.sql import SparkSession
from  pyspark.sql.functions import lpad,rpad

spark = SparkSession.builder.appName("stringpadding").getOrCreate()
data = [("Kuldeep",),("kartik",),("pralhad",)]
data_schema = ["name"]
df = spark.createDataFrame(data=data,schema=data_schema)

#left padding (lpad)
df_with_left_padding = df.withColumn("left_padded_name",lpad(df["name"],10,"0"))

#right padding (rpad)
df_with_right_padding = df.withColumn("right_padded_name",rpad(df["name"],10,"X"))
df_with_left_padding.show()
df_with_right_padding.show()
