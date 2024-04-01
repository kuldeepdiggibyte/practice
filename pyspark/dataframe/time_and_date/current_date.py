from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("current_date").getOrCreate()
data = [["1","2020-02-01"],["2","2019-03-01"],["3","2021-03-01"]]
schema = ["id","date"]
df = spark.createDataFrame(data=data,schema=schema)
df.show()

#current_date()

df.select(current_date().alias("current date")).show(1)

#date_format

df.select(col("date"),
          date_format(col("date"),"dd-MM-yyyy").alias("date format")).show()

# to_date()

df.select(col("date"),
          to_date(col("date"),"yyyy-MM-dd").alias("to_date")
          ).show()

# datediff()

df.select(col("date"),
          datediff(current_date(),col("date")).alias("dateddiff")
          ).show()

#months_between

df.select(col("date"),
          months_between(current_date(),col("date")).alias("months_between")
          ).show()

