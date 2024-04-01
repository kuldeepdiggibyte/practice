from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, substring

spark = SparkSession.builder.appName("substring").getOrCreate()


data1 = [("kuldeep",), ("kartik",), ("Pralhad",)]
data_schema1 = ["name"]
data_df1 = spark.createDataFrame(data=data1, schema=data_schema1)


df_with_substr = data_df1.withColumn("example", expr("substr(name, 1, 3)"))
df_with_substr.show()

# Using substr() to get the substring of a column
data_df1 = data_df1.withColumn("example", expr("substr(name, 1, 3)"))
data_df1.show()


data2 = [(1, "20200828"),
         (2, "20180525"),
         (3, "20210525"),
         (4, "20230623")]
data_schema2 = ["id", "date"]
data_df2 = spark.createDataFrame(data=data2, schema=data_schema2)

# Using substr() to get the year, month, and day columns for the second example
data_df2 = data_df2.withColumn("year", substring("date", 1, 4)) \
    .withColumn("month", substring("date", 5, 2)) \
    .withColumn("day", substring("date", 7, 2))
data_df2.show()
