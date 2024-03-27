from pyspark.sql import SparkSession
from pyspark.sql import functions as f
spark = SparkSession.builder.appName("window").getOrCreate()

emp_data = [(1,'manish',50000,'IT','m'),
(2,'vikash',60000,'sales','m'),
(3,'raushan',70000,'marketing','m'),
(4,'mukesh',80000,'IT','m'),
(5,'priti',90000,'sales','f'),
(6,'nikita',45000,'marketing','f'),
(7,'ragini',55000,'marketing','f'),
(8,'rashi',100000,'IT','f'),
(9,'aditya',65000,'IT','m'),
(10,'rahul',50000,'marketing','m'),
(11,'rakhi',50000,'IT','f'),
(12,'akhilesh',90000,'sales','m')]

emp_schema = ["id","name","salary","dept","sex"]
emp_df = spark.createDataFrame(data=emp_data,schema=emp_schema)

print("--- using groupby---")
emp_df.groupby("dept")\
    .agg(f.sum("salary")).show()

from pyspark.sql.window import Window
window = Window.partitionBy("dept")

emp_df.withColumn("total_sal",f.sum(f.col("salary")).over(window))\
    .show(truncate=False)

# row number

window = window.partitionBy("dept","gender").orderBy("salary")

(emp_df.withColumn("row_number",f.row_number().over(window)) \
    .withColumn("Rank",f.Rank().over(window))\
    .withColumn("Dense_Rank",f.dense_rank().over(window)).show(truncate=False))




