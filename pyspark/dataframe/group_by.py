from pyspark.sql import SparkSession
from pyspark.sql import functions as f
spark = SparkSession.builder.appName("group_by").getOrCreate()
emp_data = [(1,'manish',50000,'IT','India'),
(2,'vikash',60000,'sales','us'),
(3,'raushan',70000,'marketing','India'),
(4,'mukesh',80000,'IT','us'),
(5,'pritam',90000,'sales','India'),
(6,'nikita',45000,'marketing','us'),
(7,'ragini',55000,'marketing','India'),
(8,'rakesh',100000,'IT','us'),
(9,'aditya',65000,'IT','India'),
(10,'rahul',50000,'marketing','us')]

emp_schema = ["id","name","salary","dept","country"]
emp_df = spark.createDataFrame(data=emp_data,schema=emp_schema)
#emp_df.show()
emp_df.groupby("dept","country")\
    .agg(f.sum("salary").alias("salary")).show()
