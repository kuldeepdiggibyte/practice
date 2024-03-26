from pyspark.sql import SparkSession
from pyspark.sql import functions as f
spark = SparkSession.builder.appName("aggregate_function").getOrCreate()
emp_data = [
(1,'manish',26,20000,'india','IT'),
(2,'rahul',None,40000,'germany','engineering'),
(3,'pawan',12,60000,'india','sales'),
(4,'roshini',44,None,'uk','engineering'),
(5,'raushan',35,70000,'india','sales'),
(6,None,29,200000,'uk','IT'),
(7,'adam',37,65000,'us','IT'),
(8,'chris',16,40000,'us','sales'),
(None,None,None,None,None,None),
(7,'adam',37,65000,'us','IT')
]
emp_schema = ['id','name','age','salary','country','dept']
emp_df = spark.createDataFrame(data=emp_data,schema=emp_schema)
print(emp_df.count()) # this count returns as 10, null is included
emp_df.select(f.count("name")).show() # this returns count as 8, null is ignored

emp_df.select(f.sum("salary").alias("total"),f.max("salary").alias("maximum"),f.min("age")).show()



