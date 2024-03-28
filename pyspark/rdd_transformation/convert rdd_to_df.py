from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("to_df").getOrCreate()
dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
rdd = spark.sparkContext.parallelize(dept)
print(rdd.collect())

# create DF
deptcolumn = ["dept_name","dept_id"]
df = rdd.toDF(deptcolumn)
df.printSchema()
df.show()

# create Dataframe() as function
#dept_df = spark.createDataFrame(rdd,schema=deptcolumn, inferSchema = True)
dept_df = spark.createDataFrame(rdd, schema=deptcolumn)

dept_df.printSchema()
dept_df.show()