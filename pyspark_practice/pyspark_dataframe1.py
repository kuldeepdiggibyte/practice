from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrame").getOrCreate()
## read dataset
# 1st way of creating dataframe
df_spark = spark.read.option('header','true').csv('expirence.csv').show()
df_spark1 = spark.read.option('header','true').csv('expirence.csv',inferSchema=True)
df_spark1.printSchema() # it shows column name
#2nd way of creating dataframe
df_spark1 = spark.read.csv('expirence.csv',header=True,inferSchema=True)
#show top 3 from a file
#print(df_spark1.head(3))
#df_spark1.show()
#select particular column
#df_spark1.select('Name').show()
df_spark1.describe().show()
## adding columns in data frame
df_spark1.withColumn('Experience After 2 year', df_spark1['Experience']+2)
new_df_spark1= df_spark1.withColumn('Experience After 2 year', df_spark1['Experience']+2).show()
new_df_spark1.withColumn('Age after 2 years',new_df_spark1['Age']+2).show()
