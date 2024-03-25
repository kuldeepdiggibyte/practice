from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("newsession").getOrCreate()
flight_df = spark.read.format("csv")\
    .option("header","false")\
    .option("inferschema","false")\
    .option("mode","FAILFAST")\
    .load("2010-summary.csv")
#flight_df.show()

'''flight_df_header = spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","FAILFAST")\
    .load("2010-summary.csv")
#flight_df_header.show()
flight_df_header.printSchema()'''


