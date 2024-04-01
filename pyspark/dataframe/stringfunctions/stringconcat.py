from pyspark.sql import SparkSession
from pyspark.sql.functions import concat,col
spark = SparkSession.builder.appName("stringconcat").getOrCreate()

# string concat

data = [("James","","Smith","1991-04-01","M",3000),
        ("Michael","Rose","","2000-05-19","M",6000),
        ("Kuldeep","pralhad","Managoli","2000-11-17","M",100000),
        ("Bhau","Tapare","","2001-12-29", "M",120000),
        ("david","bhai","warner","1969-09-30","M",1200),
        ("sneha","","math","1947-10-20","F",130000)]

data_schema = ["firstname","middlename","lastname","dob","gender","salary"]
data_df = spark.createDataFrame(data=data,schema=data_schema)
data_df.show()
#to use concat(), select() can be used
data_df2 = data_df.select(concat(data_df.firstname,data_df.middlename,data_df.lastname)
                          .alias("Fullname"),"dob","gender","salary")
data_df2.show(truncate=False)

#concat_ws()

from pyspark.sql.functions import concat_ws,col
data_df3 = data_df.select(concat_ws(' ',data_df.firstname,data_df.middlename,data_df.lastname)
                          .alias("fullname"),"dob","gender","salary")
data_df3.show(truncate=False)


