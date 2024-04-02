
from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import expr
spark = SparkSession.builder.appName('Pivot').getOrCreate()


data = [
        ("Neha", "Maths", 85),
        ("Neha", "Physics", 90),
        ("Ananya", "Maths", 70),
        ("Ananya", "Physics", 80),
        ("Ananya", "Chemistry", 75),
        ("Rahul", "Maths", 60 ),
        ("Rahul", "Chemistry" ,67),
        ("Rahul", "Physics" , 87)
        ]

schema = StructType([
    StructField("Name", StringType()),
    StructField("Subject", StringType()),
    StructField("Score", IntegerType())
])

df = spark.createDataFrame(data=data , schema=schema)
pivot_df =  df.groupBy("Name").pivot("Subject").agg(expr("first(Score)"))
pivot_df.show()
