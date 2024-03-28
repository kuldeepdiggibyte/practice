from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("distinct example").getOrCreate()
data=[
    ("Kuldeep", "vijayapura", "22"),
    ("kartik", "mangaluru","25"),
    ("Ishita","nagpur","29"),
    ("vipul","Bangalore","22")
]
columns= ["Name","City", "Age"]
df= spark.createDataFrame(data= data, schema= columns)


distinctdf= df.distinct()
distinctdf.show()