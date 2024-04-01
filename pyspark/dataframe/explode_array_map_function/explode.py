from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

spark = SparkSession.builder.appName("explode").getOrCreate()

data = [
    ("kuldeep", 22, ["apple", "banana", "orange"], "New York", "Male"),
    ("kartik", 26, ["grape", "pear"], "Los Angeles", "Male"),
    ("ram", 35, ["watermelon", "kiwi", "mango"], "Chicago", "Male"),
    ("ragvendra", 40, ["pineapple", "strawberry"], "Houston", "Male"),
    ("pooja", 46, ["blueberry", "raspberry", "blackberry"], "San Francisco", "Female")
]
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Fruits", ArrayType(StringType()), True),
    StructField("Location", StringType(), True),
    StructField("Gender", StringType(), True)
])
df = spark.createDataFrame(data=data , schema=schema)

exploded_df = df.select("Name", "Age", explode("Fruits").alias("Fruit"), "Location", "Gender")
exploded_df.show()