from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType
spark = SparkSession.builder.appName("multi_line").getOrCreate()
multiline_df = spark.read.option("multiline","true")\
    .json("../../resource/sample_json_for_practice.json")
multiline_df.show()

#reading multiple files at a time

schema = StructType([
    StructField("countries",ArrayType(
        StructType([
            StructField("name",StringType(), nullable=True),
            StructField("code", StringType(),nullable=True),
            StructField("rank" , IntegerType(), nullable=True)
        ])
    ),nullable=True)
])
df_with_schema = spark.read.schema(schema=schema)\
    .json("../../resource/sample_json_for_practice.json")
df_with_schema.printSchema()
df_with_schema.show()