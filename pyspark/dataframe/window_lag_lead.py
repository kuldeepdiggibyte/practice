from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as f
spark = SparkSession.builder.appName("window_lag_lead").getOrCreate()

product_data = [
(1,"iphone","01-01-2023",1500000),
(2,"samsung","01-01-2023",1100000),
(3,"oneplus","01-01-2023",1100000),
(1,"iphone","01-02-2023",1300000),
(2,"samsung","01-02-2023",1120000),
(3,"oneplus","01-02-2023",1120000),
(1,"iphone","01-03-2023",1600000),
(2,"samsung","01-03-2023",1080000),
(3,"oneplus","01-03-2023",1160000),
(1,"iphone","01-04-2023",1700000),
(2,"samsung","01-04-2023",1800000),
(3,"oneplus","01-04-2023",1170000),
(1,"iphone","01-05-2023",1200000),
(2,"samsung","01-05-2023",980000),
(3,"oneplus","01-05-2023",1175000),
(1,"iphone","01-06-2023",1100000),
(2,"samsung","01-06-2023",1100000),
(3,"oneplus","01-06-2023",1200000)
]

product_schema = ("product_id","product_name","date","sales")
product_df = spark.createDataFrame(data=product_data,schema=product_schema)
product_df.show()
window = Window.partitionBy("product_id").orderBy("date")
last_month_df = product_df.withColumn("previous_month_sales",f.lead(f.col("sales"),1,100000).over(window))

last_month_df.withColumn("per_loss_gain",(f.col("sales")-f.col("previous_month_sales"))/f.col("sales")*100).show()

