from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("actions").getOrCreate()
#count
count_rdd = spark.sparkContext.parallelize([1,2,3,4,5])
count = count_rdd.count()
print("Count:",count)

#min
rdd_min = spark.sparkContext.parallelize([5,6,7,4,5,3])
min_value = rdd_min.min()
print("Minimun : ",min_value)

#Max
rdd_max = spark.sparkContext.parallelize([1,2,4,85,234,4])
max_value = rdd_max.max()
print("Maximun:",max_value)

#mean
rdd_mean = spark.sparkContext.parallelize([1,2,3,53,67,3456,2366,232])
mean_value = rdd_mean.mean()
print("Mean:",mean_value)

#countByKey
rdd_countby = spark.sparkContext.parallelize([('a',1),('b',2),('a',3),('b',4)])
count_by_key = rdd_countby.countByKey()
print("Count by key:",count_by_key)

#countByvalue - provides output with individual head count

rdd_count = spark.sparkContext.parallelize([1,2,1,3,2,1])
count_by_value = rdd_count.countByValue()
print(count_by_value)

#fold
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
sum_of_elements = rdd.fold(0, lambda x, y: x + y)
print("Sum of elements:", sum_of_elements)



