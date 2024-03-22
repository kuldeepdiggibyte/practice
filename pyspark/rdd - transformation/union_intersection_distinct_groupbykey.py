from pyspark import SparkContext
sc = SparkContext.getOrCreate()

# UNION - it joins two rdd into one

rdd1 = sc.parallelize([1,2,3])
rdd2 = sc.parallelize([4,5,6])
rdd3 = rdd1.union(rdd2)
#print(rdd3.collect())

# INTERSECTION - it takes common values in both created rdds
rdd_inter = sc.parallelize([1,2,3,4,5])
rdd_inter1 = sc.parallelize([3,4,5,6,7])
rdd_inter2 = rdd_inter.intersection(rdd_inter1)
#print(rdd_inter2.collect())

# DISTINCT = it will return only once, repeated values are discarded
rdd = sc.parallelize([1,2,3,3,3,3,3,3,4,5,6,7])
distinct_rdd = rdd.distinct()
#print(distinct_rdd.collect())

# groupby results into key-value pairs
rdd_groupby = sc.parallelize([('Kuldeep','rasmalai'),("kartik","mango"),("pralhad","khava_holige")])
grouped_rdd = rdd_groupby.groupByKey().collect()
for key,values in grouped_rdd:
    print(f"Key : {key},Values : {list(values)}")
sc.stop()