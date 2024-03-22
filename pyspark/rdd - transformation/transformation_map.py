

from pyspark import SparkContext
sc = SparkContext.getOrCreate()
#MAP tranformation
x_map = sc.parallelize([2,3,4,5,6,7,8])
df1 = x_map.map(lambda x:x*2)
print(df1.collect())

#MAP PARTITION
data = [("Kuldeep",23),("kartik",25),("pralhad",60)]
rdd_mapp = sc.parallelize(data,2)
result_rdd = rdd_mapp.mapPartitions(lambda iterator: (f"{name} is {age} years old"for name,age in iterator))
result = result_rdd.collect()
print(result)


#MAP PARTITION with INDEX
result_rdd_partition = rdd_mapp.mapPartitionsWithIndex(lambda index,iterator:(f"{name} is {age} years old in partition {index}"for name,age in iterator))
result_2 = result_rdd_partition.collect()
result_3 = result_rdd_partition.glom().collect()

print(f"without glom {result_3}".format(result_2))
print(f"with glom {result_3}".format(result_3))

sc.stop()
