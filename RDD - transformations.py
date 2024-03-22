from pyspark import SparkContext
sc = SparkContext.getOrCreate()
print("SparkContext",sc)
#MAP
x_map = sc.parallelize([1,2,3,4,5,6,7,8])
y_map = x_map.map(lambda x: (x,x**2))
print('values x_map: {0}'.format(x_map.collect()))
print('values y_map: {0}'.format(y_map.collect()))

#FILTER

x_add = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
y_filter = x_add.filter(lambda x : x%2==1)
print('values x_add: {0}'.format(x_add.collect()))
print('Values y_filter: {0}'.format(y_filter.collect()))

#FLAT_MAP
x_flatMap_rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
y_flatMap = x_flatMap_rdd.flatMap(lambda x:(x,x**2,100*x))
print('values x_flatMap_rdd {0}'.format(x_flatMap_rdd.collect()))
print('values of y_flatmap: {0}'.format(y_flatMap.collect()))

#MAP_PARTITION
x = sc.parallelize([1,2,3,4,5,6,7,8,9,10],4)
def f(iterator): yield  sum(iterator)
y = x.mapPartitions(f)
#glom() flatten element on the same partition
print(x.collect())
print(y.collect())
print(x.glom().collect())
print(y.glom().collect())
