>>> rdd1 = sc.parallelize([1,2,3,4,5])
>>> rdd2 = sc.parallelize([6,7,8,9])
>>> rdd3 = rdd1.union(rdd2)
>>> rdd3.collect()
[1, 2, 3, 4, 5, 6, 7, 8, 9]
>>> rdd2 = sc.parallelize([5,6,7,8,9])
>>> rdd3 = rdd1.union(rdd2)             # Narrow Transformation
>>> rdd3.collect()
[1, 2, 3, 4, 5, 5, 6, 7, 8, 9]
>>> rdd3.distinct().collect()           # Wide Transformation
[1, 2, 3, 4, 5, 6, 7, 8, 9]
>>> rdd3.getNumPartitions()
16
>>> rdd1.getNumPartitions()
8
>>> rdd4 = rdd1.intersection(rdd2)      # Wide Transformation
>>> rdd4.collect()
[5]
>>> rdd1.getNumPartitions()
8
>>> rdd2.getNumPartitions()
8
>>> rdd3.getNumPartitions()
16
>>> rdd4.getNumPartitions()
16
