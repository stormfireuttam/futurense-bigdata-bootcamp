>>> rdd1 = sc.parallelize([1,2])
>>> rdd2 = sc.parallelize([3,4])
>>> rdd3 = rdd1.cartesian(rdd2)
>>> rdd3.collect()
[(1, 3), (1, 4), (2, 3), (2, 4)]
