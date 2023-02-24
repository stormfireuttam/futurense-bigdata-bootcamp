>>> rdd = sc.parallelize([("a",1),("b",1),("a",1)])
>>> rdd.collect()
[('a', 1), ('b', 1), ('a', 1)]
>>> rdd.reduceByKey(lambda a,b : a + b)
PythonRDD[144] at RDD at PythonRDD.scala:53
>>> rdd.reduceByKey(lambda a,b : a + b).collect()
[('a', 2), ('b', 1)]
