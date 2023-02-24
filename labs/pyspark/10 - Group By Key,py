>>> rdd = sc.parallelize([("a",1),("b",1),("a",1)])
>>> rdd.collect()
[('a', 1), ('b', 1), ('a', 1)]
>>> rdd.groupByKey().collect()
[('a', <pyspark.resultiterable.ResultIterable object at 0x7f907bafcdc0>), ('b', <pyspark.resultiterable.ResultIterable object at 0x7f90801ef400>)]
>>> rdd.groupByKey().mapValues(len)
PythonRDD[128] at RDD at PythonRDD.scala:53
>>> rdd.groupByKey().mapValues(len).collect()
[('a', 2), ('b', 1)]
>>> rdd.groupByKey().mapValues(list).collect()
[('a', [1, 1]), ('b', [1])]
