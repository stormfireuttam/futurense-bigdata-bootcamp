>>> x = sc.parallelize([("a",1),("b",4)], 4)
>>> y = sc.parallelize([("a",2),("a",3)], 2)
>>> z=x.cogroup(y)
>>> z.collect()
[('b', (<pyspark.resultiterable.ResultIterable object at 0x7f907baff760>, <pyspark.resultiterable.ResultIterable object at 0x7f907bb2c070>)), ('a', (<pyspark.resultiterable.ResultIterable object at 0x7f907bb2c460>, <pyspark.resultiterable.ResultIterable object at 0x7f907bb2c940>))]
>>> type(z)
<class 'pyspark.rdd.PipelinedRDD'>
>>> [(x, tuple(map(list, y))) for x,y in sorted(list(x.cogroup(y).collect()))]
[('a', ([1], [2, 3])), ('b', ([4], []))]
