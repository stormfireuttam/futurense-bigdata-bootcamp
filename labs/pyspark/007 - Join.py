>>> x = sc.parallelize([("a",1), ("b", 4)])
>>> y = sc.parallelize([("a",2), ("a", 3)])
>>> x.collect()
[('a', 1), ('b', 4)]
>>> y.collect()
[('a', 2), ('a', 3)]
>>> z = x.join(y)
>>> z.collect()
[('a', (1, 2)), ('a', (1, 3))]
