>>> x = sc.parallelize([("a",1), ("b", 4)])
>>> y = sc.parallelize([("a",2), ("a", 3)])
>>> x.collect()
[('a', 1), ('b', 4)]
>>> y.collect()
[('a', 2), ('a', 3)]
>>> z = x.join(y)
>>> z.collect()
[('a', (1, 2)), ('a', (1, 3))]
>>> y = sc.parallelize([("a",2), ("a",3), ("b",2)])
>>> z = x.join(y)
>>> z.collect()
[('b', (4, 2)), ('a', (1, 2)), ('a', (1, 3))]
>>> z = y.join(x)
>>> z.collect()
[('b', (2, 4)), ('a', (2, 1)), ('a', (3, 1))]
