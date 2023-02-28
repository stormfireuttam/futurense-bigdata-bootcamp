rdd = sc.parallelize([1,2,3,4],2)
rdd.getNumPartitions()                  # 2

def f(iterator) : yield sum(iterator)
  
rdd.mapPartitions(f).collect()          # [3, 7]
