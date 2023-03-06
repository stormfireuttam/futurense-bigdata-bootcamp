from pyspark import SparkContext

sc = SparkContext("local", "Wordcount app")

lines = sc.textFile("/home/uttam/futurense_hadoop-pyspark/labs/dataset/wordcount/wordcount-input.txt")
rdd1 = lines.flatMap(lambda x: x.split(' ')) 
rdd2 = rdd1.map(lambda x: (x, 1)) 
rdd3 = rdd2.reduceByKey(lambda a, b: a + b)
output = rdd3.sortBy(lambda x: x[1], ascending=False).take(10)

for (word, count) in output:
        print("%s: %i" % (word, count))