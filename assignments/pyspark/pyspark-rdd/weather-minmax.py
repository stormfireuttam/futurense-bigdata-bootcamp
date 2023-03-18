import pyspark
from pyspark import SparkContext

sc = SparkContext("local", "Map app")
rdd1 = sc.textFile("/home/uttam/futurense_hadoop-pyspark/labs/dataset/weather/weather_data.txt")
rdd2 = rdd1.map(lambda x : x.split())
max_temp = rdd2.map(lambda x : float(x[5])).collect()
min_temp = rdd2.map(lambda x : float(x[6])).collect()
max = max_temp[0]
min = min_temp[0]
for i in range(len(max_temp)):
  if max_temp[i] > max:
    max = max_temp[i]
  if min_temp[i] < min:
    min = min_temp[i]
print(f'The maximum and minimum temperatures are : {max} and {min} ')
