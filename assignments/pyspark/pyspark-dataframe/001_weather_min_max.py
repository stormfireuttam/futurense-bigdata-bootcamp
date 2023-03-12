# Weather Data Analysis with DataFrame API


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# a) Load Weather Dataset and create DataFrame
spark = SparkSession.builder.master('local').appName('Weather Analaysis DataFrame').getOrCreate()
rdd=spark.sparkContext.textFile('/home/uttam/futurense-datengg-bootcamp/dataset/weather_data.txt')
df = rdd.map(lambda x : x.split()).toDF()

# b) Show Min and Max Temperature
df = df.select(col("_6").alias("max_temp"), col("_7").alias("min_temp"))
df.select(max(df.max_temp.cast('int')), min(df.min_temp.cast('double'))).show()

# c) Show month wise Min and Max Temperature
df = rdd.map(lambda x : x.split()).toDF()
df = df.select(col("_2").alias("time"), col("_6").alias("max_temp"), col("_7").alias("min_temp"))
df.show()
df=df.withColumn("month", substring("time",5,2))
df.groupBy("month").agg(max('max_temp').alias('max_temp_monthwise'), min('min_temp').alias('min_temp_monthwise')).show()
