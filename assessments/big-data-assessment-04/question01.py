from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark import SparkContext

# Load a text file and convert each line to a Row.
rdd1 = SparkContext.textFile("/labs/dataset/movies.csv")
rdd2 = rdd1.map(lambda x : x.split(';')).map(lambda x : (x[0], x[1].split("(")[0], x[2], x[1][-5:-1]))
rdd3 = rdd2.filter(lambda x : len(x) < 5).map(lambda x : [x[0],x[1],x[2],int(x[3])])


# RDD to DF
df = spark.createDataFrame(rdd3, ["id","movie_name","genres","year"])
df_with_ts = df.withColumn("timestamp", current_timestamp())

df_with_ts.show()

# Store output to HDFS
df_with_ts.write.csv("hdfs:///user/training/movie/movies1.csv")

# Convert to JSON Format and store in HDFS
df = spark.read.csv("hdfs:///user/training/movie/movies1.csv")
df.write.json("hdfs:///user/training/movie/movies.json")