import subprocess
from pkg_resources._vendor.pyparsing import col
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StringType, StructField, StructType
from pyspark.sql.functions import *


#Create Spark Session
spark = SparkSession \
    .builder \
    .appName("MovieAnalysis") \
    .getOrCreate()


topic_name = "movie-events"
bootstrap_servers = "localhost:9092"

create_topic_cmd = f"/opt/kafka/bin/kafka-topics.sh --create --bootstrap-server {bootstrap_servers} --replication-factor 1 --partitions 1 --topic {topic_name} --if-not-exists"
subprocess.run(create_topic_cmd.split(), check=True)

bootstrap_servers = "localhost:9092"
topic_name = "bank-marketing-events"
csv_file_path = "/mnt/c/Users/miles/Documents/exam/movies1.csv"

producer_cmd = f"/opt/kafka/bin/kafka-console-producer.sh --broker-list {bootstrap_servers} --topic {topic_name}"

# Read contents of CSV file into a string
with open(csv_file_path, "r") as f:
    csv_contents = f.read()

# Encode string as bytes and pass as input to the producer process
subprocess.Popen(producer_cmd.split(), stdin=subprocess.PIPE).communicate(input=csv_contents.encode("utf-8"))

#Consume message from Kafka topic and create Dataset
from pyspark.sql.functions import from_csv, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

movie_schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("training_id", IntegerType()),
    StructField("movie", StructType([
        StructField("title", StringType()),
        StructField("genres", ArrayType(StringType()))
    ]))
])


moviesDF = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "movie-events") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .load() \
    .select(col("value").cast("string"))


moviescsvDF = moviesDF.select(from_csv(col("value"), movie_schema, options={"delimiter": ";"}).alias("movies"))

moviescsvDF.printSchema()

#Schema definition for consuming message

moviescsvDF.createOrReplaceTempView("movies")


# Print the genre-wise count for every batch
movie_df.writeStream.outputMode("complete").format("console").start()


# Set the window duration to 5 minutes and the slide duration to 1 minute
window_duration = "300 seconds"
slide_duration = "60 seconds"

# Handle late-arriving movie events with a 10-minute threshold
movie_df = movies\
    .withWatermark("id", "10 minutes")\
    .groupBy(window("id", window_duration, slide_duration), "genre")\
    .agg({"genre": "count"})\
    .orderBy(col("count(genre)").desc())

df.printSchema()

topic_name = "movie-stream-analysis"
bootstrap_servers = "localhost:9092"

create_topic_cmd_2 = f"/opt/kafka/bin/kafka-topics.sh --create --bootstrap-server {bootstrap_servers} --replication-factor 1 --partitions 1 --topic {topic_name} --if-not-exists"

subprocess.run(create_topic_cmd_2.split(), check=True)


query = df\
    .writeStream \
    .outputMode("complete") \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("topic","movie-stream-analysis") \
    .option("checkpointLocation", "movies") \
    .start()

query.awaitTermination()