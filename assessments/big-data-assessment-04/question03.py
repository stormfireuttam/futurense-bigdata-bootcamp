from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, window, explode, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Define the schema for the movie.json dataset
movie_schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("training_id", IntegerType()),
    StructField("movie", StructType([
        StructField("id", IntegerType()),
        StructField("title", StringType()),
        StructField("genres", ArrayType(StringType())),
        StructField("year", IntegerType()),
        StructField("timestampe", StringType())
    ]))    
])

# Create a SparkSession
spark = SparkSession.builder.appName("MovieStream").getOrCreate()

# Create a StreamingContext with a batch interval of 60 seconds
ssc = StreamingContext(spark.sparkContext, 60)

# create a DStream from the dataframe
movies_stream = ssc \
    .readStream \
    .format("json") \
    .option("path", "/user/training/movie/movie.json") \
    .load()

# Convert the JSON data to a DataFrame using the defined schema
movie_df = movie_stream\
    .select(from_json(col("value"), movie_schema).alias("movie"))\
    .select("movie.user_id", "movie.training_id", "movie.movie.title", explode("movie.movie.genres").alias("genre"))

# Print the genre-wise count for every batch
movie_df.writeStream.outputMode("complete").format("console").start()

# Set the window duration to 5 minutes and the slide duration to 1 minute
window_duration = "300 seconds"
slide_duration = "60 seconds"

# Handle late-arriving movie events with a 10-minute threshold
movie_df = movie_df\
    .withWatermark("training_id", "10 minutes")\
    .groupBy(window("training_id", window_duration, slide_duration), "genre")\
    .agg({"genre": "count"})\
    .orderBy(col("count(genre)").desc())


# Start the streaming context
ssc.start()
ssc.awaitTermination()
