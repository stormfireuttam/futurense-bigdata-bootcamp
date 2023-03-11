from pyspark.sql.functions import *

# Create DataFrame representing the stream of input lines from connection to host:port
lines = spark\
        .readStream\
        .format('socket')\
        .option('host', 'localhost')\
        .option('port', 9999)\
        .option('includeTimestamp', 'true')\
        .load()

lines.printSchema()
# root
#  |-- value: string (nullable = true)
#  |-- timestamp: timestamp (nullable = true)
  
lines.isStreaming
# True

# Split the lines into words, retaining timestamps
# split() splits each line into an array, and explode() turns the array into multiple rows
words = lines.select(
    explode(split(lines.value, ' ')).alias('word'),
    lines.timestamp
)

windowDuration = '30 seconds'
slideDuration = '10 seconds'
# Group the data by window and word and compute the count of each group
windowedCounts = words.groupBy(
    window(words.timestamp, windowDuration, slideDuration),
    words.word
).count().orderBy('window')

windowedCounts.printSchema()
# root
#  |-- window: struct (nullable = true)
#  |    |-- start: timestamp (nullable = true)
#  |    |-- end: timestamp (nullable = true)
#  |-- word: string (nullable = false)
#  |-- count: long (nullable = false)
  
# Start running the query that prints the windowed word counts to the console
query = windowedCounts\
    .writeStream\
    .outputMode('complete')\
    .format('console') \
    .option("checkpointLocation", "checkpoint") \
    .option('truncate', 'false')\
    .start()
