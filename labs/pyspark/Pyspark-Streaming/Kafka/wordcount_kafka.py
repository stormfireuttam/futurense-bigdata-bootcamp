
uttam@MILE-BL-3968-LAP:/opt/kafka$ pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2

      
from pyspark.sql.functions import *
# lines = spark\
#         .readStream\
#         .format("kafka")\
#         .option("kafka.bootstrap.servers", bootstrapServers)\
#         .option(subscribeType, topics)\
#         .load()\
#         .selectExpr("CAST(value AS STRING)")
lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", 'localhost:9092')\
        .option('subscribe', 'wordcount')\
        .load()\
        .selectExpr("CAST(value AS STRING)")

>>> lines.printSchema()
root
 |-- value: string (nullable = true)

# Split the lines into words
words = lines.select(
    # explode turns each item in an array into a separate row
    explode(
        split(lines.value, ' ')
    ).alias('word')
)

# Generate running word count
wordCounts = words.groupBy('word').count()

 # Start running the query that prints the running counts to the console
query = wordCounts\
    .writeStream\
    .outputMode('complete')\
    .format('console')\
    .start()
