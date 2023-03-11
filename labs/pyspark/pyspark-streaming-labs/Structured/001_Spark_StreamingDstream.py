### Spark Streaming Example with DStream
#Step 1: Open cloud labs terminal and start netcat utility
#           nc -lk 9999

#Step 2: Start Spark Shell and Run below Spark Streaming program

# Command to execute this script
# spark-submit <relative_path>.py localhost 9999 



from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

# This lines DStream represents the stream of data that will be received from the data server. 
# Each record in this DStream is a line of text. Next, we want to split the lines by space into words.

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# flatMap is a one-to-many DStream operation that creates a new DStream by generating multiple new records 
# from each record in the source DStream. In this case, each line will be split into multiple words and 
# the stream of words is represented as the words DStream. 
# Next, we want to count these words.

# Count each word in each batch
pairs = words.map(lambda x : (x, 1))
wordCount = pairs.reduceByKey(lambda a,b: a + b)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCount.pprint()

# Note that when these lines are executed, Spark Streaming only sets up the computation it will perform when it is started,
# and no real processing has started yet. To start the processing after all the transformations have been setup, 
# we finally call
ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate