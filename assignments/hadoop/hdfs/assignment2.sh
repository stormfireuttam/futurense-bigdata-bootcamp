#	Create directory "movie" under /user/training
hdfs dfs -mkdir /user/training/movie
#	Load the data from ~/futurence_hadoop-pyspark/labs/dataset/movie/ratings.csv to /user/training/movie/ratings.csv
hdfs dfs -put ~/futurense_hadoop-pyspark/labs/dataset/movie/ratings.csv /user/training/movie/ratings.csv
#	Display the movie data
hdfs dfs -cat /user/training/movie/ratings.csv


