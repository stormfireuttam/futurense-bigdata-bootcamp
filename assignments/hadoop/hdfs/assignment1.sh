#Assignment #1:
#   Create directory "weather" under /user/training
hdfs dfs -mkdir /user/training/weather
#   Load the data from ~/futurence_hadoop-pyspark/labs/dataset/weather to /user/training/weather
hdfs dfs -put ~/futurence_hadoop-pyspark/labs/dataset/weather/weather_data.txt /user/training/weather
#   Display the weather data
hdfs dfs -cat /user/training/weather
#   Split the weather data file and store as weather1 and wearther2
lines=`wc -l /user/training/weather/weather_data.txt`
line=`echo $lines | cut -d' ' -f1`
h1=`expr $line / 2`
h2=`expr $line - $h1`
hdfs dfs -cat /user/training/weather/weather_data.txt | head -$h1 > weather1.txt
hdfs dfs -cat /user/training/weather/weather_data.txt | tail -$h2 > weather2.txt
#   Merge the weather data file as weather3
cat weather1.txt weather2.txt > weather3.txt
#   Display the weather stats
cat weather3.txt
