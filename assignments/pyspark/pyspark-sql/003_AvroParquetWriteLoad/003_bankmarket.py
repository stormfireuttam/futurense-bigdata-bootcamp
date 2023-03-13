import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[1]") \
                    .appName('pyspark-examples') \
                    .getOrCreate()

# a) Load Bank Marketing Campaign Data from csv file
data  = spark.read.load("/home/uttam/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv",format = "csv", sep = ";", delimiter=';',header=True,inferSchema=True)
data.createOrReplaceTempView("Banking")

# b) Get AgeGroup wise SubscriptionCount
result = spark.sql("SELECT count(*) as count, case when age < 13 then 'Kids' \
		   when (age >= 13) and (age <= 19) then 'Teenagers' \
           when (age > 19) and (age <= 30) then 'Youngsters' \
           when (age > 30) and (age < 50) then 'MiddleAgers' \
           else 'Seniors' END as age_group \
           FROM Banking where (y is not Null and y = 'yes')  \
           group by (age_group)")


# c) Write the output in parquet file format
result.write.format('parquet').save("/home/uttam/futurense-datengg-bootcamp/PySpark/fileFormat/parquet/results_csv")

# d) Load the data from parquet file written above
resultant_data  = spark.read.load("/home/uttam/futurense-datengg-bootcamp/PySpark/fileFormat/parquet/results_csv",format = "parquet",header=True,inferSchema=True)

# e) Show the data
print(resultant_data.show())

# f) Filter AgeGroup with SubcriptionCount > 2000 and write into Avro file format
resultant_data.createOrReplaceTempView("Banking2")
result = spark.sql("Select * from Banking2 where count > 2000")
print(result.show())

result.write.format('avro').save("/home/uttam/futurense-datengg-bootcamp/PySpark/fileFormat/avro/results2_csv")

# g) Load the data from avro file written above
resultant_data = spark.read.load("/home/uttam/futurense-datengg-bootcamp/PySpark/fileFormat/avro/results2_csv",format = "avro",header=True,inferSchema=True)

# h) Show the data
print(resultant_data.show())

'''
	1] spark-submit bank-marketing-analysis.py => Runs in local mode
    spark-submit --packages org.apache.spark:spark-avro_2.12:3.3.2  003_bankmarket.py
	2] Run in cluster mode
    --Set Master = spark://MILES-BL-3968-LAP.localdomain:7077
    spark-submit --packages org.apache.spark:spark-avro_2.12:3.3.2 003_bankmarket.py --master spark://MILE-BL-3968-LAP.localdomain:7077
	3] Schedule to PySparkApplication to run every N mins

'''
