# bank-marketing-export.py. Perform below operations.
import subprocess
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[1]") \
                    .appName('pyspark-examples') \
                    .getOrCreate()
#	a) Load processed Bank Marketing Campaign Data in Avro format from HDFS file system under '/user/training/bankmarketing/processed'
df = spark.read.load("hdfs://localhost:9000/user/training/bankmarketing/processed", format = "avro")
#	b) Export the data into RDBMS (MySQL DB) under bankmaketing schema and subcription_count table
df.write \
.format("jdbc") \
.option("url", "jdbc:mysql://localhost:3306/bankmarketing") \
.option("driver", "com.mysql.jdbc.Driver") \
.option("dbtable", "subscription_count") \
.option("user", "pyspark") \
.option("password", "pyspark") \
.mode("overwrite") \
.save()
#	c) Data should be moved to '/user/training/bankmarketing/processed/yyyymmdd/success' once the export job completed successfully
#	d) Data should be moved to '/user/training/bankmarketing/processed/yyyymmdd/error' once the export job is failed
try:
 df.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/processed/yyyymmdd/success')
except:
 df.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/processed/yyyymmdd/failure')
