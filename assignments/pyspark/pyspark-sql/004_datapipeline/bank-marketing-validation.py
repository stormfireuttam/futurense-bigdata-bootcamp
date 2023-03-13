# Bank-Marketing-Validation
import subprocess
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[1]") \
                    .appName('pyspark-examples') \
                    .getOrCreate()
# a) Load Bank Marketing Campaign Data in Parquet format from HDFS file system under '/user/training/bankmarketing/staging'
df1 = spark.read.load("hdfs://localhost:9000/user/training/bankmarketing/staging",format = "parquet")
# b) Remove all 'unknown' job records 
df1 = df1.filter(df1.job != 'unknown')
# c) Replace 'unknown' contact nos with 1234567890 and 'unknown' poutcome with 'na'
df1.withColumn('contact', regexp_replace('contact', 'unknown', '123467890')) \
  .withColumn('poutcome', regexp_replace('poutcome', 'unknown', 'na'))
  .show(truncate=False)
# d) Write the output as Parquet format into HDFS file system under '/user/training/bankmarketing/validated'
df1.write.mode('overwrite').format('parquet').save("hdfs://localhost:9000/user/training/bankmarketing/validated")

# e) Data should be moved to '/user/training/bankmarketing/staging/yyyymmdd/success' once the validation job completed successfully
try:
    df3.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/staging/yyyymmdd/success')
# f) Data should be moved to '/user/training/bankmarketing/staging/yyyymmdd/error' once the validation job is failed due to data error
except:
    df3.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/staging/yyyymmdd/failure')

