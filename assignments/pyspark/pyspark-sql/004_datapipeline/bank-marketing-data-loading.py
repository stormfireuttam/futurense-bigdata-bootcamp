import subprocess
import pyspark    
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[1]") \
                      .appName('pyspark-examples') \
                      .getOrCreate()

# a) Load Bank Marketing Campaign Data csv file from local to HDFS file system under '/user/training/bankmarketing/raw'       
subprocess.run(["echo","Loading data from local system to HDFS"])
subprocess.run(["hadoop", "fs", "-put",  "/home/uttam/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv", "hdfs://localhost:9000/user/training/bankmarketing/raw"])
subprocess.run(["echo","Data Loaded Successfully to HDFS"])

# b) Load Bank Marketing Campaign Data from HDFS file system under '/user/training/bankmarketing/raw' and create DataFrame      
df = spark.read.load("hdfs://localhost:9000/user/training/bankmarketing/raw/bankmarketdata.csv",format = "csv", sep = ";", delimiter=';',header=True,inferSchema=True)

# c) Convert the data into parquet format and write into HDFS file system under '/user/training/bankmarketing/staging'
df.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/staging')

# d) Data should be moved to '/user/training/bankmarketing/raw/yyyymmdd/success' once the data loading job completed successfully        
# e) Data should be moved to '/user/training/bankmarketing/raw/yyyymmdd/error' once the data loading job is failed due to data error
 try:
        subprocess.run(["hadoop", "fs", "-put" ,"/home/uttam/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv", "hdfs://localhost:9000/user/training/bankmarketing/raw/yyyymmdd/success"])
  except:
        subprocess.run(["hadoop", "fs", "-put" ,"/home/uttam/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv" ,"hdfs://localhost:9000/user/training/bankmarketing/raw/yyyymmdd/error"])

