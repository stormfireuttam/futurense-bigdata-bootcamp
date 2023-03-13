# Bank Marketing Processing
import subprocess
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = Spark.builder.master("local[1]").appName("pyspark-example").getOrCreate()
#   a) Load validated Bank Marketing Campaign Data in Parquet format from HDFS file system under '/user/training/bankmarketing/validated'
df1 = spark.read.load("hdfs://localhost:9000/user/training/bankmarketing/validated",format = "parquet")
df1.createOrReplaceTempView("Banking_Filter")
#	b) Get AgeGroup wise SubscriptionCount
age_group = spark.sql("Select case when age<13 then 'Kids' when age < 20 then 'Teenagers' when age < 30 then 'Youngsters' when age < 50 then 'Middle Agers' else 'Seniors' end as age_group, count(*) from banking where y='yes' group by age_group").show()
#	c) Filter AgeGroup with SubcriptionCount > 2000 
age_group_count = spark.sql("SELECT age, count(y) from Banking_Filter group by age having count(y) > 2000").show()
#	d) Write the output as Avro format into HDFS file system under '/user/training/bankmarketing/processed'
age_group_count.write.mode('overwrite').format('avro').save("hdfs://localhost:9000/user/training/bankmarketing/validated")
#	e) Data should be moved to '/user/training/bankmarketing/validated/yyyymmdd/success' once the trasnfomation job completed successfully
#	f) Data should be moved to '/user/training/bankmarketing/validated/yyyymmdd/error' once the transformation job is failed
try:
 age_group_count.write.mode('overwrite').format('avro').save('hdfs://localhost:9000/user/training/bankmarketing/processed/yyyymmdd/success')
except:
 age_group_count.write.mode('overwrite').format('avro').save('hdfs://localhost:9000/user/training/bankmarketing/processed/yyyymmdd/failure')

