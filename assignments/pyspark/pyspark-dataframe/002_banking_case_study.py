# Bank Marketing Campaign Data Analysis with DataFrame API

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

sc = SparkSession.builder.master('local').getOrCreate()

# a) Load Bank Marketing Dataset and create DataFrame	
df=sc.read.format('csv')\
    .option('header','True')\
    .option('delimiter',';')\
    .load('/home/uttam/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv')
df.show(5)

# b) Give marketing success rate. (No. of people subscribed / total no. of entries)
success = float(df.filter(df.y =='yes').count() / df.count()*100)

# c) Give marketing failure rate
failure = 100 - success

# d) Maximum, Mean, and Minimum age of the average targeted customer
df.select(max("age"), min("age"), round(mean("age"),2)).show()

# e) Check the quality of customers by checking the average balance, median balance of customers
df.select(round(mean("balance"),2).alias("avg_balance"), round(percentile_approx(age, 0.5),2).alias("median_balance")).show()

# f) Check if age matters in marketing subscription for deposit
df.filter(df.y == 'yes').groupBy("age").count().sort('count', ascending=False).show()

# g) Show AgeGroup [Teenagers, Youngsters, MiddleAgers, Seniors] wise Subscription Count
age_cat_dict = {20:'Teen',40:'Young',60:'Middle',80:'Seniors',100:'Seniors'}
age_cat_udf = udf(lambda age: age_cat_dict[age],StringType())
df_age_cat = df.select('age','y').filter(df.y == 'yes').withColumn('age_cat', age_cat_udf(ceil(df['age']/20) * 20)).groupBy('age_cat').count()

# h) Check if marital status mattered for subscription to deposit.
df.filter(df.y == 'yes').groupBy("marital").count().sort('count', ascending=False).show()


# i) Check if age and marital status together mattered for subscription to deposit scheme
df.filter(df.y == 'yes').groupBy("age","marital").count().sort('count', ascending=False).show()

