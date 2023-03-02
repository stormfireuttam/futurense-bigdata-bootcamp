# Load Data and Register SQL Table
df = spark.read.options(delimiter=';', header=True, inferSchema=True).csv('/home/uttam/futurense-datengg-bootcamp/dataset/bankmarketdata.csv')
df.registerTempTable("banking")

# Success Rate
spark.sql("Select round((sum(if(y='yes',1,0))/count(*))*100, 4) as success_rate from banking").show()

# Failure Rate
spark.sql("Select round((sum(if(y='no',1,0))/count(*))*100, 4) as failure_rate from banking").show()

# Age Queries
spark.sql("Select max(age), min(age), round(mean(age),2) as avg_age from banking").show()

# Balance Queries
spark.sql("Select round(mean(age),2) as mean_age, round(percentile(age, 0.5),2) as median_age from banking").show()

# Age Grouping
spark.sql("Select case when age<13 then 'Kids' when age < 20 then 'Teenagers' when age < 30 then 'Youngsters' when age < 50 then 'Middle Agers' else 'Seniors' end as age_group, count(*) from banking where y='yes' group by age_group").show()

# Marital Status Grouping
spark.sql("Select marital, count(*) from banking where y='yes' group by marital").show()

# Age,Marital Status Grouping
spark.sql("Select case when age<13 then 'Kids' when age < 20 then 'Teenagers' when age < 30 then 'Youngsters' when age < 50 then 'Middle Agers' else 'Seniors' end as age_group,marital, count(*) from banking where y='yes' group by age_group,marital").show()
