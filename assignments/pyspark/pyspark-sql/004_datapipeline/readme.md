Assignment #4

	Enhance above Bank Marketing Campaign Data Analysis PySpark Application to perform the data processing
	as pipeline of four different stages viz. Loading, Validation, Transformation and Expot each as separate PySpark applicaiton.

	Create PySpark Application - bank-marketing-data-loading.py. Perform below operations.
	a) Load Bank Marketing Campaign Data csv file from local to HDFS file system under '/user/training/bankmarketing/raw'
	b) Load Bank Marketing Campaign Data from HDFS file system under '/user/training/bankmarketing/raw' and create DataFrame
	c) Convert the data into parquet format and write into HDFS file system under '/user/training/bankmarketing/staging'
	d) Data should be moved to '/user/training/bankmarketing/raw/yyyymmdd/success' once the data loading job completed successfully
	f) Data should be moved to '/user/training/bankmarketing/raw/yyyymmdd/error' once the data loading job is failed due to data error
	
	Create PySpark Application - bank-marketing-validation.py. Perform below operations.
	a) Load Bank Marketing Campaign Data in Parquet format from HDFS file system under '/user/training/bankmarketing/staging'
	b) Remove all 'unknown' job records 
	c) Replace 'unknown' contact nos with 1234567890 and 'unknown' poutcome with 'na'
	d) Write the output as Parquet format into HDFS file system under '/user/training/bankmarketing/validated'
	e) Data should be moved to '/user/training/bankmarketing/staging/yyyymmdd/success' once the validation job completed successfully
	f) Data should be moved to '/user/training/bankmarketing/staging/yyyymmdd/error' once the validation job is failed due to data error

	Create PySpark Application - bank-marketing-tranformation.py. Perform below operations.
	a) Load validated Bank Marketing Campaign Data in Parquet format from HDFS file system under '/user/training/bankmarketing/validated'
	b) Get AgeGroup wise SubscriptionCount
	c) Filter AgeGroup with SubcriptionCount > 2000 
	d) Write the output as Avro format into HDFS file system under '/user/training/bankmarketing/processed'
	e) Data should be moved to '/user/training/bankmarketing/validated/yyyymmdd/success' once the trasnfomation job completed successfully
	f) Data should be moved to '/user/training/bankmarketing/validated/yyyymmdd/error' once the transformation job is failed
	
	Create PySpark Application - bank-marketing-export.py. Perform below operations.
	a) Load processed Bank Marketing Campaign Data in Avro format from HDFS file system under '/user/training/bankmarketing/processed'
	b) Export the data into RDBMS (MySQL DB) under bankmaketing schema and subcription_count table
	c) Data should be moved to '/user/training/bankmarketing/processed/yyyymmdd/success' once the export job completed successfully
	d) Data should be moved to '/user/training/bankmarketing/processed/yyyymmdd/error' once the export job is failed

	Create Shell Script - bank-marketing-workflow.sh. Performa below operations.
	a) Create a workflow to sequentially execute Data Loading, Validation, Tranformation and Export jobs
	b) Schedule to run this workflow every N mins e.g. 15 mins and process the new input dataset if any

