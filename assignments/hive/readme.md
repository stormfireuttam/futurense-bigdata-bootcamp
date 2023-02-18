## Hive Assignments

Assignment #1:

	- Create "ratings" managed table
	- Load the data from ~/futurence_hadoop-pyspark/labs/dataset/movie/ratings.csv
	- Display the ratings data
	- Display rating wise count

Assignment #2:

	- Create "weather" external table under /user/training/weather
	- Load the data from ~/futurence_hadoop-pyspark/labs/dataset/weather to /user/training/weather
	- Display the weather data
	- Display Max, Min weather
	- Display month wise Max and Min weather

Assignment #3:

	- Create "customers" and "transactions" tables
	- Load the data from ~/futurence_hadoop-pyspark/labs/dataset/retail
	- Perform the analysis mentioned below

	- Ref: https://github.com/asaravanakumar/futurense_hadoop-pyspark/tree/master/labs/dataset/retail
	
Customers:
==========
cust_id		int
last_name	String
first_name	String
age		int
profession	String

Transactions:
=============
trans_id	int
trans_date	date
cust_id		int
amount		double
category	String
desc		String
city		String
state		String
pymt_mode	String


1) No of transactions by customer
2) Total transaction amount by customer
3) Get top 3 customers by transaction amount
4) No of transactions by customer and mode of payment
5) Get top 3 cities which has more transactions
6) Get month wise highest transaction
7) Get sample transactions
	

	 
	

	
