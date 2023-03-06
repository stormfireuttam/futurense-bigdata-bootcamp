df = spark.read.options(delimiter=';', header=True, inferSchema=True).csv('/home/uttam/futurense-datengg-bootcamp/dataset/bankmarketdata.csv')

