rdd1 = sc.textFile("/home/uttam/futurense-datengg-bootcamp/dataset/bankmarketing.txt")
header = rdd1.first()
rdd2 = rdd1.filter(lambda x : x != header).filter(lambda x : len(x) > 1)
rdd3 = rdd2.map(lambda x : x.split(';'))
no_subscribed = rdd3.filter(lambda x : x[16] == 'yes').count()
total_count = rdd3.count()
success_rate = (no_subscribed / total_count) * 100
failure_rate = 100 - success_rate
ages = rdd3.map(lambda x : int(x[0]))
max_age = ages.max()
min_age = ages.min()
avg_age = ages.mean()
