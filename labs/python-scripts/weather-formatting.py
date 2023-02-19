with open('/home/uttam/futurense-datengg-bootcamp/dataset/weather_data.txt', 'r') as file:
    f=open("weather_data_formatted.txt",'a+')
    for line in file:
        line=line.replace(" C","C")
        values=line.split()
        values[1]=values[1][0:4]+"-"+values[1][4:6]+"-"+values[1][6:]
        record="#".join(values)
        f.write(record)
        f.write("\n")
    f.close()
