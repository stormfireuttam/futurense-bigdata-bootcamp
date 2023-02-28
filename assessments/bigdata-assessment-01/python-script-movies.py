import csv

with open('/home/uttam/futurense-datengg-bootcamp/assessments/bigdata-assessment-01/movies.csv', 'r') as file:
    f=open("movies2.csv",'a+')
    csvreader = csv.reader(file)
    idx = 0
    #output= [1,2,3,4]
    for line in csvreader:
        values=line
        if(idx == 0):
            idx += 1
            continue
        
        values.append( str(values[1])[-5:-1] )
        values[1] = values[1].split("(")[0]
        record=";".join(values)
        f.write(record)
        f.write("\n")
    f.close()

