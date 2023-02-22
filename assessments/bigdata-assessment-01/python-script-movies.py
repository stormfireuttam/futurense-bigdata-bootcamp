import csv

with open('/home/uttam/futurense-datengg-bootcamp/assessments/bigdata-assessment-01/movies.csv', 'r') as file:
    f=open("movies1.csv",'a+')
    csvreader = csv.reader(file)
    idx = 0
    output_values=[1,2,3,4]
    for line in csvreader:
        values=line
        if(idx == 0):
            idx += 1
            continue
        output_values[0]=values[0]
        if(len(values[1].split("(")) > 1):
            output_values[1]=values[1].split("(")[0]
            output_values[2]=values[1].split("(")[1][:-1]
        else:
            output_values[1]=values[1]
            output_values[2]=""
        output_values[3]=values[2]
        record=";".join(output_values)
        f.write(record)
        f.write("\n")
    f.close()

