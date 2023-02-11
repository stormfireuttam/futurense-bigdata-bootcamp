#!/bin/bash

#Create Directory if not exists
mkdir ~/Desktop
#Extract testData.tar to location
tar -xf testData.tar -C ~/Desktop
#Check if file exists or not
cd ~/Desktop
file="data.txt"
if [ ! -f "$file" ]
then
    echo "File '${file}' not found."
    echo "Creating File"
    cp /etc/passwd ~/Desktop/data.txt	 
fi
#Read data from file
cut -b 1-5,9,10 data.txt
