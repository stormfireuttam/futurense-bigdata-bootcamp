#!/bin/sh

#list all files in current user location
ls
#remove files ending with extension .txt
rm *.txt
#add on the file in the same directory inside the OtherData.tar file
tar -xf OtherData.tar .
#Make sure all files appended in the same
ls -l
#If no file present with ".txt" extension display a message
if [ ls *.txt | wc -lt 0 ]
then
	echo "NO FILE PRESENT WITH THE .TXT EXTENSION"
fi

