#!/bin/bash

python3 python-script-movies.py

start-all.sh

hdfs dfs -put '/home/uttam/futurense-datengg-bootcamp/assessments/bigdata-assessment-01/movies1.csv' '/user/training/movie'
hdfs dfs -put '/home/uttam/futurense-datengg-bootcamp/assessments/bigdata-assessment-01/ratings.csv' '/user/training/movie'
