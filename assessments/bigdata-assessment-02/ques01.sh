hdfs dfs -du -h <directory_path>/* | awk '$1 ~ /[1-9]*[0-9][0-9][0-9]MB/ {print $2}'


