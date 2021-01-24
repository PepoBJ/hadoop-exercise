#!/bin/bash

echo "Upload countries..."
hdfs dfs -mkdir -p /user/maria_dev/upload/data/country/
hdfs dfs -put -f ./data/paises.ada /user/maria_dev/upload/data/country/
echo "Upload flights..."
hdfs dfs -mkdir -p /user/maria_dev/upload/data/flight/
hdfs dfs -put -f ./data/vuelos.dat /user/maria_dev/upload/data/flight/
echo "Upload delays..."
hdfs dfs -mkdir -p /user/maria_dev/upload/data/delay/
hdfs dfs -put -f ./data/retrasos.dat /user/maria_dev/upload/data/delay/
echo "Upload dates..."
hdfs dfs -mkdir -p /user/maria_dev/upload/data/date/
hdfs dfs -put -f ./data/fecha.dat /user/maria_dev/upload/data/date/
echo "done"