#!/bin/bash

echo "Upload countries..."
hdfs dfs -mkdir -p /user/training/upload/data/country/
hdfs dfs -put -f /home/maria_dev/hadoop-exercise/data/paises.ada /user/training/upload/data/country/
echo "Upload flights..."
hdfs dfs -mkdir -p /user/training/upload/data/flight/
hdfs dfs -put -f /home/maria_dev/hadoop-exercise/data/vuelos.dat /user/training/upload/data/flight/
echo "Upload delays..."
hdfs dfs -mkdir -p /user/training/upload/data/delay/
hdfs dfs -put -f /home/maria_dev/hadoop-exercise/data/retrasos.dat /user/training/upload/data/delay/
echo "Upload dates..."
hdfs dfs -mkdir -p /user/training/upload/data/date/
hdfs dfs -put -f /home/maria_dev/hadoop-exercise/data/fecha.dat /user/training/upload/data/date/