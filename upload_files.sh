#!/bin/bash

echo "Upload countries..."
hdfs dfs -mkdir -p /user/training/upload/data/country/
hdfs dfs -put -f ./data/paises.ada /user/training/upload/data/country/
echo "Upload flights..."
hdfs dfs -mkdir -p /user/training/upload/data/flight/
hdfs dfs -put -f ./data/vuelos.dat /user/training/upload/data/flight/
echo "Upload delays..."
hdfs dfs -mkdir -p /user/training/upload/data/delay/
hdfs dfs -put -f ./data/retrasos.dat /user/training/upload/data/delay/
echo "Upload dates..."
hdfs dfs -mkdir -p /user/training/upload/data/date/
hdfs dfs -put -f ./data/fecha.dat /user/training/upload/data/date/