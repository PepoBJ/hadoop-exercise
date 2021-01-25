#!/bin/bash

echo '> UPLOAD FILES TO HDFS'
./upload_files.sh

echo '> CREATE TABLES HIVE'
hive -f create_tables.sql;
echo 'Done.'

echo '> EXECUTE QUERIES IN SPARK'
export PYTHONIOENCODING=utf8
spark-submit queries_data.py
