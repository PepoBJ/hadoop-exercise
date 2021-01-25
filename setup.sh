#!/bin/bash
GREEN='\033[0;32m'
NC='\033[0m' # No Color

printf "\n${GREEN}> UPLOAD FILES TO HDFS${NC}\n"
./upload_files.sh

printf "\n${GREEN}> CREATE TABLES HIVE${NC}\n"
hive -f create_tables.hql;
echo "Done."

printf "\n${GREEN}> EXECUTE QUERIES IN SPARK${NC}\n"
export PYTHONIOENCODING=utf8
spark-submit queries_data.py
printf "\n${GREEN}> DONE.${NC}\n"