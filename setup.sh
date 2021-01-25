#!/bin/bash
GREEN='\033[0;32m'
NC='\033[0m' # No Color

printf "${GREEN}> UPLOAD FILES TO HDFS${NC}"
./upload_files.sh

printf "${GREEN}> CREATE TABLES HIVE${NC}"
hive -f create_tables.hql;
echo "Done."

printf "${GREEN}> EXECUTE QUERIES IN SPARK${NC}"
export PYTHONIOENCODING=utf8
spark-submit queries_data.py
