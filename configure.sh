#!/bin/bash

# HDP 2.6.4
# pip install --upgrade numpy scipy pandas scikit-learn tornado pyzmq pygments matplotlib jinja2 jsonschema
# pip install jupyter
# chmod +x configure.sh

PYSPARK_DRIVER_PYTHON_OPTS="notebook --port 8889 \
--notebook-dir='/usr/hdp/current/spark-client/' \
--ip='*' --no-browser" pyspark
