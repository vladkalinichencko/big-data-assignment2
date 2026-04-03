#!/bin/bash

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

if [ -f "a.parquet" ]; then
    hdfs dfs -put -f a.parquet / && spark-submit prepare_data.py
fi

hdfs dfs -rm -r -f /data
hdfs dfs -mkdir -p /data
hdfs dfs -put data/* /data/
