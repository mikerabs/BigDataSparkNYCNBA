#!/bin/bash

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <parallelism_level>"
  exit 1
fi

PARALLELISM=$1
SPARK_MASTER=10.128.0.6  # or "manager" if hostname resolves properly

# Copy dataset into HDFS path if not already present
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /part1/input/
/usr/local/hadoop/bin/hdfs dfs -put -f ./datasets/Parking_Violations_Issued_-_Fiscal_Year_2017.csv /part1/input/

# Submit Spark job
/usr/local/spark/bin/spark-submit \
  --master spark://$SPARK_MASTER:7077 \
  --conf "spark.default.parallelism=$PARALLELISM" \
  violation_by_month.py hdfs://$SPARK_MASTER:9000/part1/input/Parking_Violations_Issued_-_Fiscal_Year_2017.csv

