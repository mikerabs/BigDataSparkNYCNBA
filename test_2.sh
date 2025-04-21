#!/bin/bash

# Usage: ./test_2.sh <parallelism_level>
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <parallelism_level>"
  exit 1
fi

PARALLELISM=$1
SPARK_MASTER="10.128.0.6"  # or "manager" if /etc/hosts resolves it

INPUT_DIR="/part2/input"
INPUT_FILE="shot_logs.csv"
LOCAL_PATH="./datasets/$INPUT_FILE"
HDFS_PATH="hdfs://$SPARK_MASTER:9000$INPUT_DIR/$INPUT_FILE"

# Ensure HDFS input directory exists
/usr/local/hadoop/bin/hdfs dfs -mkdir -p "$INPUT_DIR"

# Force copy dataset into HDFS
/usr/local/hadoop/bin/hdfs dfs -put -f "$LOCAL_PATH" "$INPUT_DIR"

# Submit Spark job
/usr/local/spark/bin/spark-submit \
  --master "spark://$SPARK_MASTER:7077" \
  --conf "spark.default.parallelism=$PARALLELISM" \
  nba_zones_spark.py "$HDFS_PATH"
