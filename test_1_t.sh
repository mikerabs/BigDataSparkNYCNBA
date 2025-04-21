from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: violation_by_month <file>", file=sys.stderr)
        sys.exit(-1)

    input_path = sys.argv[1]

    spark = SparkSession.builder.appName("ViolationByMonth").getOrCreate()

    # Read the CSV file passed as an argument
    df = spark.read.option("header", True).csv(input_path)

    # Extract month from "Issue Date" assuming format MM/DD/YYYY
    df_with_month = df.withColumn("Month", split(col("Issue Date"), "/").getItem(0))

    # Count violations by month
    monthly_counts = df_with_month.groupBy("Month").count().orderBy("count", ascending=False)

    # Save results to HDFS (adjust output path as needed)
    monthly_counts.write.csv("hdfs://10.128.0.6:9000/violation/output", header=True)

    # Print to console for logging/debugging
    for row in monthly_counts.collect():
        print(f"{row['Month']}: {row['count']}")

    spark.stop()
