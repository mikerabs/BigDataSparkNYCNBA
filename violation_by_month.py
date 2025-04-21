from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split

# Initialize Spark session
spark = SparkSession.builder.appName("ViolationByMonth").getOrCreate()

# Load CSV
df = spark.read.option("header", True).csv("hdfs:///part1/input/Parking_Violations_Issued_-_Fiscal_Year_2017.csv")
# Extract Month from Issue Date (format: MM/DD/YYYY)
df_with_month = df.withColumn("Month", split(col("Issue Date"), "/").getItem(0))

# Group by month and count
monthly_counts = df_with_month.groupBy("Month").count().orderBy("count", ascending=False)

# Show top 25
monthly_counts.show(25)

# Optional: Save to HDFS
# monthly_counts.write.csv("hdfs:///user/johndavis/output_violation_counts_spark", header=True)

spark.stop()
