from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum as _sum, lower

# Create Spark session
spark = SparkSession.builder.appName("NBAZoneHitRate").getOrCreate()

# Load CSV
df = spark.read.option("header", True).csv("hdfs://10.128.0.6:9000/part2/input/shot_logs.csv")

# Filter only for specified players (case-insensitive)
target_players = ["james harden", "chris paul", "stephen curry", "lebron james"]
df = df.withColumn("player_name_lower", lower(col("player_name")))
df = df.filter(col("player_name_lower").isin(target_players))

# Convert needed columns to numeric
df = df.withColumn("SHOT_DIST", col("SHOT_DIST").cast("double")) \
       .withColumn("CLOSE_DEF_DIST", col("CLOSE_DEF_DIST").cast("double")) \
       .withColumn("SHOT_CLOCK", col("SHOT_CLOCK").cast("double")) \
       .withColumn("FGM", col("FGM").cast("int"))

# Assign shooting zones
df = df.withColumn("Zone",
    when((col("SHOT_DIST") <= 10) & (col("CLOSE_DEF_DIST") <= 3) & (col("SHOT_CLOCK") >= 15), 1)
   .when((col("SHOT_DIST") > 10) & (col("SHOT_DIST") <= 20) &
         (col("CLOSE_DEF_DIST") > 3) & (col("CLOSE_DEF_DIST") <= 6) &
         (col("SHOT_CLOCK") > 7) & (col("SHOT_CLOCK") < 15), 2)
   .when((col("SHOT_DIST") > 20) & (col("CLOSE_DEF_DIST") > 6) &
         (col("SHOT_CLOCK") <= 7), 3)
   .otherwise(4)
)

# Group and calculate stats
results = df.groupBy("player_name", "Zone") \
    .agg(
        _sum("FGM").alias("FGM_total"),
        count("FGM").alias("Attempts")
    ) \
    .withColumn("HitRate", (col("FGM_total") / col("Attempts")).cast("double"))

# Format and show output
results.select("player_name", "Zone", "HitRate", "Attempts").show(truncate=False)

# Stop session
spark.stop()
