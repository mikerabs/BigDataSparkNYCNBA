from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, length, count
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

# Start Spark session
spark = SparkSession.builder.appName("LincolnCenterParkingProbabilities").getOrCreate()

# Load dataset
df = spark.read.option("header", True).csv("hdfs:///part1/input/Parking_Violations_Issued_-_Fiscal_Year_2017.csv")

# Filter for valid Violation Time around 10 AM and non-null Street Code1
df_filtered = df.filter(
    (length(col("Violation Time")) >= 4) &
    (substring(col("Violation Time"), 1, 2).cast("int").between(9, 11)) &
    (col("Street Code1").isNotNull())
)

# Focus on Lincoln Center vicinity street codes
lincoln_street_codes = ["34510", "34050", "10030", "34090", "34070", "34500", "34060"]

df_lc = df_filtered.filter(col("Street Code1").isin(lincoln_street_codes))
df_lc = df_lc.withColumn("StreetCodeInt", col("Street Code1").cast("int"))

# Vector assembler for clustering
vec_assembler = VectorAssembler(inputCols=["StreetCodeInt"], outputCol="features")
df_features = vec_assembler.transform(df_lc)

# KMeans clustering
kmeans = KMeans(k=4, seed=42)
model = kmeans.fit(df_features)
clustered = model.transform(df_features)

# Count tickets per cluster
cluster_counts = clustered.groupBy("prediction").agg(count("*").alias("ticket_count"))

# Total tickets for probability calculation
total_tickets = clustered.count()
cluster_probs = cluster_counts.withColumn("probability", col("ticket_count") / total_tickets)

# Output cluster probabilities
print("\n➡️ Cluster Ticket Probabilities around Lincoln Center at 10 AM:\n")
cluster_probs.orderBy("probability").show(truncate=False)

spark.stop()
