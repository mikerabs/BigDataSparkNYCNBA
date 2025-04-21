# kmeans_ticket_probability.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.clustering import KMeans

spark = SparkSession.builder.appName("ParkingTicketKMeans").getOrCreate()

# Load and clean data
df = spark.read.option("header", True).csv("hdfs:///part1/input/Parking_Violations_Issued_-_Fiscal_Year_2017.csv")
df = df.select("Vehicle Color", "Street Code1").dropna()
df = df.filter(col("Street Code1").cast("int").isNotNull())

# Filter to common colors and codes
df = df.filter(lower(col("Vehicle Color")).isin("black", "white", "gray", "silver", "red", "blue"))

# Index vehicle color
indexer = StringIndexer(inputCol="Vehicle Color", outputCol="ColorIndex")
df = indexer.fit(df).transform(df)

# Convert street code to int
df = df.withColumn("StreetCodeInt", col("Street Code1").cast("int"))

# Assemble features for clustering
vec_assembler = VectorAssembler(inputCols=["ColorIndex", "StreetCodeInt"], outputCol="features")
final_df = vec_assembler.transform(df)

# Apply K-Means
kmeans = KMeans(k=5, seed=42)  # 5 clusters is arbitrary, try different values
model = kmeans.fit(final_df)
centers = model.clusterCenters()

# Assign clusters to data
clustered = model.transform(final_df)

# Simulate query: a black vehicle on street code 34510, 10030, or 34050
query_data = spark.createDataFrame([
    ("black", 34510),
    ("black", 10030),
    ("black", 34050)
], ["Vehicle Color", "Street Code1"])

query_data = indexer.fit(df).transform(query_data)
query_data = query_data.withColumn("StreetCodeInt", col("Street Code1").cast("int"))
query_data = vec_assembler.transform(query_data)
query_clusters = model.transform(query_data).select("prediction").rdd.flatMap(lambda x: x).collect()

# Estimate ticket probability per cluster
from collections import Counter
cluster_counts = clustered.groupBy("prediction").count().collect()
total = sum(row["count"] for row in cluster_counts)
cluster_probabilities = {row["prediction"]: row["count"] / total for row in cluster_counts}

# Print estimated probabilities for each matching cluster
print("Estimated probability per matched cluster:")
for c in query_clusters:
    print(f"Cluster {c}: {cluster_probabilities.get(c, 0):.3f}")

spark.stop()
