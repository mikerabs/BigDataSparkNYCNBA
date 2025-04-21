from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, count, regexp_replace, upper, row_number
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.clustering import KMeans

# Create Spark session
spark = SparkSession.builder.appName("LincolnCenterParking").getOrCreate()

# Load dataset
df = spark.read.option("header", True).csv("hdfs:///part1/input/Parking_Violations_Issued_-_Fiscal_Year_2017.csv")

# Filter precincts near Lincoln Center (within ~0.5 miles)
df = df.filter(col("Violation Precinct").isin( "20", "18", "24"))

# Filter for violations around 10AM (9–11AM window)
df = df.filter(
    (col("Violation Time").isNotNull()) &
    (col("Street Name").isNotNull()) &
    (col("Violation Time").substr(1, 2).cast("int") >= 9) &
    (col("Violation Time").substr(1, 2).cast("int") <= 11)
)

# Clean up and normalize street names
df = df.withColumn("StreetCodeInt", col("Street Code1").cast("int"))
df = df.withColumn("StreetNameClean", regexp_replace(upper(col("Street Name")), "[^A-Z0-9]", ""))
df = df.dropna(subset=["StreetCodeInt", "Vehicle Color", "StreetNameClean"])

# Index vehicle color
indexer = StringIndexer(inputCol="Vehicle Color", outputCol="ColorIndex")
indexer_model = indexer.fit(df)
df = indexer_model.transform(df)

# Assemble features
vec_assembler = VectorAssembler(inputCols=["ColorIndex", "StreetCodeInt"], outputCol="features")
df_vector = vec_assembler.transform(df)

# KMeans clustering
kmeans = KMeans(k=5, seed=42)
model = kmeans.fit(df_vector)
clustered = model.transform(df_vector)

# Count by cluster + street
cluster_summary = clustered.groupBy("prediction", "StreetNameClean").agg(count("*").alias("count"))

# Top 10 unique cleaned street names per cluster
window_spec = Window.partitionBy("prediction").orderBy(col("count").desc())
top_streets = cluster_summary.withColumn("rank", row_number().over(window_spec)).filter(col("rank") <= 10)

# Total by cluster and overall
total_by_cluster = clustered.groupBy("prediction").agg(count("*").alias("total"))
total_all = clustered.count()
prob_df = total_by_cluster.withColumn("probability", col("total") / total_all)

# Final join
final = top_streets.join(prob_df, on="prediction").select(
    "prediction", "StreetNameClean", "count", "total", "probability"
).orderBy("prediction", "count", ascending=[True, False])

final.show(truncate=False)

spark.stop()

