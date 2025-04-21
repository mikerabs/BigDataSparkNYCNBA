from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.clustering import KMeans

# Start Spark session
spark = SparkSession.builder.appName("KMeansTicketProbability").getOrCreate()

# Load data
df = spark.read.option("header", True).csv("hdfs:///part1/input/Parking_Violations_Issued_-_Fiscal_Year_2017.csv")

# Use Street Code1, 2, and 3
df = df.select("Vehicle Color", "Street Code1", "Street Code2", "Street Code3").dropna()

# Cast street codes to integers
df = df.withColumn("StreetCode1", col("Street Code1").cast("int")) \
       .withColumn("StreetCode2", col("Street Code2").cast("int")) \
       .withColumn("StreetCode3", col("Street Code3").cast("int"))

# Encode vehicle color
indexer = StringIndexer(inputCol="Vehicle Color", outputCol="ColorIndex")
indexer_model = indexer.fit(df)
df = indexer_model.transform(df)

# Assemble feature vector
vec_assembler = VectorAssembler(
    inputCols=["ColorIndex", "StreetCode1", "StreetCode2", "StreetCode3"],
    outputCol="features"
)
final_df = vec_assembler.transform(df)

# Train KMeans
kmeans = KMeans(k=5, seed=42)
model = kmeans.fit(final_df)

# Assign clusters
clustered = model.transform(final_df)

# Query for BLACK car on 3 streets
query_data = spark.createDataFrame([
    ("BLACK", "34510", "10030", "34050")
], ["Vehicle Color", "Street Code1", "Street Code2", "Street Code3"])

query_data = query_data.withColumn("StreetCode1", col("Street Code1").cast("int")) \
                       .withColumn("StreetCode2", col("Street Code2").cast("int")) \
                       .withColumn("StreetCode3", col("Street Code3").cast("int"))

query_data = indexer_model.transform(query_data)
query_data = vec_assembler.transform(query_data)
query_clusters = model.transform(query_data).select("prediction").rdd.flatMap(lambda x: x).collect()

# Estimate probability
target_tickets = clustered.filter(col("prediction").isin(query_clusters))
black_tickets = clustered.filter(lower(col("Vehicle Color")) == "black")

prob = target_tickets.count() / black_tickets.count() if black_tickets.count() > 0 else 0
print(f"\n➡️ Estimated probability a BLACK car parked at street codes 34510, 10030, or 34050 gets a ticket: {prob:.3f}\n")

spark.stop()
