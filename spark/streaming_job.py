from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType
)

# ---- 1. Create Spark Session ----
# This is always the entry point for any Spark application
spark = SparkSession.builder \
    .appName("EcommerceStreamingPipeline") \
    .config("spark.jars.packages",
            # These packages let Spark talk to Kafka and Delta Lake
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")  # reduce noise in logs

print("✅ Spark session created")

# ---- 2. Define the Event Schema ----
# Spark needs to know the shape of our JSON events upfront
# This is the FULL schema covering all event types
# Fields not present in a given event type will just be null
schema = StructType([
    StructField("event_type",     StringType(),  True),
    StructField("event_id",       StringType(),  True),
    StructField("user_id",        StringType(),  True),
    StructField("timestamp",      StringType(),  True),
    StructField("product_id",     StringType(),  True),
    StructField("product_name",   StringType(),  True),
    StructField("category",       StringType(),  True),
    StructField("session_id",     StringType(),  True),   # page_view only
    StructField("quantity",       IntegerType(), True),   # cart + order only
    StructField("price",          DoubleType(),  True),   # cart + order only
    StructField("order_id",       StringType(),  True),   # order only
    StructField("total_amount",   DoubleType(),  True),   # order only
    StructField("payment_method", StringType(),  True),   # order only
    StructField("status",         StringType(),  True),   # order only
])

# ---- 3. Read from Kafka ----
# Spark treats the Kafka stream like a table that keeps growing
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "ecommerce_events") \
    .option("startingOffsets", "latest") \
    .load()
print("✅ Connected to Kafka topic: ecommerce_events")

# ---- 4. Parse the Raw Events ----
# Kafka gives us raw bytes. We cast to string, then parse JSON.
parsed_stream = raw_stream \
    .select(
        # Extract the message value (the JSON string)
        col("value").cast("string").alias("raw_json"),
        # Kafka also gives us useful metadata
        col("timestamp").alias("kafka_timestamp"),
        col("partition"),
        col("offset")
    ) \
    .select(
        # Parse JSON string into proper columns using our schema
        from_json(col("raw_json"), schema).alias("data"),
        col("kafka_timestamp"),
        col("partition"),
        col("offset")
    ) \
    .select(
        # Flatten the nested 'data' struct into top-level columns
        col("data.*"),
        col("kafka_timestamp"),
        col("partition"),
        col("offset")
    ) \
    .withColumn(
        # Convert timestamp string to proper timestamp type
        "event_timestamp", to_timestamp(col("timestamp"))
    ) \
    .withColumn(
        # Add ingestion time — useful for debugging late arrivals
        "ingested_at", to_timestamp(lit(
            str(__import__('datetime').datetime.utcnow())
        ))
    )

# ---- 5. Write Bronze Layer to Delta Lake ----
# Bronze = raw, unmodified events. We just parse and store everything.
# "append" mode means we never overwrite — every event is kept forever.
bronze_query = parsed_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark-data/checkpoints/bronze") \
    .option("mergeSchema", "true") \
    .start("/opt/spark-data/bronze/ecommerce_events")

print("✅ Bronze layer streaming to Delta Lake at /opt/spark-data/bronze/")
print("   Spark UI available at http://localhost:4040")
print("\n   Waiting for events... (Ctrl+C to stop)\n")

# Keep the stream running until manually stopped
bronze_query.awaitTermination()