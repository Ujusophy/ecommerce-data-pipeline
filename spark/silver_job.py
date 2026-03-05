from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, lit, count,
    window, round as spark_round
)
from delta.tables import DeltaTable
import datetime

# ---- 1. Spark Session ----
spark = SparkSession.builder \
    .appName("EcommerceSilverLayer") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

BRONZE_PATH = "/opt/spark-data/bronze/ecommerce_events"
SILVER_BASE  = "/opt/spark-data/silver"

print("✅ Spark session created")

# ---- 2. Read Bronze Layer ----
# We read the full Bronze Delta table as a batch (not streaming)
# Silver is typically run on a schedule (every hour/day) — that's Airflow's job later
bronze_df = spark.read.format("delta").load(BRONZE_PATH)

total_bronze = bronze_df.count()
print(f"📥 Bronze records loaded: {total_bronze:,}")

# ---- 3. Global Data Quality Checks ----
# Before splitting, apply rules that apply to ALL events

cleaned_df = bronze_df \
    .filter(col("event_id").isNotNull()) \
    .filter(col("user_id").isNotNull()) \
    .filter(col("event_type").isNotNull()) \
    .filter(col("event_timestamp").isNotNull()) \
    .dropDuplicates(["event_id"])

total_cleaned = cleaned_df.count()
dropped = total_bronze - total_cleaned
print(f"🧹 Records after cleaning: {total_cleaned:,}  (dropped {dropped:,} bad/duplicate records)")

# ---- 4. Split into Typed Tables ----

# -- 4a. Page Views --
page_views_df = cleaned_df \
    .filter(col("event_type") == "page_view") \
    .select(
        col("event_id"),
        col("user_id"),
        col("session_id"),
        col("product_id"),
        col("product_name"),
        col("category"),
        col("event_timestamp"),
        col("kafka_timestamp"),
        col("ingested_at"),
    ) \
    .withColumn("processed_at", lit(datetime.datetime.utcnow().isoformat()))

# -- 4b. Cart Events --
cart_df = cleaned_df \
    .filter(col("event_type") == "add_to_cart") \
    .filter(col("quantity").isNotNull()) \
    .filter(col("price").isNotNull()) \
    .select(
        col("event_id"),
        col("user_id"),
        col("product_id"),
        col("product_name"),
        col("category"),
        col("quantity"),
        col("price"),
        spark_round(col("quantity") * col("price"), 2).alias("line_total"),
        col("event_timestamp"),
        col("kafka_timestamp"),
        col("ingested_at"),
    ) \
    .withColumn("processed_at", lit(datetime.datetime.utcnow().isoformat()))

# -- 4c. Orders --
orders_df = cleaned_df \
    .filter(col("event_type") == "order_placed") \
    .filter(col("order_id").isNotNull()) \
    .filter(col("total_amount").isNotNull()) \
    .select(
        col("event_id"),
        col("order_id"),
        col("user_id"),
        col("product_id"),
        col("product_name"),
        col("category"),
        col("quantity"),
        col("price"),
        col("total_amount"),
        col("payment_method"),
        col("status"),
        # Business rule: flag failed payments explicitly
        when(col("status") == "failed", lit(True)).otherwise(lit(False)).alias("is_failed"),
        col("event_timestamp"),
        col("kafka_timestamp"),
        col("ingested_at"),
    ) \
    .withColumn("processed_at", lit(datetime.datetime.utcnow().isoformat()))


# ---- 5. Write Silver Tables ----
# We use Delta's MERGE (upsert) instead of overwrite
# This means re-running the job won't create duplicates — idempotent!

def upsert_to_delta(df, path, merge_key):
    """
    Upsert dataframe into a Delta table.
    - If record with same event_id exists → update it
    - If it's new → insert it
    This makes our Silver job safe to re-run anytime.
    """
    if DeltaTable.isDeltaTable(spark, path):
        delta_table = DeltaTable.forPath(spark, path)
        delta_table.alias("existing") \
            .merge(
                df.alias("updates"),
                f"existing.{merge_key} = updates.{merge_key}"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        print(f"   MERGE complete (upsert) → {path}")
    else:
        # First run — table doesn't exist yet, just write it
        df.write.format("delta").mode("overwrite").save(path)
        print(f"   Created new Delta table → {path}")


print("\n💾 Writing Silver tables...")

upsert_to_delta(page_views_df, f"{SILVER_BASE}/page_views", "event_id")
upsert_to_delta(cart_df,       f"{SILVER_BASE}/cart_events", "event_id")
upsert_to_delta(orders_df,     f"{SILVER_BASE}/orders",      "event_id")


# ---- 6. Print Summary ----
print("\n📊 Silver Layer Summary:")
print(f"   page_views:  {page_views_df.count():>6,} records")
print(f"   cart_events: {cart_df.count():>6,} records")
print(f"   orders:      {orders_df.count():>6,} records")

# Quick revenue snapshot
total_revenue = orders_df.filter(col("is_failed") == False) \
    .agg(spark_round(col("total_amount").cast("double").alias("total_amount"), 2)) \
    .collect()

successful_orders = orders_df.filter(col("is_failed") == False).count()
failed_orders     = orders_df.filter(col("is_failed") == True).count()

print(f"\n💰 Orders breakdown:")
print(f"   Successful: {successful_orders:,}")
print(f"   Failed:     {failed_orders:,}")
print(f"\n✅ Silver layer complete!")

spark.stop()