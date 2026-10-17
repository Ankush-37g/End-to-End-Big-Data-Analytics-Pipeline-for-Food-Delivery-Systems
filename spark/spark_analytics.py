"""
Spark Analytics Script
Phase 5: PySpark analytics matching Hive/MapReduce queries for comparison

Run: spark-submit spark/spark_analytics.py

This script replicates the same analyses done in MapReduce and Hive,
enabling a direct runtime comparison across all three frameworks.
"""

import os
import sys
import time

# Configure UTF-8 encoding for Windows compatibility
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# Configure JAVA_HOME for PySpark
if "JAVA_HOME" not in os.environ or not os.path.exists(os.environ.get("JAVA_HOME", "")):
    for candidate in [
        r"C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot",
        r"C:\Program Files\Java\jdk-17",
        r"C:\Program Files\Eclipse Adoptium\jdk-17",
    ]:
        if os.path.exists(candidate):
            os.environ["JAVA_HOME"] = candidate
            os.environ["PATH"] = os.path.join(candidate, "bin") + ";" + os.environ.get("PATH", "")
            break

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# Initialize Spark Session
# ============================================================
spark = SparkSession.builder \
    .appName("FoodDelivery-SparkAnalytics") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.ansi.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

print("=" * 60)
print("  Food Delivery Analytics - PySpark")
print("=" * 60)

# ============================================================
# Load Data (uses shared loader for HDFS/local compatibility)
# ============================================================
# Add project paths so data_loader is importable from any working directory
import os as _os, sys as _sys
_THIS_DIR = _os.path.dirname(_os.path.abspath(__file__))
_PROJECT_ROOT = _os.path.abspath(_os.path.join(_THIS_DIR, ".."))
if _THIS_DIR not in _sys.path:
    _sys.path.insert(0, _THIS_DIR)

from data_loader import load_data

df, data_source = load_data(spark)
print(f"  Data source: {data_source}")

# Cache the DataFrame for repeated use
df.cache()
total_rows = df.count()
print(f"  Total rows: {total_rows:,}")
df.printSchema()

# Track execution times for comparison
timings = {}

# ============================================================
# Analysis 1: Orders Per City/Area (matches MapReduce Job 1)
# ============================================================
print("\n" + "=" * 60)
print("📊 Analysis 1: Orders Per City/Area")
print("=" * 60)

start = time.time()

orders_by_city = df.groupBy("city") \
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("net_revenue").alias("total_revenue"),
        F.round(F.avg("net_revenue"), 2).alias("avg_order_value")
    ) \
    .orderBy(F.desc("total_orders"))

orders_by_city.show(10, truncate=False)

orders_by_area = df.groupBy("city", "area") \
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("net_revenue").alias("total_revenue")
    ) \
    .orderBy(F.desc("total_orders"))

orders_by_area.show(15, truncate=False)

timings['orders_per_city'] = round(time.time() - start, 2)
print(f"⏱️  Execution time: {timings['orders_per_city']}s")

# ============================================================
# Analysis 2: Revenue Per Cuisine (matches MapReduce Job 2)
# ============================================================
print("\n" + "=" * 60)
print("📊 Analysis 2: Revenue Per Cuisine")
print("=" * 60)

start = time.time()

delivered = df.filter(F.col("status") == "Delivered")
total_revenue = delivered.agg(F.sum("net_revenue")).collect()[0][0]

revenue_by_cuisine = delivered.groupBy("cuisine") \
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("net_revenue").alias("total_revenue"),
        F.round(F.avg("net_revenue"), 2).alias("avg_revenue"),
        F.round(F.sum("net_revenue") * 100 / total_revenue, 2).alias("revenue_share_pct")
    ) \
    .orderBy(F.desc("total_revenue"))

revenue_by_cuisine.show(20, truncate=False)

timings['revenue_per_cuisine'] = round(time.time() - start, 2)
print(f"⏱️  Execution time: {timings['revenue_per_cuisine']}s")

# ============================================================
# Analysis 3: Average Delivery Time Per Hour (matches MR Job 3)
# ============================================================
print("\n" + "=" * 60)
print("📊 Analysis 3: Average Delivery Time Per Hour")
print("=" * 60)

start = time.time()

delivery_by_hour = delivered.groupBy("order_hour") \
    .agg(
        F.count("*").alias("total_orders"),
        F.round(F.avg("delivery_duration_min"), 2).alias("avg_delivery_min"),
        F.round(F.min("delivery_duration_min"), 2).alias("min_delivery_min"),
        F.round(F.max("delivery_duration_min"), 2).alias("max_delivery_min"),
        F.round(F.stddev("delivery_duration_min"), 2).alias("stddev_delivery_min")
    ) \
    .orderBy("order_hour")

delivery_by_hour.show(24, truncate=False)

timings['avg_delivery_per_hour'] = round(time.time() - start, 2)
print(f"⏱️  Execution time: {timings['avg_delivery_per_hour']}s")

# ============================================================
# Analysis 4: Peak Hours Analysis
# ============================================================
print("\n" + "=" * 60)
print("📊 Analysis 4: Peak Hours Analysis")
print("=" * 60)

start = time.time()

peak_hours = delivered.groupBy("order_hour", "time_of_day") \
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("net_revenue").alias("total_revenue"),
        F.round(F.avg("delivery_duration_min"), 2).alias("avg_delivery_min"),
        F.sum(F.when(F.col("is_delayed") == 1, 1).otherwise(0)).alias("delayed_orders")
    ) \
    .withColumn("delay_rate_pct", 
        F.round(F.col("delayed_orders") * 100 / F.col("total_orders"), 2)) \
    .orderBy("order_hour")

peak_hours.show(24, truncate=False)

timings['peak_hours'] = round(time.time() - start, 2)
print(f"⏱️  Execution time: {timings['peak_hours']}s")

# ============================================================
# Analysis 5: Cancellation Rate by Payment Method
# ============================================================
print("\n" + "=" * 60)
print("📊 Analysis 5: Cancellation Rate by Payment Method")
print("=" * 60)

start = time.time()

cancel_by_payment = df.groupBy("payment_method") \
    .agg(
        F.count("*").alias("total_orders"),
        F.sum(F.when(F.col("status") == "Cancelled", 1).otherwise(0)).alias("cancelled"),
        F.sum(F.when(F.col("status") == "Delivered", 1).otherwise(0)).alias("delivered")
    ) \
    .withColumn("cancellation_rate_pct",
        F.round(F.col("cancelled") * 100 / F.col("total_orders"), 2)) \
    .orderBy(F.desc("cancellation_rate_pct"))

cancel_by_payment.show(truncate=False)

timings['cancel_by_payment'] = round(time.time() - start, 2)
print(f"⏱️  Execution time: {timings['cancel_by_payment']}s")

# ============================================================
# Analysis 6: Delay Rate by Weather & Traffic
# ============================================================
print("\n" + "=" * 60)
print("📊 Analysis 6: Delay Rate by Weather & Traffic")
print("=" * 60)

start = time.time()

delay_conditions = delivered.groupBy("weather", "traffic_level") \
    .agg(
        F.count("*").alias("total_orders"),
        F.round(F.avg("delivery_duration_min"), 2).alias("avg_delivery_min"),
        F.sum(F.when(F.col("is_delayed") == 1, 1).otherwise(0)).alias("delayed_orders")
    ) \
    .withColumn("delay_rate_pct",
        F.round(F.col("delayed_orders") * 100 / F.col("total_orders"), 2)) \
    .orderBy(F.desc("delay_rate_pct"))

delay_conditions.show(20, truncate=False)

timings['delay_by_conditions'] = round(time.time() - start, 2)
print(f"⏱️  Execution time: {timings['delay_by_conditions']}s")

# ============================================================
# Analysis 7: Window Functions - Restaurant Rank by City
# ============================================================
print("\n" + "=" * 60)
print("📊 Analysis 7: Restaurant Rank Within City (Window Function)")
print("=" * 60)

start = time.time()

restaurant_stats = delivered.groupBy("city", "restaurant_id", "cuisine") \
    .agg(
        F.sum("net_revenue").alias("total_revenue"),
        F.count("*").alias("total_orders"),
        F.round(F.avg("customer_rating"), 2).alias("avg_rating")
    )

windowSpec = Window.partitionBy("city").orderBy(F.desc("total_revenue"))

ranked_restaurants = restaurant_stats \
    .withColumn("revenue_rank", F.rank().over(windowSpec)) \
    .filter(F.col("revenue_rank") <= 5) \
    .orderBy("city", "revenue_rank")

ranked_restaurants.show(50, truncate=False)

timings['restaurant_rank'] = round(time.time() - start, 2)
print(f"⏱️  Execution time: {timings['restaurant_rank']}s")

# ============================================================
# Analysis 8: Caching Performance Test
# ============================================================
print("\n" + "=" * 60)
print("📊 Analysis 8: Caching Performance Comparison")
print("=" * 60)

# Unpersist and test without cache
df.unpersist()

# Without cache
start = time.time()
df.filter(F.col("status") == "Delivered") \
    .groupBy("city").agg(F.count("*")).collect()
no_cache_time = round(time.time() - start, 2)
print(f"⏱️  Without cache: {no_cache_time}s")

# With cache
df.cache()
df.count()  # Trigger caching
start = time.time()
df.filter(F.col("status") == "Delivered") \
    .groupBy("city").agg(F.count("*")).collect()
with_cache_time = round(time.time() - start, 2)
print(f"⏱️  With cache:    {with_cache_time}s")
print(f"📈 Speedup:       {round(no_cache_time / max(with_cache_time, 0.01), 2)}x")

timings['cache_no'] = no_cache_time
timings['cache_yes'] = with_cache_time

# ============================================================
# Save Results to HDFS / Local
# ============================================================
print("\n" + "=" * 60)
print("💾 Saving Results")
print("=" * 60)

OUTPUT_BASE = "hdfs:///fooddelivery/output/spark"
LOCAL_OUTPUT = _os.path.join(_PROJECT_ROOT, "data", "output", "spark")

try:
    orders_by_city.coalesce(1).write.mode("overwrite") \
        .csv(f"{OUTPUT_BASE}/orders_by_city", header=True)
    revenue_by_cuisine.coalesce(1).write.mode("overwrite") \
        .csv(f"{OUTPUT_BASE}/revenue_by_cuisine", header=True)
    delivery_by_hour.coalesce(1).write.mode("overwrite") \
        .csv(f"{OUTPUT_BASE}/delivery_by_hour", header=True)
    delay_conditions.coalesce(1).write.mode("overwrite") \
        .csv(f"{OUTPUT_BASE}/delay_conditions", header=True)
    cancel_by_payment.coalesce(1).write.mode("overwrite") \
        .csv(f"{OUTPUT_BASE}/cancel_by_payment", header=True)
    print(f"✅ Results saved to HDFS: {OUTPUT_BASE}/")
except Exception:
    os.makedirs(LOCAL_OUTPUT, exist_ok=True)
    orders_by_city.toPandas().to_csv(f"{LOCAL_OUTPUT}/orders_by_city.csv", index=False)
    revenue_by_cuisine.toPandas().to_csv(f"{LOCAL_OUTPUT}/revenue_by_cuisine.csv", index=False)
    delivery_by_hour.toPandas().to_csv(f"{LOCAL_OUTPUT}/delivery_by_hour.csv", index=False)
    delay_conditions.toPandas().to_csv(f"{LOCAL_OUTPUT}/delay_conditions.csv", index=False)
    cancel_by_payment.toPandas().to_csv(f"{LOCAL_OUTPUT}/cancel_by_payment.csv", index=False)
    print(f"✅ Results saved locally: {LOCAL_OUTPUT}/")

# ============================================================
# Timing Summary (for comparison table in report)
# ============================================================
print("\n" + "=" * 60)
print("⏱️  EXECUTION TIME SUMMARY (Spark)")
print("=" * 60)
for analysis, duration in timings.items():
    print(f"  {analysis:30s} : {duration:8.2f}s")
print("=" * 60)

# Cleanup
spark.stop()
print("\n✅ Spark Analytics Complete!")
