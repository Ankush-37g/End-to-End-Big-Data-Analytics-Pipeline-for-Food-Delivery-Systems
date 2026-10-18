"""
Spark Performance Comparison Script
Phase 5: Compare MapReduce vs Hive vs Spark execution times

Run: spark-submit spark/performance_compare.py


This script runs the same 3 queries across Spark and records timing,
to be compared with MapReduce and Hive timings recorded separately.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time
import json
import os
import sys

# Configure UTF-8 encoding for Windows compatibility
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# ============================================================
# Initialize
# ============================================================
spark = SparkSession.builder \
    .appName("FoodDelivery-PerformanceComparison") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

print("=" * 60)
print("  Performance Comparison: MapReduce vs Hive vs Spark")
print("=" * 60)

# ============================================================
# Load Data (uses shared loader for HDFS/local compatibility)
# ============================================================
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_THIS_DIR, ".."))
if _THIS_DIR not in sys.path:
    sys.path.insert(0, _THIS_DIR)

from data_loader import load_data

df, data_source = load_data(spark)
row_count = df.count()
print(f"  Data source: {data_source}")
print(f"  Total rows: {row_count:,}\n")

results = {}

# ============================================================
# Query 1: Orders Per City (matches MapReduce Job 1)
# ============================================================
print("🔄 Running Query 1: Orders Per City...")

# Run WITHOUT cache
start = time.time()
q1_result = df.groupBy("city") \
    .agg(F.count("*").alias("total_orders")) \
    .orderBy(F.desc("total_orders")) \
    .collect()
spark_no_cache_q1 = round(time.time() - start, 3)

# Run WITH cache
df.cache()
df.count()  # Materialize cache

start = time.time()
q1_result_cached = df.groupBy("city") \
    .agg(F.count("*").alias("total_orders")) \
    .orderBy(F.desc("total_orders")) \
    .collect()
spark_cached_q1 = round(time.time() - start, 3)

results['Q1_orders_per_city'] = {
    'spark_no_cache': spark_no_cache_q1,
    'spark_cached': spark_cached_q1
}
print(f"  ✅ No cache: {spark_no_cache_q1}s | Cached: {spark_cached_q1}s")

# ============================================================
# Query 2: Revenue Per Cuisine (matches MapReduce Job 2)
# ============================================================
print("🔄 Running Query 2: Revenue Per Cuisine...")

start = time.time()
q2_result = df.filter(F.col("status") == "Delivered") \
    .groupBy("cuisine") \
    .agg(
        F.sum("net_revenue").alias("total_revenue"),
        F.count("*").alias("order_count"),
        F.round(F.avg("net_revenue"), 2).alias("avg_revenue")
    ) \
    .orderBy(F.desc("total_revenue")) \
    .collect()
spark_cached_q2 = round(time.time() - start, 3)

results['Q2_revenue_per_cuisine'] = {
    'spark_cached': spark_cached_q2
}
print(f"  ✅ Cached: {spark_cached_q2}s")

# ============================================================
# Query 3: Avg Delivery Time Per Hour (matches MR Job 3)
# ============================================================
print("🔄 Running Query 3: Avg Delivery Time Per Hour...")

start = time.time()
q3_result = df.filter(
    (F.col("status") == "Delivered") & 
    (F.col("delivery_duration_min") > 0)
) \
    .groupBy("order_hour") \
    .agg(
        F.round(F.avg("delivery_duration_min"), 2).alias("avg_delivery_min"),
        F.count("*").alias("order_count"),
        F.round(F.min("delivery_duration_min"), 2).alias("min_time"),
        F.round(F.max("delivery_duration_min"), 2).alias("max_time")
    ) \
    .orderBy("order_hour") \
    .collect()
spark_cached_q3 = round(time.time() - start, 3)

results['Q3_avg_delivery_per_hour'] = {
    'spark_cached': spark_cached_q3
}
print(f"  ✅ Cached: {spark_cached_q3}s")

# ============================================================
# Query 4: Complex - Delay by Weather + Traffic
# ============================================================
print("🔄 Running Query 4: Delay by Weather & Traffic...")

start = time.time()
q4_result = df.filter(F.col("status") == "Delivered") \
    .groupBy("weather", "traffic_level") \
    .agg(
        F.count("*").alias("total_orders"),
        F.round(F.avg("delivery_duration_min"), 2).alias("avg_delivery_min"),
        F.sum(F.when(F.col("is_delayed") == 1, 1).otherwise(0)).alias("delayed"),
    ) \
    .withColumn("delay_pct", 
        F.round(F.col("delayed") * 100 / F.col("total_orders"), 2)) \
    .orderBy(F.desc("delay_pct")) \
    .collect()
spark_cached_q4 = round(time.time() - start, 3)

results['Q4_delay_by_conditions'] = {
    'spark_cached': spark_cached_q4
}
print(f"  ✅ Cached: {spark_cached_q4}s")

# ============================================================
# Comparison Table (fill in MR and Hive times from your runs)
# ============================================================
print("\n" + "=" * 70)
print("  PERFORMANCE COMPARISON TABLE")
print("  (Fill in MapReduce and Hive times from your actual runs)")
print("=" * 70)
print(f"  {'Query':<30} {'MapReduce':>10} {'Hive(Text)':>12} {'Hive(ORC)':>12} {'Spark':>10}")
print(f"  {'-'*30} {'-'*10} {'-'*12} {'-'*12} {'-'*10}")
print(f"  {'Orders per city':<30} {'___s':>10} {'___s':>12} {'___s':>12} {str(spark_cached_q1)+'s':>10}")
print(f"  {'Revenue per cuisine':<30} {'___s':>10} {'___s':>12} {'___s':>12} {str(spark_cached_q2)+'s':>10}")
print(f"  {'Avg delivery per hour':<30} {'___s':>10} {'___s':>12} {'___s':>12} {str(spark_cached_q3)+'s':>10}")
print(f"  {'Delay by weather+traffic':<30} {'N/A':>10} {'___s':>12} {'___s':>12} {str(spark_cached_q4)+'s':>10}")
print("=" * 70)
print("\n📝 Note: Replace '___' with actual times from MapReduce and Hive runs.")
print("   MapReduce times from: hadoop jar ... (check job tracker)")
print("   Hive times from: hive -f hive/performance_compare.hql (console output)")

# ============================================================
# Save timing results
# ============================================================
OUTPUT_FILE = os.path.join(_PROJECT_ROOT, "data", "output", "performance_comparison.json")
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

comparison = {
    'data_rows': row_count,
    'data_source': data_source,
    'spark_results': results,
    'placeholder_times': {
        'mapreduce': {
            'Q1_orders_per_city': 'FILL_IN_SECONDS',
            'Q2_revenue_per_cuisine': 'FILL_IN_SECONDS',
            'Q3_avg_delivery_per_hour': 'FILL_IN_SECONDS'
        },
        'hive_text': {
            'Q1_orders_per_city': 'FILL_IN_SECONDS',
            'Q2_revenue_per_cuisine': 'FILL_IN_SECONDS',
            'Q3_avg_delivery_per_hour': 'FILL_IN_SECONDS'
        },
        'hive_orc': {
            'Q1_orders_per_city': 'FILL_IN_SECONDS',
            'Q2_revenue_per_cuisine': 'FILL_IN_SECONDS',
            'Q3_avg_delivery_per_hour': 'FILL_IN_SECONDS'
        }
    }
}

with open(OUTPUT_FILE, 'w') as f:
    json.dump(comparison, f, indent=2)
print(f"\n💾 Timing results saved to: {OUTPUT_FILE}")

spark.stop()
print("\n✅ Performance Comparison Complete!")
