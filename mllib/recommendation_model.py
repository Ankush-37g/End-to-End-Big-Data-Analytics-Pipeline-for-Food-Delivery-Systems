"""
Spark MLlib: Restaurant Recommendation (ALS)
Phase 6 (Optional): Recommend restaurants to customers using
Alternating Least Squares (ALS) collaborative filtering.

Run: spark-submit mllib/recommendation_model.py

Uses customer-restaurant ratings to build a recommendation model.
"""

import os
import json
import sys

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
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# ============================================================
# Initialize Spark
# ============================================================
spark = SparkSession.builder \
    .appName("FoodDelivery-Recommendations") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.ansi.enabled", "false") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

print("=" * 60)
print("  MLlib: Restaurant Recommendations (ALS)")
print("=" * 60)

# ============================================================
# Load Data (uses shared loader for HDFS/local compatibility)
# ============================================================
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_THIS_DIR, ".."))
_SPARK_DIR = os.path.join(_PROJECT_ROOT, "spark")
if _SPARK_DIR not in sys.path:
    sys.path.insert(0, _SPARK_DIR)

from data_loader import load_data

df, _data_source = load_data(spark)

# ============================================================
# Prepare Rating Data
# ============================================================
print("\n📋 Preparing customer-restaurant ratings...")

# Filter delivered orders with valid ratings
ratings_df = df.filter(
    (F.col("status") == "Delivered") &
    (F.col("customer_rating") > 0) &
    (F.col("customer_rating") <= 5)
)

# Convert string IDs to numeric (ALS requires numeric IDs)
# Extract numeric part from customer_id (C00001 -> 1) and restaurant_id (R0001 -> 1)
ratings_df = ratings_df.withColumn(
    "customer_idx",
    F.regexp_extract("customer_id", r"(\d+)", 1).cast("int")
).withColumn(
    "restaurant_idx",
    F.regexp_extract("restaurant_id", r"(\d+)", 1).cast("int")
)

# Aggregate: average rating per customer-restaurant pair
user_item_ratings = ratings_df.groupBy("customer_idx", "restaurant_idx").agg(
    F.round(F.avg("customer_rating"), 2).alias("rating"),
    F.count("*").alias("order_count")
)

print(f"📊 Unique customers:    {user_item_ratings.select('customer_idx').distinct().count():,}")
print(f"📊 Unique restaurants:  {user_item_ratings.select('restaurant_idx').distinct().count():,}")
print(f"📊 Rating pairs:        {user_item_ratings.count():,}")
print(f"📊 Rating distribution:")
user_item_ratings.describe("rating").show()

# ============================================================
# Train/Test Split
# ============================================================
train_df, test_df = user_item_ratings.randomSplit([0.8, 0.2], seed=42)
print(f"📊 Training: {train_df.count():,} | Test: {test_df.count():,}")

# ============================================================
# ALS Model
# ============================================================
print("\n🔸 Training ALS Recommendation Model...")

als = ALS(
    userCol="customer_idx",
    itemCol="restaurant_idx",
    ratingCol="rating",
    maxIter=10,
    regParam=0.1,
    rank=10,
    coldStartStrategy="drop",  # Drop NaN predictions for cold start
    seed=42
)

# Cross-validation
paramGrid = ParamGridBuilder() \
    .addGrid(als.rank, [5, 10, 20]) \
    .addGrid(als.regParam, [0.01, 0.1, 0.5]) \
    .addGrid(als.maxIter, [5, 10]) \
    .build()

evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)

cv = CrossValidator(
    estimator=als,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    numFolds=3,
    seed=42
)

print("  Training with CrossValidator (3 folds)...")
cv_model = cv.fit(train_df)

# ============================================================
# Evaluate
# ============================================================
predictions = cv_model.transform(test_df)
rmse = evaluator.evaluate(predictions)

evaluator.setMetricName("mae")
mae = evaluator.evaluate(predictions)

print(f"\n  ✅ RMSE: {rmse:.4f}")
print(f"  ✅ MAE:  {mae:.4f}")

# Best model parameters
best_model = cv_model.bestModel
print(f"\n  📊 Best Parameters:")
print(f"     Rank: {best_model.rank}")
print(f"     MaxIter: {best_model._java_obj.parent().getMaxIter()}")

# ============================================================
# Generate Recommendations
# ============================================================
print("\n📋 Generating top-5 recommendations for all users...")

# Top 5 restaurant recommendations per user
user_recs = best_model.recommendForAllUsers(5)
print("\n  Sample recommendations (top 5 users):")
user_recs.show(5, truncate=False)

# Top 5 user recommendations per restaurant
item_recs = best_model.recommendForAllItems(5)
print("\n  Sample item-based recommendations (top 5 restaurants):")
item_recs.show(5, truncate=False)

# ============================================================
# Sample: Recommendations for specific customers
# ============================================================
print("\n📋 Detailed recommendations for sample customers:")

# Get 5 active customers
sample_customers = user_item_ratings \
    .groupBy("customer_idx") \
    .agg(F.count("*").alias("n_ratings")) \
    .filter(F.col("n_ratings") > 5) \
    .orderBy(F.desc("n_ratings")) \
    .limit(5)

sample_customer_recs = best_model.recommendForUserSubset(sample_customers, 5)

for row in sample_customer_recs.collect():
    customer = row['customer_idx']
    recs = row['recommendations']
    print(f"\n  Customer C{customer:05d}:")
    for rec in recs:
        print(f"    → Restaurant R{rec['restaurant_idx']:04d} (predicted rating: {rec['rating']:.2f})")

# ============================================================
# Save Results
# ============================================================
OUTPUT_DIR = os.path.join(_PROJECT_ROOT, "data", "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

results = {
    'task': 'Restaurant Recommendation (ALS)',
    'metrics': {
        'RMSE': round(rmse, 4),
        'MAE': round(mae, 4)
    },
    'best_params': {
        'rank': best_model.rank,
    },
    'total_users': user_item_ratings.select('customer_idx').distinct().count(),
    'total_items': user_item_ratings.select('restaurant_idx').distinct().count(),
    'total_ratings': user_item_ratings.count()
}

with open(f"{OUTPUT_DIR}/recommendation_results.json", 'w') as f:
    json.dump(results, f, indent=2)

print(f"\n💾 Results saved to: {OUTPUT_DIR}/recommendation_results.json")

# ============================================================
# Summary
# ============================================================
print("\n" + "=" * 60)
print("  RECOMMENDATION MODEL SUMMARY")
print("=" * 60)
print(f"  Algorithm:    ALS (Collaborative Filtering)")
print(f"  RMSE:         {rmse:.4f}")
print(f"  MAE:          {mae:.4f}")
print(f"  Rank:         {best_model.rank}")
print(f"  Users:        {results['total_users']:,}")
print(f"  Restaurants:  {results['total_items']:,}")
print(f"  Rating Pairs: {results['total_ratings']:,}")
print("=" * 60)

spark.stop()
print("\n✅ Recommendation Model Complete!")
