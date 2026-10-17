"""
Spark Feature Engineering Script
Phase 5: Build ML-ready features from clean food delivery data

Run: spark-submit spark/feature_engineering.py

Creates customer-level and order-level features for ML models.
"""

import os
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
from pyspark.sql.window import Window
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler,
    StandardScaler, Bucketizer
)
from pyspark.ml import Pipeline

# ============================================================
# Initialize Spark Session
# ============================================================
spark = SparkSession.builder \
    .appName("FoodDelivery-FeatureEngineering") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.ansi.enabled", "false") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

print("=" * 60)
print("  Feature Engineering Pipeline")
print("=" * 60)

# ============================================================
# Load Data (uses shared loader for HDFS/local compatibility)
# ============================================================
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_THIS_DIR, ".."))
if _THIS_DIR not in sys.path:
    sys.path.insert(0, _THIS_DIR)

from data_loader import load_data

df, _data_source = load_data(spark)
df.cache()
print(f"  Total rows: {df.count():,}")

# ============================================================
# 1. Customer-Level Feature Aggregation
# ============================================================
print("\n📋 Building customer-level features...")

customer_features = df.groupBy("customer_id").agg(
    # Order behavior
    F.count("*").alias("cust_total_orders"),
    F.sum(F.when(F.col("status") == "Delivered", 1).otherwise(0)).alias("cust_delivered_orders"),
    F.sum(F.when(F.col("status") == "Cancelled", 1).otherwise(0)).alias("cust_cancelled_orders"),
    
    # Spending behavior
    F.round(F.avg("order_value"), 2).alias("cust_avg_order_value"),
    F.round(F.sum("net_revenue"), 2).alias("cust_total_spend"),
    F.round(F.stddev("order_value"), 2).alias("cust_order_value_stddev"),
    F.max("order_value").alias("cust_max_order_value"),
    
    # Discount behavior
    F.round(F.avg("discount"), 2).alias("cust_avg_discount"),
    F.sum(F.when(F.col("discount") > 0, 1).otherwise(0)).alias("cust_discount_orders"),
    
    # Delivery behavior
    F.round(F.avg("delivery_duration_min"), 2).alias("cust_avg_delivery_time"),
    F.round(F.avg("distance_km"), 2).alias("cust_avg_distance"),
    
    # Ratings
    F.round(F.avg("customer_rating"), 2).alias("cust_avg_rating"),
    
    # Time patterns
    F.sum(F.when(F.col("is_weekend") == 1, 1).otherwise(0)).alias("cust_weekend_orders"),
    F.round(F.avg("order_hour"), 2).alias("cust_avg_order_hour"),
    
    # Cuisine diversity
    F.countDistinct("cuisine").alias("cust_cuisine_diversity"),
    F.countDistinct("restaurant_id").alias("cust_restaurant_diversity"),
    
    # Payment preference
    F.first(F.col("payment_method")).alias("cust_primary_payment"),
    F.first(F.col("city")).alias("cust_city")
)

# Derived customer features
customer_features = customer_features.withColumns({
    "cust_cancellation_ratio": F.round(
        F.col("cust_cancelled_orders") / F.col("cust_total_orders"), 4
    ),
    "cust_weekend_ratio": F.round(
        F.col("cust_weekend_orders") / F.col("cust_total_orders"), 4
    ),
    "cust_discount_ratio": F.round(
        F.col("cust_discount_orders") / F.col("cust_total_orders"), 4
    )
})

print(f"✅ Customer features: {customer_features.count():,} customers")
customer_features.printSchema()
customer_features.show(5, truncate=False)

# ============================================================
# 2. Restaurant-Level Feature Aggregation
# ============================================================
print("\n📋 Building restaurant-level features...")

restaurant_features = df.filter(F.col("status") == "Delivered").groupBy("restaurant_id").agg(
    F.count("*").alias("rest_total_orders"),
    F.round(F.avg("delivery_duration_min"), 2).alias("rest_avg_delivery_time"),
    F.round(F.avg("customer_rating"), 2).alias("rest_avg_rating"),
    F.round(F.avg("net_revenue"), 2).alias("rest_avg_order_value"),
    F.sum("net_revenue").alias("rest_total_revenue"),
    F.round(F.avg("distance_km"), 2).alias("rest_avg_distance"),
    F.sum(F.when(F.col("is_delayed") == 1, 1).otherwise(0)).alias("rest_delayed_orders")
)

restaurant_features = restaurant_features.withColumn(
    "rest_delay_rate",
    F.round(F.col("rest_delayed_orders") / F.col("rest_total_orders"), 4)
)

print(f"✅ Restaurant features: {restaurant_features.count():,} restaurants")

# ============================================================
# 3. Order-Level Feature Engineering (for ML)
# ============================================================
print("\n📋 Building order-level ML features...")

# Filter to delivered orders only (for regression/classification)
ml_df = df.filter(F.col("status") == "Delivered")

# Join customer and restaurant features
ml_df = ml_df \
    .join(customer_features.select(
        "customer_id", "cust_total_orders", "cust_avg_order_value",
        "cust_cancellation_ratio", "cust_avg_delivery_time",
        "cust_avg_rating", "cust_cuisine_diversity"
    ), on="customer_id", how="left") \
    .join(restaurant_features.select(
        "restaurant_id", "rest_total_orders", "rest_avg_delivery_time",
        "rest_avg_rating", "rest_delay_rate"
    ), on="restaurant_id", how="left")

# ============================================================
# 4. Encode Categorical Variables
# ============================================================
print("\n📋 Encoding categorical variables...")

# Categorical columns to encode
categorical_cols = ['cuisine', 'city', 'payment_method', 'weather', 
                    'traffic_level', 'time_of_day', 'order_day_of_week']

# Build indexers and encoders
indexers = [
    StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep")
    for col in categorical_cols
]

encoders = [
    OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_vec")
    for col in categorical_cols
]

# ============================================================
# 5. Assemble Feature Vector
# ============================================================
print("\n📋 Assembling feature vector...")

# Numerical features
numerical_features = [
    'distance_km', 'order_value', 'discount', 'partner_rating',
    'order_hour', 'is_weekend',
    'cust_total_orders', 'cust_avg_order_value', 'cust_cancellation_ratio',
    'cust_avg_delivery_time', 'cust_avg_rating', 'cust_cuisine_diversity',
    'rest_total_orders', 'rest_avg_delivery_time', 'rest_avg_rating',
    'rest_delay_rate'
]

# Encoded categorical features
encoded_features = [f"{col}_vec" for col in categorical_cols]

# All features
all_features = numerical_features + encoded_features

# Vector assembler
assembler = VectorAssembler(
    inputCols=all_features,
    outputCol="features_raw",
    handleInvalid="skip"
)

# Standard scaler
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=False
)

# ============================================================
# 6. Build and Run Pipeline
# ============================================================
print("\n📋 Building ML pipeline...")

pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler])

# Fill nulls in numerical columns
for col in numerical_features:
    ml_df = ml_df.withColumn(col, F.coalesce(F.col(col), F.lit(0)))

# Fit and transform
pipeline_model = pipeline.fit(ml_df)
ml_ready = pipeline_model.transform(ml_df)

print(f"✅ ML-ready dataset: {ml_ready.count():,} rows")
print(f"📋 Feature vector size: {ml_ready.select('features').first()[0].size}")

# ============================================================
# 7. Save Engineered Features
# ============================================================
print("\n💾 Saving engineered features...")

OUTPUT_BASE = "hdfs:///fooddelivery/output/features"
LOCAL_OUTPUT = os.path.join(_PROJECT_ROOT, "data", "output", "features")

try:
    # Save ML-ready data
    ml_ready.select(
        "order_id", "customer_id", "restaurant_id",
        "delivery_duration_min", "is_delayed", "features"
    ).write.mode("overwrite").parquet(f"{OUTPUT_BASE}/ml_ready")
    
    # Save customer features
    customer_features.write.mode("overwrite") \
        .parquet(f"{OUTPUT_BASE}/customer_features")
    
    # Save restaurant features
    restaurant_features.write.mode("overwrite") \
        .parquet(f"{OUTPUT_BASE}/restaurant_features")
    
    # Save pipeline model
    pipeline_model.write().overwrite().save(f"{OUTPUT_BASE}/pipeline_model")
    
    print(f"✅ Features saved to HDFS: {OUTPUT_BASE}/")
except Exception:
    os.makedirs(LOCAL_OUTPUT, exist_ok=True)
    
    customer_features.toPandas().to_csv(
        f"{LOCAL_OUTPUT}/customer_features.csv", index=False)
    restaurant_features.toPandas().to_csv(
        f"{LOCAL_OUTPUT}/restaurant_features.csv", index=False)
    
    print(f"✅ Features saved locally: {LOCAL_OUTPUT}/")

# ============================================================
# Summary
# ============================================================
print("\n" + "=" * 60)
print("  Feature Engineering Summary")
print("=" * 60)
print(f"  Customer features:    {len(customer_features.columns)} columns")
print(f"  Restaurant features:  {len(restaurant_features.columns)} columns")
print(f"  ML feature vector:    {ml_ready.select('features').first()[0].size} dimensions")
print(f"  Categorical encoded:  {len(categorical_cols)} columns")
print(f"  Numerical features:   {len(numerical_features)} columns")
print(f"  Total ML-ready rows:  {ml_ready.count():,}")
print("=" * 60)

spark.stop()
print("\n✅ Feature Engineering Complete!")
