"""
Spark MLlib: Customer Segmentation (Clustering)
Phase 6: Segment customers by ordering behavior using K-Means

Run: spark-submit mllib/clustering_model.py

Uses K-Means clustering with silhouette score optimization.
Identifies distinct customer segments for business insights.
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
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# ============================================================
# Initialize Spark
# ============================================================
spark = SparkSession.builder \
    .appName("FoodDelivery-CustomerClustering") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.ansi.enabled", "false") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

print("=" * 60)
print("  MLlib: Customer Segmentation (K-Means Clustering)")
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
print(f"  Total orders: {df.count():,}")

# ============================================================
# Build Customer-Level Features for Clustering
# ============================================================
print("\n📋 Building customer feature profiles...")

customer_df = df.groupBy("customer_id").agg(
    # Order frequency
    F.count("*").alias("total_orders"),
    F.sum(F.when(F.col("status") == "Delivered", 1).otherwise(0)).alias("delivered_orders"),
    F.sum(F.when(F.col("status") == "Cancelled", 1).otherwise(0)).alias("cancelled_orders"),
    
    # Spending behavior
    F.round(F.avg("order_value"), 2).alias("avg_order_value"),
    F.round(F.sum("net_revenue"), 2).alias("total_spend"),
    F.round(F.max("order_value"), 2).alias("max_order_value"),
    
    # Discount affinity
    F.round(F.avg("discount"), 2).alias("avg_discount"),
    F.sum(F.when(F.col("discount") > 0, 1).otherwise(0)).alias("discount_orders"),
    
    # Ordering patterns
    F.round(F.avg("order_hour"), 2).alias("avg_order_hour"),
    F.sum(F.when(F.col("is_weekend") == 1, 1).otherwise(0)).alias("weekend_orders"),
    
    # Diversity
    F.countDistinct("cuisine").alias("cuisine_diversity"),
    F.countDistinct("restaurant_id").alias("restaurant_diversity"),
    
    # Satisfaction
    F.round(F.avg("customer_rating"), 2).alias("avg_rating"),
    F.round(F.avg("distance_km"), 2).alias("avg_distance")
)

# Derived features
customer_df = customer_df.withColumns({
    "cancellation_rate": F.round(
        F.when(F.col("total_orders") > 0, F.col("cancelled_orders") / F.col("total_orders")).otherwise(0.0), 4
    ),
    "weekend_ratio": F.round(
        F.when(F.col("total_orders") > 0, F.col("weekend_orders") / F.col("total_orders")).otherwise(0.0), 4
    ),
    "discount_usage_rate": F.round(
        F.when(F.col("total_orders") > 0, F.col("discount_orders") / F.col("total_orders")).otherwise(0.0), 4
    ),
    "avg_spend_per_order": F.round(
        F.when(F.col("delivered_orders") > 0, F.col("total_spend") / F.col("delivered_orders")).otherwise(0.0), 2
    )
})

# Fill nulls
customer_df = customer_df.fillna(0)

print(f"✅ Customers profiled: {customer_df.count():,}")
customer_df.describe().show()

# ============================================================
# Feature Assembly for Clustering
# ============================================================
clustering_features = [
    'total_orders', 'avg_order_value', 'total_spend',
    'avg_discount', 'discount_usage_rate',
    'avg_order_hour', 'weekend_ratio',
    'cuisine_diversity', 'restaurant_diversity',
    'avg_rating', 'avg_distance', 'cancellation_rate'
]

assembler = VectorAssembler(
    inputCols=clustering_features,
    outputCol="features_raw",
    handleInvalid="skip"
)

scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=True
)

# Assemble and scale
feature_pipeline = Pipeline(stages=[assembler, scaler])
feature_model = feature_pipeline.fit(customer_df)
scaled_df = feature_model.transform(customer_df)

# ============================================================
# Find Optimal K using Silhouette Score (Elbow Method)
# ============================================================
print("\n📋 Finding optimal number of clusters...")
print(f"  Testing k = 2 to 8...\n")

evaluator = ClusteringEvaluator(
    predictionCol="prediction",
    featuresCol="features",
    metricName="silhouette"
)

silhouette_scores = {}
k_range = range(2, 9)

for k in k_range:
    kmeans = KMeans(
        featuresCol="features",
        predictionCol="prediction",
        k=k,
        seed=42,
        maxIter=100
    )
    model = kmeans.fit(scaled_df)
    predictions = model.transform(scaled_df)
    score = evaluator.evaluate(predictions)
    silhouette_scores[k] = round(score, 4)
    
    # Cluster sizes
    sizes = predictions.groupBy("prediction").count().orderBy("prediction").collect()
    size_str = ", ".join([f"C{row['prediction']}:{row['count']}" for row in sizes])
    
    print(f"  k={k}: Silhouette={score:.4f} | Sizes: {size_str}")

# Find best k
best_k = max(silhouette_scores, key=silhouette_scores.get)
print(f"\n🏆 Optimal k = {best_k} (Silhouette = {silhouette_scores[best_k]:.4f})")

# ============================================================
# Final Clustering with Best K
# ============================================================
print(f"\n📋 Running final K-Means with k={best_k}...")

final_kmeans = KMeans(
    featuresCol="features",
    predictionCol="cluster",
    k=best_k,
    seed=42,
    maxIter=200
)

final_model = final_kmeans.fit(scaled_df)
clustered_df = final_model.transform(scaled_df)

# ============================================================
# Cluster Analysis - Profile Each Segment
# ============================================================
print("\n" + "=" * 60)
print("  CUSTOMER SEGMENT PROFILES")
print("=" * 60)

segment_profiles = clustered_df.groupBy("cluster").agg(
    F.count("*").alias("customer_count"),
    F.round(F.avg("total_orders"), 1).alias("avg_orders"),
    F.round(F.avg("avg_order_value"), 0).alias("avg_order_value"),
    F.round(F.avg("total_spend"), 0).alias("avg_total_spend"),
    F.round(F.avg("discount_usage_rate"), 3).alias("avg_discount_rate"),
    F.round(F.avg("weekend_ratio"), 3).alias("avg_weekend_ratio"),
    F.round(F.avg("cuisine_diversity"), 1).alias("avg_cuisine_diversity"),
    F.round(F.avg("avg_rating"), 2).alias("avg_rating"),
    F.round(F.avg("cancellation_rate"), 4).alias("avg_cancel_rate"),
    F.round(F.avg("avg_order_hour"), 1).alias("avg_order_hour"),
    F.round(F.avg("avg_distance"), 1).alias("avg_distance")
).orderBy("cluster")

segment_profiles.show(truncate=False)

# ============================================================
# Segment Naming (based on characteristics)
# ============================================================
print("\n📋 Segment Descriptions:")
profiles = segment_profiles.collect()

segment_names = {}
for row in profiles:
    cluster_id = row['cluster']
    orders = row['avg_orders']
    spend = row['avg_total_spend']
    discount = row['avg_discount_rate']
    
    # Heuristic naming
    if orders > 5 and spend > 3000:
        name = "Loyal High-Value"
        desc = "Frequent orderers with high spending. Key retention targets."
    elif orders > 3 and discount > 0.4:
        name = "Deal Seekers"
        desc = "Regular customers who primarily order with discounts."
    elif orders <= 2 and spend < 1000:
        name = "Casual/New"
        desc = "Infrequent orderers, possibly new. Need engagement campaigns."
    elif orders > 3 and spend > 1500:
        name = "Regular Spenders"
        desc = "Moderate frequency with decent spend. Growth potential."
    else:
        name = f"Segment {cluster_id}"
        desc = "Mixed behavior segment."
    
    segment_names[cluster_id] = {'name': name, 'description': desc}
    print(f"\n  Cluster {cluster_id}: {name}")
    print(f"    {desc}")
    print(f"    Customers: {row['customer_count']:,}")
    print(f"    Avg Orders: {orders:.1f} | Avg Spend: ₹{spend:,.0f}")
    print(f"    Discount Rate: {discount:.1%} | Cancel Rate: {row['avg_cancel_rate']:.2%}")
    print(f"    Avg Rating: {row['avg_rating']:.2f}")

# ============================================================
# Cluster Centers
# ============================================================
print("\n📋 Cluster Centers (standardized features):")
centers = final_model.clusterCenters()
for i, center in enumerate(centers):
    print(f"\n  Cluster {i}: {[round(v, 3) for v in center[:6]]}...")

# ============================================================
# Save Results
# ============================================================
print("\n💾 Saving results...")
OUTPUT_DIR = os.path.join(_PROJECT_ROOT, "data", "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Save segment profiles
segment_profiles.toPandas().to_csv(f"{OUTPUT_DIR}/customer_segments.csv", index=False)

# Save customer assignments
clustered_df.select(
    "customer_id", "cluster", "total_orders", "avg_order_value",
    "total_spend", "discount_usage_rate", "avg_rating", "cancellation_rate"
).toPandas().to_csv(f"{OUTPUT_DIR}/customer_cluster_assignments.csv", index=False)

# Save clustering results
clustering_results = {
    'task': 'Customer Segmentation (K-Means)',
    'optimal_k': best_k,
    'silhouette_scores': silhouette_scores,
    'best_silhouette': silhouette_scores[best_k],
    'segment_names': segment_names,
    'features_used': clustering_features,
    'total_customers': customer_df.count()
}

with open(f"{OUTPUT_DIR}/clustering_results.json", 'w') as f:
    json.dump(clustering_results, f, indent=2, default=str)

print(f"✅ Results saved to: {OUTPUT_DIR}/")

# ============================================================
# Summary
# ============================================================
print("\n" + "=" * 60)
print("  CLUSTERING SUMMARY")
print("=" * 60)
print(f"  Algorithm:       K-Means")
print(f"  Optimal k:       {best_k}")
print(f"  Silhouette:      {silhouette_scores[best_k]:.4f}")
print(f"  Total Customers: {customer_df.count():,}")
print(f"  Features Used:   {len(clustering_features)}")
print(f"\n  Silhouette Scores by k:")
for k, score in silhouette_scores.items():
    marker = " ← BEST" if k == best_k else ""
    print(f"    k={k}: {score:.4f}{marker}")
print("=" * 60)

spark.stop()
print("\n✅ Customer Segmentation Complete!")
