"""
Spark MLlib: Delivery Time Prediction (Regression)
Phase 6: Predict delivery_duration_min using multiple regression algorithms

Run: spark-submit mllib/regression_model.py

Models:
  1. Linear Regression
  2. Random Forest Regressor
  3. Gradient Boosted Trees (GBT) Regressor

Uses Pipeline, VectorAssembler, train/test split, and CrossValidator.
Reports RMSE, R², MAE, and feature importances.
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
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
)
from pyspark.ml.regression import (
    LinearRegression, RandomForestRegressor, GBTRegressor
)
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# ============================================================
# Initialize Spark
# ============================================================
spark = SparkSession.builder \
    .appName("FoodDelivery-RegressionModels") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.ansi.enabled", "false") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

print("=" * 60)
print("  MLlib: Delivery Time Prediction (Regression)")
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

# Filter to delivered orders only
df = df.filter(
    (F.col("status") == "Delivered") & 
    (F.col("delivery_duration_min") > 0) &
    (F.col("delivery_duration_min") < 180)
)

print(f"  Delivered orders: {df.count():,}")

# ============================================================
# Feature Preparation
# ============================================================
print("\n📋 Preparing features...")

# Target variable
target_col = "delivery_duration_min"

# Categorical features to encode
categorical_cols = ['cuisine', 'city', 'payment_method', 'weather', 
                    'traffic_level', 'time_of_day']

# Numerical features
numerical_cols = ['distance_km', 'order_value', 'discount', 'partner_rating',
                  'order_hour', 'is_weekend']

# Build encoding pipeline
indexers = [
    StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep")
    for col in categorical_cols
]

encoders = [
    OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_vec")
    for col in categorical_cols
]

# Feature columns
feature_cols = numerical_cols + [f"{col}_vec" for col in categorical_cols]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features",
    handleInvalid="skip"
)

# Fill nulls
for col in numerical_cols:
    df = df.withColumn(col, F.coalesce(F.col(col).cast("double"), F.lit(0.0)))

# ============================================================
# Train/Test Split
# ============================================================
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"📊 Training set: {train_df.count():,} rows")
print(f"📊 Test set:     {test_df.count():,} rows")

# ============================================================
# Model 1: Linear Regression
# ============================================================
print("\n" + "=" * 60)
print("🔸 Model 1: Linear Regression")
print("=" * 60)

lr = LinearRegression(
    featuresCol="features",
    labelCol=target_col,
    maxIter=100,
    regParam=0.1,
    elasticNetParam=0.8
)

lr_pipeline = Pipeline(stages=indexers + encoders + [assembler, lr])

# Cross-validation
lr_paramGrid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.01, 0.1, 0.5]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
    .build()

lr_evaluator = RegressionEvaluator(
    labelCol=target_col, predictionCol="prediction", metricName="rmse"
)

lr_cv = CrossValidator(
    estimator=lr_pipeline,
    estimatorParamMaps=lr_paramGrid,
    evaluator=lr_evaluator,
    numFolds=3,
    seed=42
)

print("  Training with CrossValidator (3 folds)...")
lr_model = lr_cv.fit(train_df)
lr_predictions = lr_model.transform(test_df)

# Evaluate
lr_rmse = lr_evaluator.evaluate(lr_predictions)
lr_evaluator.setMetricName("r2")
lr_r2 = lr_evaluator.evaluate(lr_predictions)
lr_evaluator.setMetricName("mae")
lr_mae = lr_evaluator.evaluate(lr_predictions)
lr_evaluator.setMetricName("rmse")  # Reset

print(f"  ✅ RMSE:  {lr_rmse:.4f}")
print(f"  ✅ R²:    {lr_r2:.4f}")
print(f"  ✅ MAE:   {lr_mae:.4f}")

# Coefficients
best_lr = lr_model.bestModel.stages[-1]
print(f"  📊 Intercept: {best_lr.intercept:.4f}")
print(f"  📊 Number of features: {best_lr.numFeatures}")

# ============================================================
# Model 2: Random Forest Regressor
# ============================================================
print("\n" + "=" * 60)
print("🔸 Model 2: Random Forest Regressor")
print("=" * 60)

rf = RandomForestRegressor(
    featuresCol="features",
    labelCol=target_col,
    numTrees=100,
    maxDepth=10,
    seed=42
)

rf_pipeline = Pipeline(stages=indexers + encoders + [assembler, rf])

# Cross-validation with parameter tuning
rf_paramGrid = ParamGridBuilder() \
    .addGrid(rf.numTrees, [50, 100]) \
    .addGrid(rf.maxDepth, [5, 10, 15]) \
    .build()

rf_cv = CrossValidator(
    estimator=rf_pipeline,
    estimatorParamMaps=rf_paramGrid,
    evaluator=lr_evaluator,
    numFolds=3,
    seed=42
)

print("  Training with CrossValidator (3 folds)...")
rf_model = rf_cv.fit(train_df)
rf_predictions = rf_model.transform(test_df)

# Evaluate
rf_rmse = lr_evaluator.evaluate(rf_predictions)
lr_evaluator.setMetricName("r2")
rf_r2 = lr_evaluator.evaluate(rf_predictions)
lr_evaluator.setMetricName("mae")
rf_mae = lr_evaluator.evaluate(rf_predictions)
lr_evaluator.setMetricName("rmse")

print(f"  ✅ RMSE:  {rf_rmse:.4f}")
print(f"  ✅ R²:    {rf_r2:.4f}")
print(f"  ✅ MAE:   {rf_mae:.4f}")

# Feature Importances
best_rf = rf_model.bestModel.stages[-1]
importances = best_rf.featureImportances.toArray()

# Map feature indices to names
feature_names = numerical_cols.copy()
for col in categorical_cols:
    # Get the number of categories from the indexer
    feature_names.append(f"{col}_encoded")

print("\n  📊 Top 10 Feature Importances (Random Forest):")
importance_pairs = list(zip(range(len(importances)), importances))
importance_pairs.sort(key=lambda x: x[1], reverse=True)
for idx, (feat_idx, imp) in enumerate(importance_pairs[:10]):
    feat_name = feature_names[feat_idx] if feat_idx < len(feature_names) else f"feature_{feat_idx}"
    print(f"     {idx+1}. {feat_name}: {imp:.4f}")

# ============================================================
# Model 3: Gradient Boosted Trees Regressor
# ============================================================
print("\n" + "=" * 60)
print("🔸 Model 3: Gradient Boosted Trees (GBT) Regressor")
print("=" * 60)

gbt = GBTRegressor(
    featuresCol="features",
    labelCol=target_col,
    maxIter=50,
    maxDepth=5,
    seed=42
)

gbt_pipeline = Pipeline(stages=indexers + encoders + [assembler, gbt])

gbt_paramGrid = ParamGridBuilder() \
    .addGrid(gbt.maxIter, [30, 50]) \
    .addGrid(gbt.maxDepth, [3, 5, 7]) \
    .build()

gbt_cv = CrossValidator(
    estimator=gbt_pipeline,
    estimatorParamMaps=gbt_paramGrid,
    evaluator=lr_evaluator,
    numFolds=3,
    seed=42
)

print("  Training with CrossValidator (3 folds)...")
gbt_model = gbt_cv.fit(train_df)
gbt_predictions = gbt_model.transform(test_df)

# Evaluate
gbt_rmse = lr_evaluator.evaluate(gbt_predictions)
lr_evaluator.setMetricName("r2")
gbt_r2 = lr_evaluator.evaluate(gbt_predictions)
lr_evaluator.setMetricName("mae")
gbt_mae = lr_evaluator.evaluate(gbt_predictions)

print(f"  ✅ RMSE:  {gbt_rmse:.4f}")
print(f"  ✅ R²:    {gbt_r2:.4f}")
print(f"  ✅ MAE:   {gbt_mae:.4f}")

# GBT Feature Importances
best_gbt = gbt_model.bestModel.stages[-1]
gbt_importances = best_gbt.featureImportances.toArray()
gbt_pairs = list(zip(range(len(gbt_importances)), gbt_importances))
gbt_pairs.sort(key=lambda x: x[1], reverse=True)
print("\n  📊 Top 10 Feature Importances (GBT):")
for idx, (feat_idx, imp) in enumerate(gbt_pairs[:10]):
    feat_name = feature_names[feat_idx] if feat_idx < len(feature_names) else f"feature_{feat_idx}"
    print(f"     {idx+1}. {feat_name}: {imp:.4f}")

# ============================================================
# Model Comparison Summary
# ============================================================
print("\n" + "=" * 60)
print("  REGRESSION MODEL COMPARISON")
print("=" * 60)
print(f"  {'Model':<25} {'RMSE':>8} {'R²':>8} {'MAE':>8}")
print(f"  {'-'*25} {'-'*8} {'-'*8} {'-'*8}")
print(f"  {'Linear Regression':<25} {lr_rmse:>8.4f} {lr_r2:>8.4f} {lr_mae:>8.4f}")
print(f"  {'Random Forest':<25} {rf_rmse:>8.4f} {rf_r2:>8.4f} {rf_mae:>8.4f}")
print(f"  {'GBT Regressor':<25} {gbt_rmse:>8.4f} {gbt_r2:>8.4f} {gbt_mae:>8.4f}")
print("=" * 60)

# Determine best model
best_model_name = min(
    [('Linear Regression', lr_rmse), ('Random Forest', rf_rmse), ('GBT', gbt_rmse)],
    key=lambda x: x[1]
)[0]
print(f"\n🏆 Best Model: {best_model_name}")

# ============================================================
# Save Results
# ============================================================
OUTPUT_FILE = os.path.join(_PROJECT_ROOT, "data", "output", "regression_results.json")
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

results = {
    'task': 'Delivery Time Prediction (Regression)',
    'target': 'delivery_duration_min',
    'train_size': train_df.count(),
    'test_size': test_df.count(),
    'models': {
        'Linear Regression': {'RMSE': round(lr_rmse, 4), 'R2': round(lr_r2, 4), 'MAE': round(lr_mae, 4)},
        'Random Forest': {'RMSE': round(rf_rmse, 4), 'R2': round(rf_r2, 4), 'MAE': round(rf_mae, 4)},
        'GBT Regressor': {'RMSE': round(gbt_rmse, 4), 'R2': round(gbt_r2, 4), 'MAE': round(gbt_mae, 4)}
    },
    'best_model': best_model_name
}

with open(OUTPUT_FILE, 'w') as f:
    json.dump(results, f, indent=2)
print(f"\n💾 Results saved to: {OUTPUT_FILE}")

# Save sample predictions
sample_preds = rf_predictions.select(
    "order_id", target_col, "prediction", "distance_km", "weather", "traffic_level"
).limit(100).toPandas()
sample_preds.to_csv(os.path.join(_PROJECT_ROOT, "data", "output", "regression_predictions_sample.csv"), index=False)

spark.stop()
print("\n✅ Regression Models Complete!")
