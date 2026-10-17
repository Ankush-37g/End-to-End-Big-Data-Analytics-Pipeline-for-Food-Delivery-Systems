"""
Spark MLlib: Delay/Cancellation Prediction (Classification)
Phase 6: Predict whether an order will be delayed or cancelled

Run: spark-submit mllib/classification_model.py

Models:
  1. Logistic Regression
  2. Decision Tree Classifier
  3. Random Forest Classifier

Reports Accuracy, F1 Score, AUC, and feature importances.
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
    StringIndexer, OneHotEncoder, VectorAssembler
)
from pyspark.ml.classification import (
    LogisticRegression, DecisionTreeClassifier, RandomForestClassifier
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# ============================================================
# Initialize Spark
# ============================================================
spark = SparkSession.builder \
    .appName("FoodDelivery-ClassificationModels") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.ansi.enabled", "false") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

print("=" * 60)
print("  MLlib: Delay/Cancellation Prediction (Classification)")
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
print(f"  Total rows: {df.count():,}")

# ============================================================
# Task A: Predict Delivery Delay (is_delayed)
# ============================================================
print("\n" + "=" * 60)
print("  TASK A: Predict Delivery Delay (Binary Classification)")
print("=" * 60)

# Filter to delivered orders only
delay_df = df.filter(F.col("status") == "Delivered")

# Check class distribution
delay_dist = delay_df.groupBy("is_delayed").count().collect()
print("\n📊 Class Distribution (is_delayed):")
for row in delay_dist:
    print(f"   is_delayed={row['is_delayed']}: {row['count']:,}")

# Target: is_delayed (already 0/1)
target_col = "is_delayed"

# Feature prep
categorical_cols = ['cuisine', 'city', 'payment_method', 'weather', 
                    'traffic_level', 'time_of_day']
numerical_cols = ['distance_km', 'order_value', 'discount', 'partner_rating',
                  'order_hour', 'is_weekend']

indexers = [
    StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep")
    for col in categorical_cols
]
encoders = [
    OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_vec")
    for col in categorical_cols
]

feature_cols = numerical_cols + [f"{col}_vec" for col in categorical_cols]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")

# Fill nulls
for col in numerical_cols:
    delay_df = delay_df.withColumn(col, F.coalesce(F.col(col).cast("double"), F.lit(0.0)))

# Ensure target is double
delay_df = delay_df.withColumn(target_col, F.col(target_col).cast("double"))

# Train/Test split
train_df, test_df = delay_df.randomSplit([0.8, 0.2], seed=42)
print(f"\n📊 Training: {train_df.count():,} | Test: {test_df.count():,}")

# Evaluators
binary_evaluator = BinaryClassificationEvaluator(
    labelCol=target_col, rawPredictionCol="rawPrediction", metricName="areaUnderROC"
)
mc_evaluator = MulticlassClassificationEvaluator(
    labelCol=target_col, predictionCol="prediction"
)

delay_results = {}

# --- Model 1: Logistic Regression ---
print("\n🔸 Model 1: Logistic Regression")
lr = LogisticRegression(
    featuresCol="features", labelCol=target_col,
    maxIter=100, regParam=0.1
)
lr_pipeline = Pipeline(stages=indexers + encoders + [assembler, lr])

lr_paramGrid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.01, 0.1, 0.5]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
    .build()

lr_cv = CrossValidator(
    estimator=lr_pipeline, estimatorParamMaps=lr_paramGrid,
    evaluator=binary_evaluator, numFolds=3, seed=42
)

print("  Training with CrossValidator...")
lr_model = lr_cv.fit(train_df)
lr_preds = lr_model.transform(test_df)

lr_auc = binary_evaluator.evaluate(lr_preds)
mc_evaluator.setMetricName("accuracy")
lr_accuracy = mc_evaluator.evaluate(lr_preds)
mc_evaluator.setMetricName("f1")
lr_f1 = mc_evaluator.evaluate(lr_preds)

print(f"  ✅ AUC:      {lr_auc:.4f}")
print(f"  ✅ Accuracy: {lr_accuracy:.4f}")
print(f"  ✅ F1:       {lr_f1:.4f}")

delay_results['Logistic Regression'] = {
    'AUC': round(lr_auc, 4), 'Accuracy': round(lr_accuracy, 4), 'F1': round(lr_f1, 4)
}

# --- Model 2: Decision Tree ---
print("\n🔸 Model 2: Decision Tree Classifier")
dt = DecisionTreeClassifier(
    featuresCol="features", labelCol=target_col,
    maxDepth=10, seed=42
)
dt_pipeline = Pipeline(stages=indexers + encoders + [assembler, dt])

dt_paramGrid = ParamGridBuilder() \
    .addGrid(dt.maxDepth, [5, 10, 15, 20]) \
    .addGrid(dt.minInstancesPerNode, [1, 5, 10]) \
    .build()

dt_cv = CrossValidator(
    estimator=dt_pipeline, estimatorParamMaps=dt_paramGrid,
    evaluator=binary_evaluator, numFolds=3, seed=42
)

print("  Training with CrossValidator...")
dt_model = dt_cv.fit(train_df)
dt_preds = dt_model.transform(test_df)

dt_auc = binary_evaluator.evaluate(dt_preds)
mc_evaluator.setMetricName("accuracy")
dt_accuracy = mc_evaluator.evaluate(dt_preds)
mc_evaluator.setMetricName("f1")
dt_f1 = mc_evaluator.evaluate(dt_preds)

print(f"  ✅ AUC:      {dt_auc:.4f}")
print(f"  ✅ Accuracy: {dt_accuracy:.4f}")
print(f"  ✅ F1:       {dt_f1:.4f}")

delay_results['Decision Tree'] = {
    'AUC': round(dt_auc, 4), 'Accuracy': round(dt_accuracy, 4), 'F1': round(dt_f1, 4)
}

# --- Model 3: Random Forest ---
print("\n🔸 Model 3: Random Forest Classifier")
rf = RandomForestClassifier(
    featuresCol="features", labelCol=target_col,
    numTrees=100, maxDepth=10, seed=42
)
rf_pipeline = Pipeline(stages=indexers + encoders + [assembler, rf])

rf_paramGrid = ParamGridBuilder() \
    .addGrid(rf.numTrees, [50, 100]) \
    .addGrid(rf.maxDepth, [5, 10, 15]) \
    .build()

rf_cv = CrossValidator(
    estimator=rf_pipeline, estimatorParamMaps=rf_paramGrid,
    evaluator=binary_evaluator, numFolds=3, seed=42
)

print("  Training with CrossValidator...")
rf_model = rf_cv.fit(train_df)
rf_preds = rf_model.transform(test_df)

rf_auc = binary_evaluator.evaluate(rf_preds)
mc_evaluator.setMetricName("accuracy")
rf_accuracy = mc_evaluator.evaluate(rf_preds)
mc_evaluator.setMetricName("f1")
rf_f1 = mc_evaluator.evaluate(rf_preds)

print(f"  ✅ AUC:      {rf_auc:.4f}")
print(f"  ✅ Accuracy: {rf_accuracy:.4f}")
print(f"  ✅ F1:       {rf_f1:.4f}")

delay_results['Random Forest'] = {
    'AUC': round(rf_auc, 4), 'Accuracy': round(rf_accuracy, 4), 'F1': round(rf_f1, 4)
}

# Feature Importances (Random Forest)
best_rf = rf_model.bestModel.stages[-1]
importances = best_rf.featureImportances.toArray()
feature_names = numerical_cols + [f"{col}_encoded" for col in categorical_cols]

print("\n  📊 Top 10 Feature Importances (Random Forest):")
importance_pairs = sorted(enumerate(importances), key=lambda x: x[1], reverse=True)
for idx, (feat_idx, imp) in enumerate(importance_pairs[:10]):
    name = feature_names[feat_idx] if feat_idx < len(feature_names) else f"feature_{feat_idx}"
    print(f"     {idx+1}. {name}: {imp:.4f}")

# ============================================================
# Task B: Predict Cancellation (status = Cancelled)
# ============================================================
print("\n" + "=" * 60)
print("  TASK B: Predict Order Cancellation")
print("=" * 60)

# Create binary target: 1 = Cancelled, 0 = Delivered
cancel_df = df.withColumn(
    "is_cancelled",
    F.when(F.col("status") == "Cancelled", 1.0).otherwise(0.0)
)

cancel_dist = cancel_df.groupBy("is_cancelled").count().collect()
print("\n📊 Class Distribution (is_cancelled):")
for row in cancel_dist:
    print(f"   is_cancelled={int(row['is_cancelled'])}: {row['count']:,}")

# Features (excluding delivery-related features that aren't available pre-delivery)
cancel_numerical = ['distance_km', 'order_value', 'discount', 'order_hour', 'is_weekend']
cancel_categorical = ['cuisine', 'city', 'payment_method', 'weather', 'traffic_level']

cancel_indexers = [
    StringIndexer(inputCol=col, outputCol=f"{col}_cidx", handleInvalid="keep")
    for col in cancel_categorical
]
cancel_encoders = [
    OneHotEncoder(inputCol=f"{col}_cidx", outputCol=f"{col}_cvec")
    for col in cancel_categorical
]

cancel_feature_cols = cancel_numerical + [f"{col}_cvec" for col in cancel_categorical]
cancel_assembler = VectorAssembler(
    inputCols=cancel_feature_cols, outputCol="cancel_features", handleInvalid="skip"
)

for col in cancel_numerical:
    cancel_df = cancel_df.withColumn(col, F.coalesce(F.col(col).cast("double"), F.lit(0.0)))

cancel_train, cancel_test = cancel_df.randomSplit([0.8, 0.2], seed=42)

# Random Forest for cancellation prediction
cancel_rf = RandomForestClassifier(
    featuresCol="cancel_features", labelCol="is_cancelled",
    numTrees=100, maxDepth=10, seed=42
)

cancel_pipeline = Pipeline(
    stages=cancel_indexers + cancel_encoders + [cancel_assembler, cancel_rf]
)

cancel_evaluator = BinaryClassificationEvaluator(
    labelCol="is_cancelled", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
)

cancel_paramGrid = ParamGridBuilder() \
    .addGrid(cancel_rf.numTrees, [50, 100]) \
    .addGrid(cancel_rf.maxDepth, [5, 10]) \
    .build()

cancel_cv = CrossValidator(
    estimator=cancel_pipeline, estimatorParamMaps=cancel_paramGrid,
    evaluator=cancel_evaluator, numFolds=3, seed=42
)

print("\n🔸 Random Forest for Cancellation Prediction")
print("  Training with CrossValidator...")
cancel_model = cancel_cv.fit(cancel_train)
cancel_preds = cancel_model.transform(cancel_test)

cancel_auc = cancel_evaluator.evaluate(cancel_preds)
mc_eval_cancel = MulticlassClassificationEvaluator(
    labelCol="is_cancelled", predictionCol="prediction"
)
mc_eval_cancel.setMetricName("accuracy")
cancel_acc = mc_eval_cancel.evaluate(cancel_preds)
mc_eval_cancel.setMetricName("f1")
cancel_f1 = mc_eval_cancel.evaluate(cancel_preds)

print(f"  ✅ AUC:      {cancel_auc:.4f}")
print(f"  ✅ Accuracy: {cancel_acc:.4f}")
print(f"  ✅ F1:       {cancel_f1:.4f}")

# Confusion matrix
print("\n  📊 Confusion Matrix:")
cancel_preds.groupBy("is_cancelled", "prediction").count().show()

# ============================================================
# Summary
# ============================================================
print("\n" + "=" * 60)
print("  CLASSIFICATION MODEL COMPARISON - DELAY PREDICTION")
print("=" * 60)
print(f"  {'Model':<25} {'AUC':>8} {'Accuracy':>10} {'F1':>8}")
print(f"  {'-'*25} {'-'*8} {'-'*10} {'-'*8}")
for model_name, metrics in delay_results.items():
    print(f"  {model_name:<25} {metrics['AUC']:>8.4f} {metrics['Accuracy']:>10.4f} {metrics['F1']:>8.4f}")
print("=" * 60)

print(f"\n  CANCELLATION PREDICTION (Random Forest)")
print(f"  AUC: {cancel_auc:.4f} | Accuracy: {cancel_acc:.4f} | F1: {cancel_f1:.4f}")

# ============================================================
# Save Results
# ============================================================
OUTPUT_FILE = os.path.join(_PROJECT_ROOT, "data", "output", "classification_results.json")
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

all_results = {
    'delay_prediction': delay_results,
    'cancellation_prediction': {
        'Random Forest': {
            'AUC': round(cancel_auc, 4),
            'Accuracy': round(cancel_acc, 4),
            'F1': round(cancel_f1, 4)
        }
    }
}

with open(OUTPUT_FILE, 'w') as f:
    json.dump(all_results, f, indent=2)
print(f"\n💾 Results saved to: {OUTPUT_FILE}")

spark.stop()
print("\n✅ Classification Models Complete!")
