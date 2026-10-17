"""
Shared data loading utility for Spark/MLlib scripts.
Handles both HDFS (Pig-cleaned) and local (raw CSV) data sources.

When reading from local CSV, derived columns (net_revenue, time_of_day, etc.)
are computed to match the Pig-cleaned schema that HDFS data already has.
"""

import os
import sys

# Configure UTF-8 encoding for Windows compatibility
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

from pyspark.sql import functions as F

# Project root: resolve relative to this file's location
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(_THIS_DIR, ".."))

# Paths
HDFS_CLEAN_PATH = "hdfs:///fooddelivery/clean"
LOCAL_CSV_PATH = os.path.join(PROJECT_ROOT, "data", "food_delivery_data.csv")

# Column names that match the Pig-cleaned output schema (28 columns)
PIG_COLUMNS = [
    'order_id', 'customer_id', 'restaurant_id', 'cuisine', 'city', 'area',
    'order_time', 'delivery_time', 'delivery_duration_min', 'distance_km',
    'order_value', 'discount', 'net_revenue', 'payment_method',
    'delivery_partner_id', 'partner_rating', 'weather', 'traffic_level',
    'status', 'customer_rating', 'order_hour', 'order_day_of_week',
    'is_weekend', 'is_delayed', 'delivery_speed_kmph',
    'order_value_bucket', 'distance_bucket', 'time_of_day'
]


def load_data(spark, prefer_local=False):
    """
    Load the food delivery dataset.

    Attempts HDFS first (Pig-cleaned data), then falls back to local CSV.
    When reading local CSV, computes the derived columns that Pig would
    normally add, so the resulting DataFrame has a consistent schema
    regardless of source.

    Args:
        spark: SparkSession instance
        prefer_local: If True, skip HDFS and load local CSV directly

    Returns:
        tuple: (DataFrame, data_source_string)
    """
    df = None
    source = None

    # Try HDFS first unless prefer_local
    if not prefer_local:
        try:
            df = spark.read.csv(HDFS_CLEAN_PATH, header=False, inferSchema=True)
            if len(df.columns) == len(PIG_COLUMNS):
                df = df.toDF(*PIG_COLUMNS)
                source = "HDFS"
                print(f"  Loaded data from HDFS: {HDFS_CLEAN_PATH}")
            else:
                df = None  # Column count mismatch, fall through to local
        except Exception:
            df = None

    # Fall back to local CSV
    if df is None:
        if not os.path.exists(LOCAL_CSV_PATH):
            print(f"  ERROR: Data file not found at {LOCAL_CSV_PATH}")
            print(f"  Generate it first: python data/generate_data.py")
            sys.exit(1)

        df = spark.read.csv(LOCAL_CSV_PATH, header=True, inferSchema=True)
        source = "Local"
        print(f"  Loaded data from local: {LOCAL_CSV_PATH}")

        # Compute derived columns that Pig would normally add
        df = _add_derived_columns(df)

    return df, source


def _add_derived_columns(df):
    """
    Add the derived columns that the Pig ETL script normally computes.
    Only called when reading raw CSV (local fallback).
    """
    # net_revenue = order_value - discount
    if 'net_revenue' not in df.columns:
        df = df.withColumn('net_revenue',
            F.col('order_value') - F.col('discount'))

    # delivery_speed_kmph = distance_km / (delivery_duration_min / 60)
    if 'delivery_speed_kmph' not in df.columns:
        df = df.withColumn('delivery_speed_kmph',
            F.when(F.col('delivery_duration_min') > 0,
                   F.round(F.col('distance_km') / (F.col('delivery_duration_min') / 60.0), 2)
            ).otherwise(0.0))

    # order_value_bucket
    if 'order_value_bucket' not in df.columns:
        df = df.withColumn('order_value_bucket',
            F.when(F.col('order_value') < 200, 'Low')
             .when(F.col('order_value') < 500, 'Medium')
             .when(F.col('order_value') < 1000, 'High')
             .otherwise('Premium'))

    # distance_bucket
    if 'distance_bucket' not in df.columns:
        df = df.withColumn('distance_bucket',
            F.when(F.col('distance_km') < 3.0, 'Short')
             .when(F.col('distance_km') < 7.0, 'Medium')
             .when(F.col('distance_km') < 12.0, 'Long')
             .otherwise('Very Long'))

    # time_of_day
    if 'time_of_day' not in df.columns:
        df = df.withColumn('time_of_day',
            F.when((F.col('order_hour') >= 6) & (F.col('order_hour') < 11), 'Morning')
             .when((F.col('order_hour') >= 11) & (F.col('order_hour') < 14), 'Lunch')
             .when((F.col('order_hour') >= 14) & (F.col('order_hour') < 17), 'Afternoon')
             .when((F.col('order_hour') >= 17) & (F.col('order_hour') < 22), 'Evening')
             .otherwise('Night'))

    return df
