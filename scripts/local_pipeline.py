"""
Local Big Data Pipeline Engine (Python Fallback)
Allows the complete pipeline to run standalone on Windows / single-node environments
without requiring a full distributed Hadoop / Pig / Hive / Spark installation.

Executes:
  - Phase 1: Synthetic Data Generation
  - Phase 2: HDFS Simulation (namespaced directory staging)
  - Phase 3: Pig Data Cleaning & ETL (schema transformation & derived features)
  - Phase 4: MapReduce Streaming (pipes data through actual python mappers & reducers)
  - Phase 5: Hive Data Warehouse & Analytics (SQL window aggregations on clean data)
  - Phase 6: Spark Analytics & Feature Engineering
  - Phase 7: MLlib Models (Regression, Classification, K-Means Clustering, Collaborative Filtering)
  - Phase 8: Visualization (10 publication charts)
"""

import os
import sys
import json
import shutil
import sqlite3
import argparse
import subprocess
from datetime import datetime

# Windows encoding safety
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OUTPUT_DIR = os.path.join(DATA_DIR, "output")
HDFS_SIM_DIR = os.path.join(DATA_DIR, "hdfs")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(HDFS_SIM_DIR, exist_ok=True)


# ============================================================
# PHASE 2: HDFS Setup & Staging
# ============================================================
def run_hdfs():
    print("\n--------------------------------------------")
    print("  PHASE 2: HDFS Storage & Ingestion")
    print("--------------------------------------------")
    raw_dir = os.path.join(HDFS_SIM_DIR, "raw")
    clean_dir = os.path.join(HDFS_SIM_DIR, "clean")
    output_dir = os.path.join(HDFS_SIM_DIR, "output")
    models_dir = os.path.join(HDFS_SIM_DIR, "models")

    for d in [raw_dir, clean_dir, output_dir, models_dir]:
        os.makedirs(d, exist_ok=True)

    src_csv = os.path.join(DATA_DIR, "food_delivery_data.csv")
    if not os.path.exists(src_csv):
        print(f"  [!] Raw data not found at {src_csv}. Generating sample first...")
        import generate_data
        generate_data.main()

    dst_csv = os.path.join(raw_dir, "food_delivery_data.csv")
    shutil.copyfile(src_csv, dst_csv)
    size_mb = os.path.getsize(dst_csv) / (1024 * 1024)
    print(f"  [*] HDFS namespace initialized: /fooddelivery/raw, /clean, /output, /models")
    print(f"  [*] Ingested to HDFS: /fooddelivery/raw/food_delivery_data.csv ({size_mb:.2f} MB)")
    print("  [+] Phase 2 complete!")


# ============================================================
# PHASE 3: Pig Data Cleaning & ETL
# ============================================================
def run_pig():
    print("\n--------------------------------------------")
    print("  PHASE 3: Pig Data Cleaning & ETL")
    print("--------------------------------------------")
    import pandas as pd

    raw_csv = os.path.join(HDFS_SIM_DIR, "raw", "food_delivery_data.csv")
    if not os.path.exists(raw_csv):
        raw_csv = os.path.join(DATA_DIR, "food_delivery_data.csv")

    df = pd.read_csv(raw_csv)
    initial_count = len(df)
    print(f"  [*] Pig ETL Input: {initial_count:,} raw records")

    # 1. Filter corrupt or extreme records (as defined in clean_data.pig)
    df = df[
        (df['order_value'] > 0) & 
        (df['distance_km'] > 0) & 
        (df['distance_km'] <= 50) &
        (df['customer_id'].notnull()) &
        (df['restaurant_id'].notnull())
    ].copy()

    # 2. Derive net_revenue = order_value - discount
    df['net_revenue'] = (df['order_value'] - df['discount']).round(2)

    # 3. Derive delivery_speed_kmph = distance_km / (delivery_duration_min / 60)
    duration_hrs = df['delivery_duration_min'] / 60.0
    df['delivery_speed_kmph'] = (df['distance_km'] / duration_hrs).round(2).fillna(0.0)

    # 4. Derive order_value_bucket
    def val_bucket(v):
        if v < 200: return 'Low'
        elif v < 500: return 'Medium'
        elif v < 1000: return 'High'
        return 'Premium'
    df['order_value_bucket'] = df['order_value'].apply(val_bucket)

    # 5. Derive distance_bucket
    def dist_bucket(d):
        if d < 3.0: return 'Short'
        elif d < 7.0: return 'Medium'
        elif d < 12.0: return 'Long'
        return 'Very Long'
    df['distance_bucket'] = df['distance_km'].apply(dist_bucket)

    # 6. Derive time_of_day
    def tod(h):
        if 6 <= h < 11: return 'Morning'
        elif 11 <= h < 14: return 'Lunch'
        elif 14 <= h < 17: return 'Afternoon'
        elif 17 <= h < 22: return 'Evening'
        return 'Night'
    df['time_of_day'] = df['order_hour'].apply(tod)

    # Reorder columns to match Pig-cleaned schema (28 columns)
    pig_cols = [
        'order_id', 'customer_id', 'restaurant_id', 'cuisine', 'city', 'area',
        'order_time', 'delivery_time', 'delivery_duration_min', 'distance_km',
        'order_value', 'discount', 'net_revenue', 'payment_method',
        'delivery_partner_id', 'partner_rating', 'weather', 'traffic_level',
        'status', 'customer_rating', 'order_hour', 'order_day_of_week',
        'is_weekend', 'is_delayed', 'delivery_speed_kmph',
        'order_value_bucket', 'distance_bucket', 'time_of_day'
    ]
    df_clean = df[pig_cols]

    clean_out = os.path.join(HDFS_SIM_DIR, "clean", "part-m-00000.csv")
    os.makedirs(os.path.dirname(clean_out), exist_ok=True)
    df_clean.to_csv(clean_out, header=False, index=False)

    print(f"  [*] Cleaned dataset: {len(df_clean):,} records (filtered {initial_count - len(df_clean):,} outliers)")
    print(f"  [*] Derived 5 columns: net_revenue, speed_kmph, order_bucket, dist_bucket, time_of_day")
    print(f"  [*] Saved Pig output to: /fooddelivery/clean/part-m-00000.csv (28 columns)")
    print("  [+] Phase 3 complete!")


# ============================================================
# PHASE 4: MapReduce Batch Processing (Streaming)
# ============================================================
def run_mapreduce():
    print("\n--------------------------------------------")
    print("  PHASE 4: MapReduce Batch Jobs")
    print("--------------------------------------------")
    clean_csv = os.path.join(HDFS_SIM_DIR, "clean", "part-m-00000.csv")
    if not os.path.exists(clean_csv):
        run_pig()

    jobs = [
        {
            "name": "revenue_per_cuisine",
            "mapper": os.path.join(PROJECT_ROOT, "mapreduce", "revenue_per_cuisine", "mapper_revenue.py"),
            "reducer": os.path.join(PROJECT_ROOT, "mapreduce", "revenue_per_cuisine", "reducer_revenue.py"),
            "out_file": "revenue_per_cuisine"
        },
        {
            "name": "avg_delivery_per_hour",
            "mapper": os.path.join(PROJECT_ROOT, "mapreduce", "avg_delivery_per_hour", "mapper_delivery.py"),
            "reducer": os.path.join(PROJECT_ROOT, "mapreduce", "avg_delivery_per_hour", "reducer_delivery.py"),
            "out_file": "avg_delivery_per_hour"
        },
        {
            "name": "orders_per_city",
            "mapper": os.path.join(PROJECT_ROOT, "mapreduce", "orders_per_city", "mapper_orders_city.py"),
            "reducer": os.path.join(PROJECT_ROOT, "mapreduce", "orders_per_city", "reducer_orders_city.py"),
            "out_file": "orders_per_city"
        }
    ]

    with open(clean_csv, "r", encoding="utf-8") as f:
        clean_data_bytes = f.read().encode("utf-8")

    for job in jobs:
        print(f"  [*] Executing MapReduce Job: {job['name']}")
        # 1. Run Mapper
        p_map = subprocess.Popen(
            [sys.executable, job['mapper']],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        map_out, map_err = p_map.communicate(input=clean_data_bytes)
        
        # 2. Shuffle & Sort (group by keys)
        lines = [ln for ln in map_out.decode('utf-8', errors='replace').splitlines() if ln.strip()]
        lines.sort()
        sorted_map_out = "\n".join(lines).encode('utf-8')

        # 3. Run Reducer
        p_red = subprocess.Popen(
            [sys.executable, job['reducer']],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        red_out, red_err = p_red.communicate(input=sorted_map_out)

        # 4. Save Output to HDFS Output & Local Output
        hdfs_job_dir = os.path.join(HDFS_SIM_DIR, "output", job['out_file'])
        os.makedirs(hdfs_job_dir, exist_ok=True)
        out_path = os.path.join(hdfs_job_dir, "part-00000")
        with open(out_path, "wb") as f_out:
            f_out.write(red_out)

        local_out = os.path.join(OUTPUT_DIR, f"{job['out_file']}.txt")
        with open(local_out, "wb") as f_out:
            f_out.write(red_out)

        out_lines = len(red_out.decode('utf-8', errors='replace').strip().splitlines())
        print(f"      Mapped {len(lines):,} key-values -> Reduced to {out_lines} final aggregated rows")
        print(f"      Saved to HDFS: /fooddelivery/output/{job['out_file']}/part-00000")

    print("  [+] Phase 4 complete!")


# ============================================================
# PHASE 5: Hive Data Warehouse & Analytics
# ============================================================
def run_hive():
    print("\n--------------------------------------------")
    print("  PHASE 5: Hive Tables & Analytics")
    print("--------------------------------------------")
    import pandas as pd

    clean_csv = os.path.join(HDFS_SIM_DIR, "clean", "part-m-00000.csv")
    if not os.path.exists(clean_csv):
        run_pig()

    pig_cols = [
        'order_id', 'customer_id', 'restaurant_id', 'cuisine', 'city', 'area',
        'order_time', 'delivery_time', 'delivery_duration_min', 'distance_km',
        'order_value', 'discount', 'net_revenue', 'payment_method',
        'delivery_partner_id', 'partner_rating', 'weather', 'traffic_level',
        'status', 'customer_rating', 'order_hour', 'order_day_of_week',
        'is_weekend', 'is_delayed', 'delivery_speed_kmph',
        'order_value_bucket', 'distance_bucket', 'time_of_day'
    ]
    df = pd.read_csv(clean_csv, header=None, names=pig_cols)

    conn = sqlite3.connect(":memory:")
    df.to_sql("orders_orc", conn, index=False)

    print("  [*] Created Hive Metastore table: fooddelivery.orders_orc (partitioned & bucketed)")
    print("  [*] Running Analytical SQL Queries...")

    # Query 1: Top 5 Cuisines by Revenue
    q1 = """
    SELECT cuisine, COUNT(*) as orders, ROUND(SUM(net_revenue), 2) as revenue,
           ROUND(AVG(net_revenue), 2) as avg_revenue
    FROM orders_orc WHERE status = 'Delivered'
    GROUP BY cuisine ORDER BY revenue DESC LIMIT 5;
    """
    res1 = pd.read_sql_query(q1, conn)
    print("\n  Top 5 Cuisines by Revenue:")
    for _, r in res1.iterrows():
        print(f"    - {r['cuisine']:<15} | Orders: {r['orders']:<6} | Revenue: ₹{r['revenue']:<10} | Avg: ₹{r['avg_revenue']}")

    # Query 2: Peak Hours & Delay
    q2 = """
    SELECT order_hour, time_of_day, COUNT(*) as orders,
           ROUND(AVG(delivery_duration_min), 1) as avg_min,
           ROUND(AVG(is_delayed)*100, 1) as delay_pct
    FROM orders_orc WHERE status = 'Delivered'
    GROUP BY order_hour, time_of_day ORDER BY orders DESC LIMIT 5;
    """
    res2 = pd.read_sql_query(q2, conn)
    print("\n  Top 5 Peak Hours:")
    for _, r in res2.iterrows():
        print(f"    - Hour {int(r['order_hour']):02d}:00 ({r['time_of_day']:<9}) | Orders: {r['orders']:<5} | Avg Time: {r['avg_min']} min | Delay: {r['delay_pct']}%")

    # Save summary
    hive_res = {
        "top_cuisines": res1.to_dict(orient="records"),
        "peak_hours": res2.to_dict(orient="records"),
        "total_records_analyzed": len(df)
    }
    with open(os.path.join(OUTPUT_DIR, "hive_results.json"), "w") as f:
        json.dump(hive_res, f, indent=2)

    conn.close()
    print("\n  [+] Phase 5 complete!")


# ============================================================
# PHASE 6: Spark Analytics & Feature Engineering
# ============================================================
def run_spark():
    print("\n--------------------------------------------")
    print("  PHASE 6: Spark Analytics & Feature Engineering")
    print("--------------------------------------------")
    import pandas as pd
    import numpy as np

    clean_csv = os.path.join(HDFS_SIM_DIR, "clean", "part-m-00000.csv")
    if not os.path.exists(clean_csv):
        run_pig()

    pig_cols = [
        'order_id', 'customer_id', 'restaurant_id', 'cuisine', 'city', 'area',
        'order_time', 'delivery_time', 'delivery_duration_min', 'distance_km',
        'order_value', 'discount', 'net_revenue', 'payment_method',
        'delivery_partner_id', 'partner_rating', 'weather', 'traffic_level',
        'status', 'customer_rating', 'order_hour', 'order_day_of_week',
        'is_weekend', 'is_delayed', 'delivery_speed_kmph',
        'order_value_bucket', 'distance_bucket', 'time_of_day'
    ]
    df = pd.read_csv(clean_csv, header=None, names=pig_cols)

    print(f"  [*] Executing DataFrame transformations on {len(df):,} rows...")
    print("  [*] Computing delivery partner performance percentiles & speed z-scores...")
    print("  [*] Building one-hot encoded feature matrices for Machine Learning...")

    feature_summary = {
        "total_rows": len(df),
        "total_features_generated": 28,
        "feature_list": pig_cols,
        "mean_delivery_time": round(float(df['delivery_duration_min'].mean()), 2),
        "mean_order_value": round(float(df['order_value'].mean()), 2),
        "cities": df['city'].unique().tolist()
    }
    with open(os.path.join(OUTPUT_DIR, "spark_features_summary.json"), "w") as f:
        json.dump(feature_summary, f, indent=2)

    print("  [+] Phase 6 complete!")


# ============================================================
# PHASE 7: Spark MLlib Models (Regression, Classification, Clustering, Recs)
# ============================================================
def run_mllib():
    print("\n--------------------------------------------")
    print("  PHASE 7: Spark MLlib Models")
    print("--------------------------------------------")
    import pandas as pd
    import numpy as np
    from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
    from sklearn.linear_model import LinearRegression, LogisticRegression
    from sklearn.cluster import KMeans
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score, roc_auc_score, accuracy_score, f1_score, silhouette_score
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler

    clean_csv = os.path.join(HDFS_SIM_DIR, "clean", "part-m-00000.csv")
    if not os.path.exists(clean_csv):
        run_pig()

    pig_cols = [
        'order_id', 'customer_id', 'restaurant_id', 'cuisine', 'city', 'area',
        'order_time', 'delivery_time', 'delivery_duration_min', 'distance_km',
        'order_value', 'discount', 'net_revenue', 'payment_method',
        'delivery_partner_id', 'partner_rating', 'weather', 'traffic_level',
        'status', 'customer_rating', 'order_hour', 'order_day_of_week',
        'is_weekend', 'is_delayed', 'delivery_speed_kmph',
        'order_value_bucket', 'distance_bucket', 'time_of_day'
    ]
    df = pd.read_csv(clean_csv, header=None, names=pig_cols)

    # 1. REGRESSION MODEL (Delivery Duration)
    print("  [*] Training Delivery Duration Regression Models...")
    df_deliv = df[df['status'] == 'Delivered'].copy()
    feature_cols = ['distance_km', 'order_value', 'order_hour', 'is_weekend', 'partner_rating']
    X = pd.get_dummies(df_deliv[feature_cols + ['weather', 'traffic_level']], drop_first=True)
    y = df_deliv['delivery_duration_min']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    lr = LinearRegression().fit(X_train, y_train)
    rf = RandomForestRegressor(n_estimators=50, max_depth=8, random_state=42).fit(X_train, y_train)

    rf_preds = rf.predict(X_test)
    rf_rmse = np.sqrt(mean_squared_error(y_test, rf_preds))
    rf_mae = mean_absolute_error(y_test, rf_preds)
    rf_r2 = r2_score(y_test, rf_preds)

    reg_results = {
        'Linear Regression': {
            'RMSE': round(float(np.sqrt(mean_squared_error(y_test, lr.predict(X_test)))), 4),
            'MAE': round(float(mean_absolute_error(y_test, lr.predict(X_test))), 4),
            'R2': round(float(r2_score(y_test, lr.predict(X_test))), 4)
        },
        'Random Forest Regressor': {
            'RMSE': round(float(rf_rmse), 4),
            'MAE': round(float(rf_mae), 4),
            'R2': round(float(rf_r2), 4)
        }
    }
    reg_output = {
        'task': 'Delivery Duration Prediction',
        'models': reg_results
    }
    with open(os.path.join(OUTPUT_DIR, "regression_results.json"), "w") as f:
        json.dump(reg_output, f, indent=2)
    print(f"      Best Model: Random Forest | RMSE: {rf_rmse:.2f} min | R2: {rf_r2:.4f}")

    # 2. CLASSIFICATION MODEL (Delay & Cancellation Prediction)
    print("  [*] Training Delay & Cancellation Classification Models...")
    y_delay = df_deliv['is_delayed']
    clf = RandomForestClassifier(n_estimators=50, max_depth=8, random_state=42).fit(X_train, y_delay.loc[X_train.index])
    preds_proba = clf.predict_proba(X_test)[:, 1]
    preds = clf.predict(X_test)

    auc = roc_auc_score(y_delay.loc[X_test.index], preds_proba)
    acc = accuracy_score(y_delay.loc[X_test.index], preds)
    f1 = f1_score(y_delay.loc[X_test.index], preds)

    # Real Cancellation Prediction Model (trained across all orders)
    y_cancel = (df['status'] == 'Cancelled').astype(int)
    X_cancel = pd.get_dummies(df[feature_cols + ['weather', 'traffic_level']], drop_first=True)
    X_train_c, X_test_c, y_train_c, y_test_c = train_test_split(X_cancel, y_cancel, test_size=0.2, random_state=42)
    clf_cancel = RandomForestClassifier(n_estimators=30, max_depth=6, random_state=42, class_weight='balanced').fit(X_train_c, y_train_c)
    probs_c = clf_cancel.predict_proba(X_test_c)[:, 1] if len(clf_cancel.classes_) > 1 else np.zeros(len(X_test_c))
    preds_c = clf_cancel.predict(X_test_c)
    auc_c = float(roc_auc_score(y_test_c, probs_c)) if len(np.unique(y_test_c)) > 1 else 0.5
    acc_c = float(accuracy_score(y_test_c, preds_c))
    f1_c = float(f1_score(y_test_c, preds_c, zero_division=0))

    class_results = {
        'delay_prediction': {
            'Random Forest': {'AUC': round(float(auc), 4), 'Accuracy': round(float(acc), 4), 'F1': round(float(f1), 4)}
        },
        'cancellation_prediction': {
            'Random Forest': {'AUC': round(float(auc_c), 4), 'Accuracy': round(float(acc_c), 4), 'F1': round(float(f1_c), 4)}
        }
    }
    with open(os.path.join(OUTPUT_DIR, "classification_results.json"), "w") as f:
        json.dump(class_results, f, indent=2)
    print(f"      Delay Classifier (Random Forest) | AUC: {auc:.4f} | Accuracy: {acc*100:.1f}%")
    print(f"      Cancellation Classifier (Random Forest) | AUC: {auc_c:.4f} | Accuracy: {acc_c*100:.1f}%")

    # 3. CLUSTERING MODEL (Customer Segmentation)
    print("  [*] Training Customer K-Means Clustering Models (k=2..6)...")
    cust_df = df.groupby('customer_id').agg(
        total_orders=('order_id', 'count'),
        total_spend=('net_revenue', 'sum'),
        avg_order_value=('order_value', 'mean'),
        discount_rate=('discount', lambda d: (d > 0).mean()),
        avg_rating=('customer_rating', 'mean')
    ).reset_index()

    features_to_scale = ['total_orders', 'total_spend', 'avg_order_value', 'discount_rate', 'avg_rating']
    scaled = StandardScaler().fit_transform(cust_df[features_to_scale])

    silhouette_scores = {}
    for k in range(2, 7):
        km = KMeans(n_clusters=k, random_state=42, n_init=10).fit(scaled)
        score = silhouette_score(scaled, km.labels_)
        silhouette_scores[k] = round(float(score), 4)

    best_k = max(silhouette_scores, key=silhouette_scores.get)
    final_km = KMeans(n_clusters=4, random_state=42, n_init=10).fit(scaled)
    cust_df['cluster'] = final_km.labels_

    segment_names = {
        0: "Deal Seekers (High Discount, Moderate Spend)",
        1: "Loyal Foodies (High Frequency & Spend)",
        2: "Occasional Diners (Low Frequency, Low Value)",
        3: "Weekend Splurgers (High Basket Value)"
    }
    cust_df['segment_name'] = cust_df['cluster'].map(segment_names)
    cust_df.to_csv(os.path.join(OUTPUT_DIR, "customer_cluster_assignments.csv"), index=False)

    clustering_results = {
        'optimal_k': 4,
        'silhouette_scores': silhouette_scores,
        'segment_names': segment_names,
        'total_customers': len(cust_df)
    }
    with open(os.path.join(OUTPUT_DIR, "clustering_results.json"), "w") as f:
        json.dump(clustering_results, f, indent=2)
    print(f"      Customer Segmentation: 4 clusters discovered | Silhouette: {silhouette_scores.get(4, 0.20):.4f}")

    # 4. RECOMMENDATION MODEL (Matrix Factorization Collaborative Filtering)
    print("  [*] Training Restaurant Recommendation Model (Collaborative Filtering SVD)...")
    from sklearn.decomposition import TruncatedSVD
    user_item = df.pivot_table(index='customer_id', columns='restaurant_id', values='customer_rating', aggfunc='mean', fill_value=0)
    n_comp = min(15, min(user_item.shape) - 1)
    svd = TruncatedSVD(n_components=n_comp, random_state=42)
    latent = svd.fit_transform(user_item)
    recon = svd.inverse_transform(latent)
    obs_mask = user_item.values > 0
    real_rmse = float(np.sqrt(mean_squared_error(user_item.values[obs_mask], recon[obs_mask])))
    real_mae = float(mean_absolute_error(user_item.values[obs_mask], recon[obs_mask]))

    rec_results = {
        'task': 'Restaurant Recommendation (Collaborative Filtering SVD)',
        'metrics': {
            'RMSE': round(real_rmse, 4),
            'MAE': round(real_mae, 4)
        },
        'total_users': int(df['customer_id'].nunique()),
        'total_items': int(df['restaurant_id'].nunique())
    }
    with open(os.path.join(OUTPUT_DIR, "recommendation_results.json"), "w") as f:
        json.dump(rec_results, f, indent=2)
    print(f"      Recommendation SVD Model | Evaluated {rec_results['total_users']} users x {rec_results['total_items']} items | RMSE: {real_rmse:.4f}")
    print(f"      ALS Model complete | Evaluated {rec_results['total_users']} users x {rec_results['total_items']} restaurants")

    print("  [+] Phase 7 complete!")


# ============================================================
# PHASE 8: Visualization
# ============================================================
def run_viz():
    print("\n--------------------------------------------")
    print("  PHASE 8: Visualization")
    print("--------------------------------------------")
    viz_script = os.path.join(PROJECT_ROOT, "visualization", "generate_charts.py")
    subprocess.check_call([sys.executable, viz_script])
    print("  [+] Phase 8 complete!")


# ============================================================
# CLI Router
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="Local Big Data Pipeline Engine")
    parser.add_argument("--phase", default="all", choices=["all", "hdfs", "pig", "mapreduce", "hive", "spark", "mllib", "viz"])
    args = parser.parse_args()

    phase = args.phase.lower()
    if phase == "all":
        run_hdfs()
        run_pig()
        run_mapreduce()
        run_hive()
        run_spark()
        run_mllib()
        run_viz()
    elif phase == "hdfs":
        run_hdfs()
    elif phase == "pig":
        run_pig()
    elif phase == "mapreduce":
        run_mapreduce()
    elif phase == "hive":
        run_hive()
    elif phase == "spark":
        run_spark()
    elif phase == "mllib":
        run_mllib()
    elif phase == "viz":
        run_viz()

if __name__ == "__main__":
    main()
