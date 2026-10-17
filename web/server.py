"""
FeastFlow Analytics - Flask Backend Server
Handles:
- Static file serving (Web Dashboard)
- CSV Dataset Uploading & Ingestion
- Real-Time 8-Phase Big Data Pipeline Execution
- Dynamic Test Dataset Switching (3 Real Kaggle Test Sets)
- Live Log Streaming (Server-Sent Events)
- KPI & ML Metric API Endpoints
"""

import os
import sys
import json
import glob
import shutil
import time
import subprocess
from flask import Flask, request, jsonify, send_from_directory, Response, stream_with_context

# Paths
WEB_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(WEB_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OUTPUT_DIR = os.path.join(DATA_DIR, "output")
TEST_DATA_DIR = os.path.join(DATA_DIR, "test_datasets")
CHARTS_DIR = os.path.join(PROJECT_ROOT, "visualization", "charts")
WEB_CHARTS_DIR = os.path.join(WEB_DIR, "charts")

sys.path.insert(0, DATA_DIR)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "scripts"))

app = Flask(__name__, static_folder=WEB_DIR, static_url_path="")

# Ensure directories exist
os.makedirs(WEB_CHARTS_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TEST_DATA_DIR, exist_ok=True)

@app.after_request
def add_header(response):
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

def sync_charts():
    """Copy freshly generated charts into web/charts"""
    files = glob.glob(os.path.join(CHARTS_DIR, "*.png"))
    for f in files:
        shutil.copy2(f, WEB_CHARTS_DIR)
    print(f"[*] Synced {len(files)} updated charts to {WEB_CHARTS_DIR}")

def build_data_summary():
    """Build a comprehensive data_summary.json from active dataset and pipeline outputs"""
    csv_path = os.path.join(DATA_DIR, "food_delivery_data.csv")
    if not os.path.exists(csv_path):
        return {"status": "error", "message": "No active dataset found"}

    import pandas as pd
    df = pd.read_csv(csv_path)
    total_orders = len(df)
    delivered = int((df['status'] == 'Delivered').sum())
    cancelled = int((df['status'] == 'Cancelled').sum())
    avg_duration = round(float(df['delivery_duration_min'].mean()), 1)
    
    # Net revenue
    if 'net_revenue' in df.columns:
        total_rev = round(float(df['net_revenue'].sum()), 2)
    else:
        total_rev = round(float((df['order_value'] - df['discount']).sum()), 2)
        
    delay_rate = round(float((df['is_delayed'] == 1).mean() * 100), 1)

    # Sample records for Data Catalog table (first 60)
    sample_records = df.head(60).fillna('').to_dict(orient='records')

    # Read ML outputs if available
    reg_path = os.path.join(OUTPUT_DIR, "regression_results.json")
    class_path = os.path.join(OUTPUT_DIR, "classification_results.json")
    clust_path = os.path.join(OUTPUT_DIR, "clustering_results.json")
    rec_path = os.path.join(OUTPUT_DIR, "recommendation_results.json")

    reg_metrics = {}
    if os.path.exists(reg_path):
        with open(reg_path, 'r') as f:
            reg_metrics = json.load(f)

    class_metrics = {}
    if os.path.exists(class_path):
        with open(class_path, 'r') as f:
            class_metrics = json.load(f)

    clust_metrics = {}
    if os.path.exists(clust_path):
        with open(clust_path, 'r') as f:
            clust_metrics = json.load(f)

    rec_metrics = {}
    if os.path.exists(rec_path):
        with open(rec_path, 'r') as f:
            rec_metrics = json.load(f)

    # Read MapReduce revenue
    cuisine_rev = []
    rev_file = os.path.join(OUTPUT_DIR, "revenue_per_cuisine.txt")
    if os.path.exists(rev_file):
        with open(rev_file, 'r') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 4:
                    cuisine_rev.append({
                        "cuisine": parts[0],
                        "revenue": float(parts[1]),
                        "orders": int(parts[2]),
                        "avg": float(parts[3])
                    })

    # Dynamic Peak Hours Breakdown
    rev_col = 'net_revenue' if 'net_revenue' in df.columns else 'order_value'
    hourly_stats = df.groupby('order_hour').agg(
        orders=('order_id', 'count'),
        rev=(rev_col, 'sum')
    ).sort_values('orders', ascending=False).head(3).reset_index()

    hourly_breakdown = []
    for _, r in hourly_stats.iterrows():
        h = int(r['order_hour'])
        period = "Peak Dinner" if 19 <= h <= 21 else ("Late Dinner" if h >= 22 else ("Peak Lunch" if 12 <= h <= 14 else "Surge Hours"))
        rev_l = round(float(r['rev']) / 100000, 1)
        hourly_breakdown.append({
            "label": f"{h:02d}:00 - {h+1:02d}:00 ({period})",
            "orders": int(r['orders']),
            "text": f"{int(r['orders']):,} Orders (₹{rev_l}L)"
        })

    # Dynamic Top Cities Breakdown
    city_stats = df.groupby('city').agg(
        orders=('order_id', 'count'),
        avg_time=('delivery_duration_min', 'mean')
    ).sort_values('orders', ascending=False).head(4).reset_index()

    city_breakdown = []
    for _, r in city_stats.iterrows():
        city_breakdown.append({
            "city": str(r['city']),
            "orders": int(r['orders']),
            "avg_time": round(float(r['avg_time']), 1),
            "text": f"{int(r['orders']):,} Orders (Avg {float(r['avg_time']):.1f} min)"
        })

    summary = {
        "total_orders": total_orders,
        "total_revenue": total_rev,
        "avg_delivery_min": avg_duration,
        "delivered_orders": delivered,
        "cancelled_orders": cancelled,
        "delay_rate": delay_rate,
        "cuisine_revenue": sorted(cuisine_rev, key=lambda x: x['revenue'], reverse=True),
        "hourly_breakdown": hourly_breakdown,
        "city_breakdown": city_breakdown,
        "ml_regression": reg_metrics,
        "ml_classification": class_metrics,
        "ml_clustering": clust_metrics,
        "ml_recommendation": rec_metrics,
        "sample_records": sample_records,
        "timestamp": time.time()
    }

    summary_path = os.path.join(WEB_DIR, "data_summary.json")
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)

    return summary

# Routes
@app.route("/")
def index():
    return send_from_directory(WEB_DIR, "index.html")

@app.route("/<path:path>")
def static_files(path):
    return send_from_directory(WEB_DIR, path)

@app.route("/api/summary", methods=["GET"])
def get_summary():
    summary_path = os.path.join(WEB_DIR, "data_summary.json")
    if os.path.exists(summary_path):
        with open(summary_path, 'r') as f:
            return jsonify(json.load(f))
    return jsonify(build_data_summary())

@app.route("/api/test-datasets", methods=["GET"])
def get_test_datasets():
    datasets = [
        {
            "id": "swiggy_zomato_120k_orders.csv",
            "name": "🔥 Flagship: Swiggy & Zomato India",
            "description": "120,000 real orders (1.2 Lakh+) across 10 top Indian metros (Bangalore, Mumbai, Delhi, Hyderabad, Pune, etc.) covering 20 authentic cuisines.",
            "rows": 120000,
            "size": "19.1 MB",
            "focus": "1.2 Lakh Orders • 10 Metropolitan Hubs",
            "badge": "1.2 Lakh Orders"
        },
        {
            "id": "dataset_1_metro_rush.csv",
            "name": "Dataset 1: Metro Rush Hours",
            "description": "9,811 orders filtered for high-density metropolitan zones (Bangalore, Mumbai, Delhi, Jaipur) during lunch & dinner peak demand.",
            "rows": 9811,
            "size": "1.61 MB",
            "focus": "City Capacity & Peak Courier Rush",
            "badge": "High Demand"
        },
        {
            "id": "dataset_2_storm_weather.csv",
            "name": "Dataset 2: Severe Storm & Traffic Surge",
            "description": "31,883 orders featuring Stormy, Foggy, and Windy weather conditions paired with heavy road Jam bottlenecks.",
            "rows": 31883,
            "size": "5.17 MB",
            "focus": "Adverse Weather & Multi-Hour Transit Delays",
            "badge": "Severe Delay"
        },
        {
            "id": "dataset_3_quick_5k.csv",
            "name": "Dataset 3: Quick Stratified Sample (Fast Test)",
            "description": "5,000 real Kaggle orders uniformly sampled across all 22 cities. Runs the full 8-phase big data pipeline in ~10 seconds.",
            "rows": 5000,
            "size": "0.81 MB",
            "focus": "Rapid Validation & Interactive Testing",
            "badge": "Fast Test"
        }
    ]
    return jsonify(datasets)

@app.route("/download/test-dataset/<filename>")
def download_test_dataset(filename):
    return send_from_directory(TEST_DATA_DIR, filename, as_attachment=True)

@app.route("/api/run-pipeline-stream", methods=["POST", "GET"])
def run_pipeline_stream():
    """Stream live pipeline execution using Server-Sent Events (SSE)"""
    def event_stream():
        yield "data: [START] Initializing FeastFlow Big Data Pipeline...\n\n"
        pipeline_py = os.path.join(PROJECT_ROOT, "scripts", "local_pipeline.py")
        cmd = [sys.executable, pipeline_py, "--phase", "all"]
        
        proc = subprocess.Popen(
            cmd,
            cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

        for line in proc.stdout:
            clean_line = line.rstrip()
            if clean_line:
                yield f"data: {clean_line}\n\n"

        proc.wait()
        if proc.returncode == 0:
            sync_charts()
            build_data_summary()
            yield "data: [DONE] Pipeline successfully executed! Dashboard updated.\n\n"
        else:
            yield f"data: [ERROR] Pipeline failed with exit code {proc.returncode}\n\n"

    return Response(stream_with_context(event_stream()), mimetype="text/event-stream")

@app.route("/api/upload-and-run", methods=["POST"])
def upload_and_run():
    """Upload custom Kaggle CSV, ingest it, and execute pipeline"""
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "No file uploaded"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"status": "error", "message": "Empty filename"}), 400

    upload_dest = os.path.join(DATA_DIR, "uploaded_dataset.csv")
    file.save(upload_dest)

    try:
        # 1. Ingest via upload_dataset.py
        from upload_dataset import upload
        upload(file_path=upload_dest)

        # 2. Run local pipeline
        pipeline_py = os.path.join(PROJECT_ROOT, "scripts", "local_pipeline.py")
        subprocess.check_call([sys.executable, pipeline_py, "--phase", "all"], cwd=PROJECT_ROOT)

        # 3. Sync charts & refresh summary
        sync_charts()
        summary = build_data_summary()

        return jsonify({
            "status": "success",
            "message": f"Successfully ingested '{file.filename}' and executed full Big Data pipeline!",
            "summary": summary
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/load-sample-and-run", methods=["POST"])
def load_sample_and_run():
    """Load one of the 3 test datasets and execute pipeline"""
    data = request.get_json() or {}
    sample_id = data.get('sample_id', 'dataset_3_quick_5k.csv')
    sample_path = os.path.join(TEST_DATA_DIR, sample_id)

    if not os.path.exists(sample_path):
        return jsonify({"status": "error", "message": f"Sample dataset '{sample_id}' not found"}), 404

    try:
        from upload_dataset import upload
        upload(file_path=sample_path)

        pipeline_py = os.path.join(PROJECT_ROOT, "scripts", "local_pipeline.py")
        subprocess.check_call([sys.executable, pipeline_py, "--phase", "all"], cwd=PROJECT_ROOT)

        sync_charts()
        summary = build_data_summary()

        return jsonify({
            "status": "success",
            "message": f"Successfully loaded test dataset '{sample_id}' and updated all analytics!",
            "summary": summary
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    print("=" * 60)
    print("  FeastFlow Analytics Web & Pipeline Server")
    print("  Running at: http://localhost:8000")
    print("=" * 60)
    # Sync charts and build summary on startup
    sync_charts()
    build_data_summary()
    app.run(host="0.0.0.0", port=8000, debug=False)
