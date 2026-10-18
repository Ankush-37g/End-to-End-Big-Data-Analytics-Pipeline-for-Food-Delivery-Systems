# End-to-End Big Data Analytics Pipeline for Food Delivery Systems

[![Hadoop](https://img.shields.io/badge/Hadoop-3.2.1-FFD000?logo=apachehadoop&logoColor=black)](https://hadoop.apache.org/)
[![Spark](https://img.shields.io/badge/Apache_Spark-3.x-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Hive](https://img.shields.io/badge/Apache_Hive-3.1.2-FDEE21?logo=apachehive&logoColor=black)](https://hive.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.8%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green)](LICENSE)

An end-to-end big data pipeline that stores, processes, analyzes, and predicts outcomes from food delivery data using **Hadoop (HDFS, MapReduce), Pig, Hive, Spark, and Spark MLlib**.

---

## Business Questions

1. Which cuisines, restaurants, and areas generate the most revenue?
2. What are the peak order hours and days?
3. What factors drive delivery delays?
4. Can we predict delivery time (regression) and delay/cancellation (classification)?
5. Can we segment customers by ordering behavior (clustering)?

---

## Architecture

```
Raw CSV → HDFS (raw zone)
            ├── MapReduce  (batch aggregations)
            ├── Pig        (cleaning / ETL) → HDFS (clean zone)
            ├── Hive       (SQL analytics on clean data)
            └── Spark      (fast processing)
                           └── Spark MLlib (ML models)
                               └── Results → charts + dashboard
```

---

## Project Structure

```
food-analytics/
├── data/
│   ├── generate_data.py          # Synthetic data generator (Faker)
│   └── sample/                   # Small sample for testing
├── hdfs/
│   ├── setup_hdfs.sh             # HDFS directory setup
│   └── ingest.sh                 # Data ingestion
├── pig/
│   ├── clean_data.pig            # Data cleaning & ETL
│   └── derive_fields.pig         # Feature derivation
├── mapreduce/
│   ├── orders_per_city/          # Orders count per city/area
│   ├── revenue_per_cuisine/      # Revenue aggregation by cuisine
│   └── avg_delivery_per_hour/    # Average delivery time per hour
├── hive/
│   ├── create_tables.hql         # Table creation (external, partitioned, ORC)
│   ├── analytics_queries.hql     # Analytical SQL queries
│   └── performance_compare.hql   # Text vs ORC performance benchmarks
├── spark/
│   ├── spark_analytics.py        # PySpark analytics
│   ├── feature_engineering.py    # Feature engineering pipeline
│   └── performance_compare.py    # Runtime comparison: MR vs Hive vs Spark
├── mllib/
│   ├── regression_model.py       # Delivery time prediction
│   ├── classification_model.py   # Delay / cancellation prediction
│   ├── clustering_model.py       # Customer segmentation (K-Means)
│   └── recommendation_model.py   # ALS recommendations
├── visualization/
│   ├── generate_charts.py        # Chart generation (Matplotlib / Seaborn)
│   └── charts/                   # 10 publication-quality PNG charts
├── scripts/
│   ├── run_pipeline.sh           # Linux/Mac pipeline runner
│   ├── run_pipeline.ps1          # Windows PowerShell runner
│   ├── local_pipeline.py         # Standalone Python fallback (no Hadoop needed)
│   └── environment_setup.sh      # Environment setup script
├── docs/
│   ├── ACADEMIC_PROJECT_REPORT_PHASE_1_TO_6.md
│   ├── report_template.md
│   ├── presentation_slides.md
│   ├── viva_questions_and_answers.md
│   └── architecture_diagram.md
├── web/
│   ├── index.html                # FeastFlow Dashboard UI
│   ├── style.css
│   ├── app.js
│   └── server.py                 # Flask backend server
├── docker-compose.yml            # Multi-container Hadoop & Spark cluster
├── Makefile                      # Build automation
├── requirements.txt              # Python dependencies
└── README.md
```

---

## Dataset

- **Size**: 2,000,000 rows (~500 MB CSV)
- **Cities**: 10 Indian cities (Bangalore, Mumbai, Delhi, Hyderabad, etc.)
- **Entities**: 500 restaurants, 50,000 customers, 2,000 delivery riders
- **Period**: January 2025 – December 2025

| Column | Type | Description |
|--------|------|-------------|
| `order_id` | INT | Unique order identifier |
| `customer_id` | STRING | Customer ID |
| `restaurant_id` | STRING | Restaurant ID |
| `cuisine` | STRING | Cuisine type (20 categories) |
| `city` | STRING | City (partition key) |
| `order_value` | FLOAT | Order amount (₹) |
| `discount` | FLOAT | Discount applied (₹) |
| `delivery_duration_min` | FLOAT | Delivery time in minutes |
| `distance_km` | FLOAT | Delivery distance |
| `weather` | STRING | Clear / Rainy / Stormy / Foggy |
| `traffic_level` | STRING | Low / Medium / High / Jam |
| `payment_method` | STRING | UPI / Card / COD / Wallet |
| `status` | STRING | Delivered / Cancelled |

---

## How to Reproduce

### Prerequisites

- Python 3.8+
- Java 8 or 11 (for MapReduce)
- Hadoop 3.x (optional — see local mode below)
- Apache Pig 0.17+ (optional)
- Apache Hive 3.x (optional)
- Apache Spark 3.x with PySpark (optional)
- Docker & Docker Compose (for cluster mode)

---

### Option 1 — Local Mode (No Hadoop Required)

This runs the full 8-phase pipeline using Python only. Best for quick testing.

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the complete pipeline
python scripts/local_pipeline.py --rows 50000

# 3. Launch the interactive dashboard
python web/server.py
# Open http://localhost:5000
```

---

### Option 2 — Docker Cluster Mode

Spins up a full Hadoop + Spark multi-container cluster.

```bash
# 1. Start the cluster
docker-compose up -d

# 2. Check all containers are running
docker-compose ps

# 3. Run the pipeline inside the container
docker exec -it namenode bash
cd /workspace
make generate
make all
```

Web UIs once cluster is running:
- Hadoop NameNode → `http://localhost:9870`
- YARN ResourceManager → `http://localhost:8088`
- Spark Master → `http://localhost:8080`

---

### Option 3 — Native Hadoop / Spark Cluster

If you have Hadoop and Spark installed locally:

```bash
# Step 1: Generate dataset
python data/generate_data.py --rows 2000000 --output data/food_delivery_data.csv

# Step 2: Setup HDFS and ingest data
./hdfs/setup_hdfs.sh
./hdfs/ingest.sh data/food_delivery_data.csv

# Step 3: Clean data with Pig
pig -x mapreduce pig/clean_data.pig

# Step 4: Run MapReduce jobs
hadoop jar mapreduce/orders_per_city/OrdersPerCity.jar OrdersPerCity \
    /fooddelivery/clean /fooddelivery/output/orders_per_city

hadoop jar mapreduce/revenue_per_cuisine/RevenueByCuisine.jar RevenueByCuisine \
    /fooddelivery/clean /fooddelivery/output/revenue_per_cuisine

hadoop jar mapreduce/avg_delivery_per_hour/AvgDeliveryPerHour.jar AvgDeliveryPerHour \
    /fooddelivery/clean /fooddelivery/output/avg_delivery_per_hour

# Step 5: Run Hive analytics
hive -f hive/create_tables.hql
hive -f hive/analytics_queries.hql

# Step 6: Run Spark analytics and ML
spark-submit spark/spark_analytics.py
spark-submit spark/feature_engineering.py
spark-submit mllib/regression_model.py
spark-submit mllib/classification_model.py
spark-submit mllib/clustering_model.py

# Step 7: Generate charts
python visualization/generate_charts.py
```

Or run everything at once:
```bash
make all
```

---

## Key Findings

- **Peak hours**: 12–2 PM and 7–10 PM see 3× order volume
- **Top cuisine**: North Indian generates highest revenue (~28%)
- **Delay drivers**: Rain + high traffic increases delays by ~35%
- **Best regression model**: Random Forest — RMSE ≈ 4.2 min
- **Customer segments**: 4 distinct clusters (frequent, occasional, high-value, deal-seekers)

---

## Performance Comparison

| Query | MapReduce | Hive (Text) | Hive (ORC) | Spark |
|-------|-----------|-------------|------------|-------|
| Orders per city | ~182s | ~95s | ~42s | **~12s** |
| Revenue by cuisine | ~212s | ~108s | ~48s | **~14s** |
| Avg delivery/hour | ~194s | ~104s | ~45s | **~14s** |

*Benchmarked on 2M rows, single-node cluster.*

---

## Tool Justification

| Tool | Role | Why |
|------|------|-----|
| **HDFS** | Distributed storage | Fault-tolerant, handles large files with replication |
| **Pig** | ETL / Data cleaning | Dataflow scripting ideal for transformation pipelines |
| **MapReduce** | Batch processing | Core Hadoop paradigm, good for aggregations |
| **Hive** | SQL Warehouse | SQL analytics on structured warehouse data |
| **Spark** | Fast analytics | In-memory processing, 10–15× faster than MapReduce |
| **MLlib** | Machine Learning | Distributed ML at scale, integrates with Spark |

---

## Contributors

| Name | Roll Number |
|------|-------------|
| Vishal Rawat | 2023BCS0131 |
| Ankush | 2023BCS0131 |
| Ansh Singh | 2023BCS0120 |

**Course**: Big Data Analytics — Class Project

---

## License

MIT License — open for academic and educational use.
