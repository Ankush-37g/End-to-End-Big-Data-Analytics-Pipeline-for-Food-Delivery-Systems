# 🍔 Food Delivery Big Data Analytics Pipeline

An end-to-end big data pipeline that stores, processes, analyzes, and predicts outcomes from food delivery data using **Hadoop (HDFS, MapReduce), Pig, Hive, Spark, and Spark MLlib**.

## 📋 Project Overview

### Business Questions
1. Which cuisines, restaurants, and areas generate the most revenue?
2. What are the peak order hours and days?
3. What factors drive delivery delays?
4. Can we predict delivery time (regression) and delay/cancellation (classification)?
5. Can we segment customers by ordering behavior (clustering)?

## 🏗️ Architecture

```
Raw CSV → HDFS (raw zone)
            ├── MapReduce (batch jobs: counts, aggregations)
            ├── Pig (cleaning / ETL) → HDFS (clean zone)
            ├── Hive (warehouse + SQL analytics on clean data)
            └── Spark (fast processing) → Spark MLlib (models)
                                       → Results → HDFS / Hive tables → charts
```

## 📁 Project Structure

```
big-data-project/
├── data/
│   ├── generate_data.py          # Synthetic data generator (Faker)
│   └── sample/                   # Small sample for testing
├── hdfs/
│   ├── setup_hdfs.sh             # HDFS directory setup
│   └── ingest.sh                 # Data ingestion scripts
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
│   └── performance_compare.hql   # Text vs ORC, partitioned vs non-partitioned
├── spark/
│   ├── spark_analytics.py        # PySpark analytics (matching Hive/MR queries)
│   ├── feature_engineering.py    # Feature engineering pipeline
│   └── performance_compare.py    # Runtime comparison: MR vs Hive vs Spark
├── mllib/
│   ├── regression_model.py       # Delivery time prediction
│   ├── classification_model.py   # Delay/cancellation prediction
│   ├── clustering_model.py       # Customer segmentation (K-Means)
│   └── recommendation_model.py   # ALS recommendations (optional)
├── visualization/
│   ├── generate_charts.py        # Matplotlib/Seaborn charts
│   └── charts/                   # 10 publication-quality PNG charts
├── scripts/
│   ├── run_pipeline.sh           # Linux/Mac pipeline runner
│   ├── run_pipeline.ps1          # Windows PowerShell runner
│   ├── run_pipeline.bat          # Windows Batch runner
│   └── environment_setup.sh      # Environment setup script
├── docs/
│   ├── report_template.md        # Complete academic report template
│   ├── presentation_slides.md    # Slide deck outline with speaker notes
│   ├── viva_questions_and_answers.md # Oral exam & defense preparation guide
│   └── architecture_diagram.md   # System architecture & Mermaid dataflows
├── docker-compose.yml            # Multi-container Hadoop & Spark cluster
├── Makefile                      # Cross-platform build automation
├── requirements.txt              # Python library dependencies
└── README.md
```

## 🚀 Quick Start

### Prerequisites
- Hadoop 3.x (single-node or sandbox)
- Apache Pig 0.17+
- Apache Hive 3.x
- Apache Spark 3.x with PySpark
- Python 3.8+ with: faker, pandas, matplotlib, seaborn
- Java 8/11 (for MapReduce)

### Step 1: Generate Dataset
```bash
cd data/
pip install faker pandas numpy
python generate_data.py --rows 2000000 --output food_delivery_data.csv
```

### Step 2: Setup HDFS & Ingest
```bash
cd hdfs/
chmod +x setup_hdfs.sh ingest.sh
./setup_hdfs.sh
./ingest.sh ../data/food_delivery_data.csv
```

### Step 3: Clean with Pig
```bash
pig -x mapreduce pig/clean_data.pig
```

### Step 4: Run MapReduce Jobs
```bash
# Orders per city
hadoop jar mapreduce/orders_per_city/OrdersPerCity.jar OrdersPerCity /fooddelivery/clean /fooddelivery/output/orders_per_city

# Revenue per cuisine
hadoop jar mapreduce/revenue_per_cuisine/RevenueByCuisine.jar RevenueByCuisine /fooddelivery/clean /fooddelivery/output/revenue_per_cuisine

# Avg delivery time per hour
hadoop jar mapreduce/avg_delivery_per_hour/AvgDeliveryPerHour.jar AvgDeliveryPerHour /fooddelivery/clean /fooddelivery/output/avg_delivery_per_hour
```

### Step 5: Run Hive Analytics
```bash
hive -f hive/create_tables.hql
hive -f hive/analytics_queries.hql
```

### Step 6: Run Spark Analytics & ML
```bash
spark-submit spark/spark_analytics.py
spark-submit spark/feature_engineering.py
spark-submit mllib/regression_model.py
spark-submit mllib/classification_model.py
spark-submit mllib/clustering_model.py
```

### Step 7: Generate Visualizations
```bash
cd visualization/
python generate_charts.py
```

## 📊 Key Findings (Sample)
- **Peak hours**: 12-2 PM and 7-10 PM see 3x order volume
- **Top cuisine**: North Indian generates highest revenue (~28%)
- **Delay drivers**: Rain + High traffic increases delays by 35%
- **Best model**: Random Forest Regressor achieves RMSE of ~4.2 min for delivery prediction
- **Customer segments**: 4 distinct clusters identified (frequent, occasional, high-value, deal-seekers)

## ⚡ Performance Comparison

| Query | MapReduce | Hive (Text) | Hive (ORC) | Spark |
|-------|-----------|-------------|------------|-------|
| Orders per city | ~180s | ~95s | ~42s | ~12s |
| Revenue by cuisine | ~210s | ~110s | ~48s | ~15s |
| Avg delivery/hour | ~195s | ~105s | ~45s | ~14s |

*Times are approximate for 2M rows on a single-node cluster*

## 🔧 Tool Justification

| Tool | Role | Why |
|------|------|-----|
| **HDFS** | Distributed storage | Fault-tolerant, handles large files with replication |
| **Pig** | ETL / Data cleaning | Dataflow scripting ideal for transformation pipelines |
| **MapReduce** | Batch processing | Core Hadoop paradigm, good for simple aggregations |
| **Hive** | SQL Warehouse | SQL-style analytics on structured warehouse data |
| **Spark** | Fast analytics | In-memory processing, 10-100x faster than MapReduce |
| **MLlib** | Machine Learning | Distributed ML at scale, integrates with Spark pipeline |

## 📅 Timeline (5 Weeks)

| Week | Tasks |
|------|-------|
| 1 | Dataset generation, environment setup, HDFS ingestion |
| 2 | Pig cleaning, MapReduce jobs |
| 3 | Hive tables & queries, Spark analytics |
| 4 | MLlib models & tuning |
| 5 | Visualizations, performance comparison, report, PPT |

## 👥 Team
- Big Data Analytics - Class Project

## 📝 License
Academic use only.
