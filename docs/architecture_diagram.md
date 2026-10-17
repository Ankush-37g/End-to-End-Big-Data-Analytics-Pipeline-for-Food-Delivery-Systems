# 🏗️ System Architecture & Dataflow Documentation

## 1. High-Level Pipeline Architecture

The diagram below details how data transitions across the storage, compute, warehousing, machine learning, and visualization tiers:

```mermaid
flowchart TD
    subgraph S1["Phase 1: Ingestion & Storage"]
        DG["Synthetic Data Generator<br/>(Faker 2M rows)"] -->|"CSV Export"| HDFS_RAW["HDFS: /fooddelivery/raw/<br/>(Replication x3)"]
    end

    subgraph S2["Phase 2: Cleaning & ETL"]
        HDFS_RAW -->|"Pig Latin Dataflow"| PIG["Apache Pig<br/>(clean_data.pig)"]
        PIG -->|"Clean Typed CSV"| HDFS_CLEAN["HDFS: /fooddelivery/clean/"]
    end

    subgraph S3["Phase 3: Core Batch Processing"]
        HDFS_CLEAN --> MR1["MR 1: Orders Per City"]
        HDFS_CLEAN --> MR2["MR 2: Revenue By Cuisine"]
        HDFS_CLEAN --> MR3["MR 3: Avg Delivery / Hour"]
        MR1 & MR2 & MR3 --> HDFS_OUT["HDFS: /fooddelivery/output/"]
    end

    subgraph S4["Phase 4: Data Warehousing"]
        HDFS_CLEAN -->|"DDL Ingest"| HIVE_RAW["Hive External Table"]
        HIVE_RAW -->|"CTAS Transformation"| HIVE_ORC["Hive ORC Table<br/>(Partitioned by City, 16 Buckets)"]
        HIVE_ORC --> HIVE_ANALYTICS["Analytical SQL & Window Queries"]
    end

    subgraph S5["Phase 5: In-Memory Analytics & ML"]
        HDFS_CLEAN -->|"Distributed RDD / DataFrame"| SPARK["Apache Spark Engine<br/>(Catalyst Optimizer)"]
        SPARK --> SPARK_FE["Feature Engineering & Caching"]
        SPARK_FE --> ML_REG["Regression: Delivery Duration"]
        SPARK_FE --> ML_CLS["Classification: Delay & Cancellation"]
        SPARK_FE --> ML_CLUST["Clustering: Customer K-Means"]
        SPARK_FE --> ML_REC["Recommendation: ALS Matrix Factorization"]
    end

    subgraph S6["Phase 6: Reporting & Visuals"]
        HIVE_ANALYTICS & HDFS_OUT & ML_REG & ML_CLUST --> VIZ["Python Seaborn / Matplotlib"]
        VIZ --> CHARTS["10 Publication Charts<br/>(visualization/charts/*.png)"]
    end

    classDef storage fill:#1e293b,stroke:#3b82f6,stroke-width:2px,color:#f8fafc;
    classDef compute fill:#0f172a,stroke:#10b981,stroke-width:2px,color:#f8fafc;
    classDef ml fill:#312e81,stroke:#8b5cf6,stroke-width:2px,color:#f8fafc;

    class HDFS_RAW,HDFS_CLEAN,HDFS_OUT,HIVE_ORC storage;
    class PIG,MR1,MR2,MR3,SPARK,HIVE_ANALYTICS compute;
    class ML_REG,ML_CLS,ML_CLUST,ML_REC ml;
```

---

## 2. Directory Layout in HDFS

```
hdfs://namenode:9000/fooddelivery/
├── raw/
│   └── food_delivery_data.csv        # Immutable raw data (~500 MB)
├── clean/
│   ├── part-m-00000.csv              # Pig-cleaned partition 1
│   └── part-m-00001.csv              # Pig-cleaned partition 2
├── output/
│   ├── orders_per_city/
│   │   └── part-r-00000              # MapReduce output (City/Area -> Count)
│   ├── revenue_per_cuisine/
│   │   └── part-r-00000              # MapReduce output (Cuisine -> Total, Count, Avg)
│   └── avg_delivery_per_hour/
│       └── part-r-00000              # MapReduce output (Hour -> Avg, Min, Max, Count)
└── models/
    ├── delivery_regressor_rf/        # Saved Spark MLlib Random Forest model
    ├── delay_classifier_rf/          # Saved Spark MLlib Classifier model
    └── customer_kmeans_k4/           # Saved K-Means clustering pipeline
```

---

## 3. Data Flow & Transformation Stages

| Stage | Input Schema | Operation | Output Schema |
|---|---|---|---|
| **Raw Ingest** | Unstructured CSV | Faker generation & HDFS copy | 23 raw fields |
| **Pig ETL** | Raw 23 fields | Filter corrupt rows, drop invalid distances, compute net revenue & speed | 28 enriched fields |
| **MapReduce** | Clean 28 fields | Key/Value partitioning, local combiner aggregation, global reducer summation | Aggregated tab-delimited text |
| **Hive Warehouse** | Clean 28 fields | Schema-on-read, Columnar compression (ORC), City partitioning, Customer bucketing | Optimized binary columnar store |
| **Spark Feature Eng.** | Clean 28 fields | VectorAssembler, StringIndexer, OneHotEncoder, StandardScaler | ML Feature Vectors |
| **MLlib** | Feature Vectors | Gradient descent, tree splitting, cross-validation | Predictions, Metrics, Cluster IDs |
| **Visualization** | Aggregation Outputs | Matplotlib & Seaborn rendering | PNG figures (150 DPI) |

---

## 4. Hive Table Architecture

```
                                  [ food_delivery_raw ]
                                  (External Text Table)
                                            │
                                            ▼
                    INSERT OVERWRITE TABLE food_delivery_orc_partitioned
                                PARTITION (city)
                                            │
               ┌────────────────────────────┼────────────────────────────┐
               ▼                            ▼                            ▼
         [ city=Mumbai ]              [ city=Delhi ]              [ city=Bangalore ]
         16 Buckets (ORC)             16 Buckets (ORC)             16 Buckets (ORC)
      ┌─────────┬─────────┐        ┌─────────┬─────────┐        ┌─────────┬─────────┐
      │bucket_00│bucket_01│...     │bucket_00│bucket_01│...     │bucket_00│bucket_01│...
      └─────────┴─────────┘        └─────────┴─────────┘        └─────────┴─────────┘
```

---

## 5. Spark MLlib Machine Learning Pipeline Architecture

```
   Raw DataFrame
        │
        ▼
[ StringIndexer ] ───> Converts 'weather', 'traffic_level', 'cuisine' to numeric indices
        │
        ▼
[ OneHotEncoder ] ───> Converts category indices to sparse one-hot binary vectors
        │
        ▼
[ VectorAssembler ] ─> Combines numeric features + encoded vectors into single 'features' vector
        │
        ▼
[ StandardScaler ] ──> Standardizes features to zero mean and unit variance (for K-Means)
        │
        ├──────────────────────┬──────────────────────┬──────────────────────┐
        ▼                      ▼                      ▼                      ▼
[ RF Regressor ]      [ RF Classifier ]       [ K-Means (K=4) ]       [ ALS Model ]
Predicts Delivery       Predicts Delay          Customer RFM           Collaborative
Duration (Minutes)       Probability             Segmentation         Filtering Recs
```
