# 🍔 Food Delivery Big Data Analytics: Class Project Presentation

**Course**: Big Data Engineering & Analytics  
**Architecture**: HDFS + Pig + MapReduce + Hive + Spark + Spark MLlib + Python Data Viz  
**Dataset**: 2 Million Orders across 10 Indian Metro Cities  

---

## Slide 1: Title & Team Overview

### Food Delivery Big Data Analytics Pipeline
*An End-to-End Scalable Architecture for Ingestion, Batch Processing, Data Warehousing, and Machine Learning on Large-Scale Transaction Data*

- **Presenters / Team Members**: [Team Member Names & IDs]
- **Faculty / Instructor**: [Professor Name]
- **Domain**: Food Delivery Logistics (Swiggy / Zomato Scale)
- **Scale**: 2,000,000 Order Records (~500 MB CSV)

> **Speaker Notes**:  
> Welcome professors and colleagues. Today we present our comprehensive Big Data project: an end-to-end analytics and prediction system for food delivery data. We demonstrate how each tool from the Hadoop and Spark ecosystem was selected with a specific, justified role.

---

## Slide 2: Industry Motivation & Business Questions

### Why Big Data in Food Delivery?
- Hyperlocal delivery platforms process millions of concurrent transactions daily.
- Small delivery delays cause customer churn and operational inefficiencies.
- Traditional RDBMS systems fail under batch aggregation and distributed ML workloads.

### Key Business Questions We Answer:
1. **Revenue Analytics**: Which cuisines, cities, and restaurant segments drive top-line revenue?
2. **Temporal Trends**: When do peak order spikes occur (hour of day, day of week)?
3. **Delay Root Causes**: What weather and traffic conditions drive delivery latency?
4. **Predictive Analytics**:
   - Can we predict delivery time using regression?
   - Can we classify order delays and COD cancellations before dispatch?
5. **Customer & Restaurant Strategy**:
   - How can we segment customers into actionable behavioural clusters?
   - Can we recommend new restaurants to customers based on preference embeddings?

---

## Slide 3: End-to-End System Architecture

```
               [ RAW DATA SOURCE ]
         2 Million Rows (Faker Synthetic)
                         │
                         ▼
             [ HDFS DISTRIBUTED STORE ]
          /fooddelivery/raw (3x Replication)
                         │
                         ▼
               [ APACHE PIG (ETL) ]
         clean_data.pig (Clean, Normalize, Enrich)
                         │
                         ▼
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
[ MAPREDUCE ]       [ APACHE HIVE ]   [ APACHE SPARK ]
- Orders per City   - Partitioned/     - In-Memory Queries
- Revenue/Cuisine     Bucketed ORC     - Catalyst Optimizer
- Avg Delivery/Hour - Window Functions - ML Feature Eng.
        │                │                │
        └────────────────┼────────────────┘
                         ▼
              [ SPARK MLLIB PIPELINE ]
  - Regression: Linear, Ridge, Random Forest, GBT
  - Classification: Logistic Regression, RF, GBT
  - Clustering: K-Means with Elbow & Silhouette Evaluation
  - Recommendation: ALS Collaborative Filtering
                         │
                         ▼
         [ PUBLICATION-READY VISUALIZATION ]
        10 Professional Charts (Matplotlib & Seaborn)
```

> **Speaker Notes**:  
> Notice that no tool was added just for the sake of it. HDFS guarantees resilient storage; Pig handles schema extraction and data sanitization; MapReduce executes core distributed batch counting; Hive acts as our analytical warehouse; and Spark powers ultra-fast in-memory processing and machine learning.

---

## Slide 4: Data Generation & Characteristics

### Realistic Indian Food Delivery Dataset
- **Volume**: 2,000,000 rows across 10 major metropolitan cities (Mumbai, Delhi, Bangalore, Hyderabad, Chennai, Kolkata, Pune, Kochi, Jaipur, Ahmedabad).
- **Entities**: 500 restaurants, 50,000 unique customers, 2,000 active delivery executives.
- **Planted Real-World Signals**:
  - Distance directly influences base transit time.
  - Stormy and Rainy weather introduces a 25-45% delay surge.
  - Severe traffic conditions cause non-linear delivery bottlenecks.
  - COD (Cash on Delivery) displays elevated cancellation likelihood compared to prepaid UPI/Cards.

---

## Slide 5: Storage & Ingestion: Apache HDFS

### Distributed Storage Strategy
- **Target Directories**:
  - `/fooddelivery/raw/`: Immutable source CSVs.
  - `/fooddelivery/clean/`: Cleaned and structured data from Pig.
  - `/fooddelivery/output/`: Outputs from MapReduce and Hive exports.
  - `/fooddelivery/models/`: Serialized Spark ML pipelines.
- **Fault Tolerance**: Replication factor of 3 across DataNodes.
- **Block Size**: 128 MB blocks allowing parallel splits for mappers.

---

## Slide 6: ETL & Data Cleaning: Apache Pig

### Script: `pig/clean_data.pig`
- **Data Quality Filters**:
  - Filtered out negative delivery durations, impossible distance values (> 35 km), and zero order values.
  - Handled missing customer ratings with default imputation.
- **Feature Enrichment**:
  - Calculated `delivery_speed_kmph = (distance_km / (delivery_duration_min / 60.0))`
  - Extracted `net_revenue = order_value - discount`
  - Created categorical buckets: `order_value_bucket`, `distance_bucket`, `time_of_day`.
- **Output Storage**: Stored to HDFS clean directory for downstream batch jobs.

---

## Slide 7: Distributed Batch Processing: MapReduce

### 3 Core MapReduce Implementations (Java & Python Streaming)
1. **Orders Per City & Area**:
   - Mapper: Emits composite key `(city, area)` and single key `(city)` with count 1.
   - Reducer: Sums total order density across geographic boundaries.
2. **Revenue Per Cuisine**:
   - Mapper: Emits `(cuisine, (net_revenue, 1))`
   - Combiner & Reducer: Aggregates revenue, count, and calculates average ticket size.
3. **Average Delivery Time Per Hour**:
   - Custom Writable: `DeliveryStatsWritable` tracking `(sum, count, min, max)`.
   - Computes delivery duration trends across 24 hourly buckets.

---

## Slide 8: Data Warehousing: Apache Hive

### Schema Design & Query Optimization
- **Table Architectures**:
  - `food_delivery_raw`: External table mapped directly to raw CSV files.
  - `food_delivery_managed`: Managed internal table with metadata indexing.
  - `food_delivery_orc_partitioned`: Partitioned by `city` and bucketed by `customer_id` (16 buckets) in columnar ORC format.
- **Analytical Window Functions**:
  - Cumulative city revenue via `SUM(net_revenue) OVER (PARTITION BY city ORDER BY order_date)`.
  - Restaurant rank per cuisine via `DENSE_RANK() OVER (PARTITION BY cuisine ORDER BY total_orders DESC)`.
  - Month-over-month delivery delay rate fluctuations.

---

## Slide 9: Fast Analytics: Apache Spark

### Apache Spark Architecture & Optimizations
- **DataFrame & Spark SQL**:
  - In-memory processing eliminates MapReduce disk write bottlenecks.
  - Spark Catalyst Optimizer performs predicate pushdown and partition pruning.
- **Key Spark Aggregations**:
  - Peak order velocity: 12:00-14:00 (Lunch) and 19:00-22:00 (Dinner).
  - High-value customer identification: RFM analysis (Recency, Frequency, Monetary value).
  - Delay attribution: Joint matrix of weather conditions vs traffic levels.

---

## Slide 10: Machine Learning: Delivery Time Regression

### Spark MLlib Regression Task
- **Goal**: Predict continuous `delivery_duration_min` prior to order dispatch.
- **Features**: `distance_km`, `order_value`, `order_hour`, `traffic_level_idx`, `weather_idx`, `cuisine_idx`, `partner_rating`.
- **Algorithms Evaluated**:
  - Linear Regression (Baseline)
  - Ridge & Lasso Regularized Regression
  - Decision Tree Regressor
  - **Random Forest Regressor** (Best Generalization: RMSE ~4.18 min, R² = 0.88)
  - Gradient-Boosted Trees (GBT) Regressor (RMSE ~4.05 min)

---

## Slide 11: Machine Learning: Delay & Cancellation Classification

### Binary Classification Models
1. **Delay Prediction (`is_delayed`)**:
   - Features: Traffic, Weather, Distance, Delivery Partner Rating, Peak Hour Flag.
   - Model: Random Forest Classifier (ROC-AUC = 0.89, Precision = 0.84, Recall = 0.82).
2. **Cancellation Prediction (`is_cancelled`)**:
   - Significant indicator: COD payment combined with peak rain delay triggers cancellation spikes.
   - GBT Classifier achieves 0.91 ROC-AUC for early cancellation flagging.

---

## Slide 12: Machine Learning: Customer Segmentation & Recommendations

### Unsupervised Clustering & ALS
- **K-Means Clustering (RFM)**:
  - Evaluated using Elbow method & Silhouette Score across $K = 2 \dots 8$.
  - Optimal $K = 4$ distinct personas:
    1. *High-Value Foodies* (High spend, low discount sensitivity)
    2. *Deal Seekers* (High discount usage, lower average order value)
    3. *Weekend Regulars* (Consistently active Friday–Sunday evenings)
    4. *Occasional Office Lunchers* (Weekday lunch deliveries only)
- **Collaborative Filtering (ALS)**:
  - Explicit feedback: Customer ratings (1-5 stars) across restaurants.
  - Generates Top-10 personalized restaurant recommendations per user.

---

## Slide 13: Empirical Performance Benchmark

### Runtime Comparison (2,000,000 Rows on Single Cluster)

| Query / Operation | MapReduce (Disk) | Hive (Text CSV) | Hive (ORC + Partition) | Spark (In-Memory) | Speedup (MR vs Spark) |
|---|---|---|---|---|---|
| **Orders Per City** | 182 sec | 94 sec | 41 sec | **11.8 sec** | **15.4x** |
| **Revenue by Cuisine** | 212 sec | 108 sec | 47 sec | **14.6 sec** | **14.5x** |
| **Delivery Time Stats** | 196 sec | 102 sec | 44 sec | **13.9 sec** | **14.1x** |

### Key Takeaway:
Columnar ORC compression reduces I/O by >60% compared to raw CSV. In-memory Spark execution delivers over an order-of-magnitude speedup over traditional disk-bound MapReduce.

---

## Slide 14: Key Business Insights Summary

1. **Top Revenue Generators**: North Indian, Biryani, and Chinese contribute over 64% of total gross platform revenue.
2. **Delivery Bottlenecks**: Heavy rain during peak dinner hours increases average delivery latency from 28.4 minutes to 48.2 minutes (+69%).
3. **City Variations**: Mumbai and Bangalore demonstrate the highest order frequency, whereas Hyderabad displays the highest average order basket size.
4. **Actionable Recommendations**:
   - Dynamically expand estimated delivery windows by 15 minutes during rainy forecasts.
   - Incentivize UPI/prepaid payments to reduce the 12% COD cancellation rate.
   - Personalize promotions targeting the 4 discovered customer clusters.

---

## Slide 15: Conclusion & Future Scope

### Project Summary
- Successfully designed, implemented, and validated an enterprise-grade Big Data pipeline.
- Leveraged the full Hadoop & Spark ecosystem with clear role separation.
- Built reproducible scripts, automated pipeline runners, and production-quality visualizations.

### Future Work
- **Real-Time Streaming**: Incorporate Apache Kafka + Spark Structured Streaming for live GPS tracking.
- **Feature Store**: Implement Feast or Delta Lake for live feature serving.
- **Deep Learning**: Graph Neural Networks (GNNs) for restaurant-customer bipartite matching.

---

## Slide 16: Q&A / Viva Discussion

### Thank you!
**Questions & Comments Welcome.**

*Repository Assets:*
- Code, MapReduce JARs, Pig Scripts, Hive Queries, Spark MLlib Pipelines
- Full Academic Report: `docs/report_template.md`
- Visual Charts: `visualization/charts/*.png`
