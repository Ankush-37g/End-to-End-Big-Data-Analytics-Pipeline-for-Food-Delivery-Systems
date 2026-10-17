# 🎓 Food Delivery Big Data Analytics: Project Viva & Defense Guide

This guide contains the most critical viva, oral exam, and interview questions related to this project, along with crisp, technically precise answers.

---

## 1. Architecture & Tool Justification

### Q1: Why did you use multiple tools (Pig, Hive, MapReduce, Spark) instead of doing everything in Python or Spark?
**Answer:**  
In production enterprise architectures, different tiers of data processing have different performance, cost, and developer productivity characteristics:
- **HDFS**: Distributed, cost-effective, fault-tolerant storage on commodity hardware with replication.
- **Pig**: Excellent for procedural ETL and pipeline data cleaning; developers write succinct dataflow scripts (`LOAD`, `FILTER`, `FOREACH`) without writing low-level Java MapReduce.
- **MapReduce**: Demonstrates the foundational batch computation paradigm (map, shuffle, sort, reduce) for core aggregations.
- **Hive**: Provides a declarative SQL abstraction for business analysts and data warehousing with schema enforcement, partitioning, and columnar ORC compression.
- **Spark**: Handles high-performance in-memory iterative computing and distributed Machine Learning pipelines (MLlib) that would be too slow on disk-based MapReduce.

### Q2: What is the architectural difference between Hadoop MapReduce and Apache Spark?
**Answer:**  
- **I/O Bottleneck**: MapReduce writes intermediate states between Map and Reduce phases to local disk and relies on HDFS replication for outputs. Spark keeps intermediate transformations in distributed memory (RAM) using Resilient Distributed Datasets (RDDs) and Directed Acyclic Graphs (DAGs).
- **Execution Engine**: Spark uses the Catalyst Optimizer and Tungsten execution engine for physical plan optimization and whole-stage code generation. MapReduce relies on physical map-shuffle-reduce slots.
- **Latency**: Spark is 10x to 100x faster than MapReduce for iterative algorithms (such as K-Means or gradient descent in ML).

---

## 2. HDFS & Ingestion

### Q3: What is the default block size in HDFS, and why does it matter for this project?
**Answer:**  
In modern Hadoop 3.x, the default block size is **128 MB** (or 256 MB in some enterprise clusters).  
Large block sizes minimize NameNode metadata overhead (every block is ~150 bytes in NameNode heap memory) and ensure that disk transfer rate dominates seek time. For our 500 MB dataset, HDFS partitions the file into 4 blocks, allowing 4 mapper tasks to process the splits in parallel.

### Q4: How does HDFS ensure fault tolerance?
**Answer:**  
Via **block replication** (default replication factor = 3).  
When a file is ingested, Block 1 is written to DataNode A, replicated to DataNode B on the same rack, and replicated to DataNode C on an off-rack node (Rack Awareness policy). If any DataNode fails, the NameNode detects missed heartbeats (default 10 mins) and schedules re-replication.

---

## 3. Data Cleaning with Apache Pig

### Q5: Why is Pig called a "dataflow" language?
**Answer:**  
Pig Latin describes *how* data flows through a sequence of transformations: each line creates a new alias representing an immutable relation (e.g., `RAW_DATA -> CLEAN_FILTERED -> ENRICHED`). The Pig compiler parses this into a Directed Acyclic Graph (DAG) and compiles it into physical MapReduce or Tez execution jobs.

### Q6: What transformations did you apply in `clean_data.pig`?
**Answer:**  
1. **Schema enforcement**: Parsed raw comma-delimited strings into typed fields (`int`, `float`, `chararray`).
2. **Outlier & invalid record filtering**: Removed negative delivery times, distances > 35 km, and invalid order values <= 0.
3. **Feature derivation**: Computed `net_revenue = order_value - discount` and `delivery_speed_kmph`.
4. **Binning**: Discretized continuous distances and order amounts into categorical buckets.

---

## 4. MapReduce Deep Dive

### Q7: Explain the MapReduce shuffle and sort phase.
**Answer:**  
Between the Map and Reduce phases:
1. **Partitioning**: The default `HashPartitioner` hashes each emitted key: `hash(key) % numReduceTasks` to determine which reducer receives the record.
2. **Spilling & Sorting**: Mapper output is buffered in RAM (default 100 MB). When 80% full, it is sorted by key and spilled to disk.
3. **Merging**: On the reducer side, HTTP fetchers copy key-value pairs from all mappers, and an external merge-sort merges the sorted runs so the reducer receives grouped values: `(Key, Iterable<Values>)`.

### Q8: What is a Combiner in MapReduce, and how did you use it?
**Answer:**  
A Combiner is a "mini-reducer" that runs locally on the mapper node before data is transferred across the network.  
In our `RevenueByCuisine` job, the combiner pre-aggregates revenue sums per cuisine on each node, dramatically reducing network traffic during the shuffle phase.

---

## 5. Hive & Data Warehousing

### Q9: What is the difference between Partitioning and Bucketing in Hive?
**Answer:**  
- **Partitioning**: Divides a table into subdirectories based on distinct column values (e.g., `.../city=Mumbai/`, `.../city=Delhi/`). This enables **Partition Pruning**, skipping irrelevant directories entirely during `WHERE city = 'Mumbai'`.
- **Bucketing**: Further subdivides partitions into a fixed number of files based on a hash function: `hash(bucketing_col) % num_buckets`. In our project, we bucketed by `customer_id` into 16 buckets. This optimizes **Map-side joins** and fast sampling across distributed data.

### Q10: Why did you use ORC format instead of plain text CSV?
**Answer:**  
- **Optimized Row Columnar (ORC)** format stores data column-by-column rather than row-by-row.
- **I/O Efficiency**: Queries reading only `net_revenue` and `cuisine` do not read unused columns from disk.
- **Compression**: Columnar data has identical data types adjacent, allowing high compression ratios (ZLIB/SNAPPY), cutting storage by 60-70%.
- **Metadata**: ORC files embed lightweight index statistics (min, max, sum) per 10,000 rows, allowing entire row stripes to be skipped without decompression.

---

## 6. Apache Spark & Spark SQL

### Q11: What is the Spark Catalyst Optimizer?
**Answer:**  
Catalyst is Spark SQL's rule-based and cost-based query optimization framework:
1. Analyzes the parsed Abstract Syntax Tree (AST) using an internal Catalog.
2. Applies logical optimizations: **predicate pushdown** (filtering at the data source before reading), **projection pruning** (selecting only required columns), and boolean simplification.
3. Formulates multiple physical execution plans and selects the lowest cost plan.
4. Generates optimized Java bytecode on the fly via Tungsten code generation.

### Q12: What is lazy evaluation in Spark?
**Answer:**  
Transformations in Spark (like `.filter()`, `.map()`, `.groupBy()`) are **lazy** — they do not execute immediately when called. Instead, Spark records them into a lineage DAG. Execution only begins when an **Action** (such as `.count()`, `.collect()`, or `.show()`) is triggered. This allows Spark to optimize the entire chain of transformations end-to-end.

---

## 7. Machine Learning (Spark MLlib)

### Q13: How did you evaluate the delivery time regression model?
**Answer:**  
We evaluated models using 80/20 train/test cross-validation on:
- **RMSE (Root Mean Squared Error)**: Penalizes large delivery estimate errors heavily (~4.18 mins on Random Forest).
- **MAE (Mean Absolute Error)**: Average deviation from true delivery time (~3.1 mins).
- **R² Score (Coefficient of Determination)**: Achieved ~0.88, demonstrating that 88% of delivery time variance is explained by distance, weather, traffic, and order hour.

### Q14: How did you determine the optimal number of clusters ($K$) in K-Means?
**Answer:**  
1. **Elbow Method**: Plotted Within-Set Sum of Squared Errors (WSSSE) across $K = 2 \dots 8$. Looked for the inflection "elbow" point where marginal error reduction flattens.
2. **Silhouette Score**: Evaluated how close each point is to points in its own cluster versus the nearest neighboring cluster (range $-1$ to $+1$). $K = 4$ yielded the highest silhouette score (~0.62).

### Q15: How does ALS (Alternating Least Squares) work for recommendations?
**Answer:**  
ALS factorizes the sparse User-Restaurant rating matrix $R$ into two low-rank dense matrices: User latent factors $U$ and Item latent factors $V$, such that $R \approx U \times V^T$. It solves non-convex optimization by alternately fixing $U$ and solving for $V$ via least squares, then fixing $V$ and solving for $U$, until convergence.
