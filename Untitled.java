Project Audit — Bugs Found & Fixes Planned
Critical Bugs
1. RevenueByCuisine.java — Combiner corrupts order count
The combiner pre-sums revenue, but the reducer counts each incoming value as 1 order. After combining, if 100 orders from one mapper become 1 summed value, the reducer counts it as 1 instead of 100. Total revenue is correct, but order count and average revenue are wrong.

2. All Spark/MLlib scripts — Crash on local CSV (no net_revenue column)
When HDFS is unavailable, scripts fall back to reading the raw CSV. But the raw CSV doesn't have net_revenue, delivery_speed_kmph, time_of_day, order_value_bucket, or distance_bucket. Any query using net_revenue will throw AnalysisException. Affects 7 scripts.

3. generate_data.py — Duplicate import numpy block
Lines 26-33 import numpy twice with the same try/except pattern.

Medium Issues
4. All Spark/MLlib scripts — Fragile relative paths
LOCAL_PATH = "../data/food_delivery_data.csv" breaks if scripts are run from any directory other than spark/ or mllib/.

5. Python streaming mappers — No schema documentation
Mappers reference column indices (fields[12], fields[18]) that only work on Pig-cleaned data, but this isn't documented. Running them on raw data silently produces wrong results.

6. No .gitignore — Generated data and charts will bloat the repo
Fixes Applied
#	File(s)	Fix
1	RevenueByCuisine.java	Remove combiner (fixes count bug; AvgDeliveryPerHour.java already demonstrates proper custom-Writable combiner pattern)
2	7 Spark/MLlib scripts	Add derived column computation when reading raw CSV locally
3	generate_data.py	Remove duplicate numpy import
4	All Spark/MLlib scripts	Use os.path for robust path resolution from project root
5	3 Python streaming mappers	Add clear schema header comments
6	Project root	Add .gitignore fix this all this issue  fix all this issue