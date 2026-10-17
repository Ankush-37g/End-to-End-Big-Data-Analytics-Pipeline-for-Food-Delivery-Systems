-- ============================================================
-- Hive: Performance Comparison
-- Text vs ORC, Partitioned vs Non-Partitioned
-- ============================================================
-- Run: hive -f hive/performance_compare.hql
-- Record execution times from the console output for your report

USE fooddelivery;

-- ============================================================
-- Test 1: Full Table Scan - Count by City
-- Compare: Text vs ORC vs Partitioned
-- ============================================================

-- 1a. Text Table
SELECT '--- TEXT TABLE: Count by City ---' AS test;
SELECT city, COUNT(*) AS cnt
FROM orders_text
GROUP BY city
ORDER BY cnt DESC;

-- 1b. ORC Table
SELECT '--- ORC TABLE: Count by City ---' AS test;
SELECT city, COUNT(*) AS cnt
FROM orders_orc
GROUP BY city
ORDER BY cnt DESC;

-- 1c. Partitioned Table
SELECT '--- PARTITIONED TABLE: Count by City ---' AS test;
SELECT city, COUNT(*) AS cnt
FROM orders_partitioned
GROUP BY city
ORDER BY cnt DESC;

-- ============================================================
-- Test 2: Filtered Query - Single City
-- Partitioned table should excel here (partition pruning)
-- ============================================================

-- 2a. Text Table
SELECT '--- TEXT TABLE: Mumbai Orders ---' AS test;
SELECT COUNT(*) AS mumbai_orders,
       SUM(net_revenue) AS mumbai_revenue,
       ROUND(AVG(delivery_duration_min), 2) AS avg_delivery
FROM orders_text
WHERE city = 'Mumbai' AND status = 'Delivered';

-- 2b. ORC Table
SELECT '--- ORC TABLE: Mumbai Orders ---' AS test;
SELECT COUNT(*) AS mumbai_orders,
       SUM(net_revenue) AS mumbai_revenue,
       ROUND(AVG(delivery_duration_min), 2) AS avg_delivery
FROM orders_orc
WHERE city = 'Mumbai' AND status = 'Delivered';

-- 2c. Partitioned Table (partition pruning)
SELECT '--- PARTITIONED TABLE: Mumbai Orders (partition pruning) ---' AS test;
SELECT COUNT(*) AS mumbai_orders,
       SUM(net_revenue) AS mumbai_revenue,
       ROUND(AVG(delivery_duration_min), 2) AS avg_delivery
FROM orders_partitioned
WHERE city = 'Mumbai' AND status = 'Delivered';

-- ============================================================
-- Test 3: Aggregation with Multiple Columns
-- ORC should benefit from column pruning
-- ============================================================

-- 3a. Text Table
SELECT '--- TEXT TABLE: Revenue by Cuisine ---' AS test;
SELECT cuisine, SUM(net_revenue) AS total_rev
FROM orders_text
WHERE status = 'Delivered'
GROUP BY cuisine
ORDER BY total_rev DESC;

-- 3b. ORC Table
SELECT '--- ORC TABLE: Revenue by Cuisine ---' AS test;
SELECT cuisine, SUM(net_revenue) AS total_rev
FROM orders_orc
WHERE status = 'Delivered'
GROUP BY cuisine
ORDER BY total_rev DESC;

-- ============================================================
-- Test 4: Complex Join-like Query
-- ============================================================

-- 4a. Text Table
SELECT '--- TEXT TABLE: Complex Aggregation ---' AS test;
SELECT
    city,
    weather,
    traffic_level,
    COUNT(*) AS orders,
    ROUND(AVG(delivery_duration_min), 2) AS avg_time,
    ROUND(SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        AS delay_pct
FROM orders_text
WHERE status = 'Delivered'
GROUP BY city, weather, traffic_level
HAVING COUNT(*) > 100
ORDER BY delay_pct DESC
LIMIT 20;

-- 4b. ORC Table
SELECT '--- ORC TABLE: Complex Aggregation ---' AS test;
SELECT
    city,
    weather,
    traffic_level,
    COUNT(*) AS orders,
    ROUND(AVG(delivery_duration_min), 2) AS avg_time,
    ROUND(SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        AS delay_pct
FROM orders_orc
WHERE status = 'Delivered'
GROUP BY city, weather, traffic_level
HAVING COUNT(*) > 100
ORDER BY delay_pct DESC
LIMIT 20;

-- ============================================================
-- Storage Size Comparison
-- ============================================================
-- Check the storage sizes of each table format
-- Run these in HDFS shell:
--   hdfs dfs -du -s -h /fooddelivery/clean
--   hdfs dfs -du -s -h /user/hive/warehouse/fooddelivery.db/orders_orc
--   hdfs dfs -du -s -h /user/hive/warehouse/fooddelivery.db/orders_partitioned
--   hdfs dfs -du -s -h /user/hive/warehouse/fooddelivery.db/orders_parquet

-- ============================================================
-- EXPLAIN Plans for comparison
-- ============================================================
EXPLAIN SELECT COUNT(*) FROM orders_text WHERE city = 'Mumbai';
EXPLAIN SELECT COUNT(*) FROM orders_orc WHERE city = 'Mumbai';
EXPLAIN SELECT COUNT(*) FROM orders_partitioned WHERE city = 'Mumbai';
