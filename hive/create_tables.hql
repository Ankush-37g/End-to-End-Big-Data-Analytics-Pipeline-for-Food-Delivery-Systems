-- ============================================================
-- Hive: Table Creation Script
-- Phase 4: Create external, partitioned, and ORC tables
-- ============================================================
-- Run: hive -f hive/create_tables.hql

-- ============================================================
-- Create Database
-- ============================================================
CREATE DATABASE IF NOT EXISTS fooddelivery;
USE fooddelivery;

-- ============================================================
-- 1. External Table (Text Format) over clean data
--    This table points to the Pig-cleaned data in HDFS
-- ============================================================
DROP TABLE IF EXISTS orders_text;

CREATE EXTERNAL TABLE orders_text (
    order_id            INT,
    customer_id         STRING,
    restaurant_id       STRING,
    cuisine             STRING,
    city                STRING,
    area                STRING,
    order_time          STRING,
    delivery_time       STRING,
    delivery_duration_min FLOAT,
    distance_km         FLOAT,
    order_value         INT,
    discount            INT,
    net_revenue         INT,
    payment_method      STRING,
    delivery_partner_id STRING,
    partner_rating      FLOAT,
    weather             STRING,
    traffic_level       STRING,
    status              STRING,
    customer_rating     FLOAT,
    order_hour          INT,
    order_day_of_week   STRING,
    is_weekend          INT,
    is_delayed          INT,
    delivery_speed_kmph FLOAT,
    order_value_bucket  STRING,
    distance_bucket     STRING,
    time_of_day         STRING
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/fooddelivery/clean';

-- Verify
SELECT COUNT(*) AS total_rows FROM orders_text;

-- ============================================================
-- 2. ORC Table (Columnar, Compressed)
--    For performance comparison
-- ============================================================
DROP TABLE IF EXISTS orders_orc;

CREATE TABLE orders_orc (
    order_id            INT,
    customer_id         STRING,
    restaurant_id       STRING,
    cuisine             STRING,
    city                STRING,
    area                STRING,
    order_time          STRING,
    delivery_time       STRING,
    delivery_duration_min FLOAT,
    distance_km         FLOAT,
    order_value         INT,
    discount            INT,
    net_revenue         INT,
    payment_method      STRING,
    delivery_partner_id STRING,
    partner_rating      FLOAT,
    weather             STRING,
    traffic_level       STRING,
    status              STRING,
    customer_rating     FLOAT,
    order_hour          INT,
    order_day_of_week   STRING,
    is_weekend          INT,
    is_delayed          INT,
    delivery_speed_kmph FLOAT,
    order_value_bucket  STRING,
    distance_bucket     STRING,
    time_of_day         STRING
)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

-- Load data from text table into ORC
INSERT OVERWRITE TABLE orders_orc
SELECT * FROM orders_text;

-- ============================================================
-- 3. Partitioned Table (by city, ORC format)
--    For performance comparison with partitioning
-- ============================================================
DROP TABLE IF EXISTS orders_partitioned;

CREATE TABLE orders_partitioned (
    order_id            INT,
    customer_id         STRING,
    restaurant_id       STRING,
    cuisine             STRING,
    area                STRING,
    order_time          STRING,
    delivery_time       STRING,
    delivery_duration_min FLOAT,
    distance_km         FLOAT,
    order_value         INT,
    discount            INT,
    net_revenue         INT,
    payment_method      STRING,
    delivery_partner_id STRING,
    partner_rating      FLOAT,
    weather             STRING,
    traffic_level       STRING,
    status              STRING,
    customer_rating     FLOAT,
    order_hour          INT,
    order_day_of_week   STRING,
    is_weekend          INT,
    is_delayed          INT,
    delivery_speed_kmph FLOAT,
    order_value_bucket  STRING,
    distance_bucket     STRING,
    time_of_day         STRING
)
PARTITIONED BY (city STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

-- Enable dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=100;

-- Load data with dynamic partitioning
INSERT OVERWRITE TABLE orders_partitioned PARTITION (city)
SELECT
    order_id, customer_id, restaurant_id, cuisine, area,
    order_time, delivery_time, delivery_duration_min, distance_km,
    order_value, discount, net_revenue, payment_method,
    delivery_partner_id, partner_rating, weather, traffic_level,
    status, customer_rating, order_hour, order_day_of_week,
    is_weekend, is_delayed, delivery_speed_kmph,
    order_value_bucket, distance_bucket, time_of_day,
    city  -- partition column must be last
FROM orders_text;

-- Verify partitions
SHOW PARTITIONS orders_partitioned;

-- ============================================================
-- 4. Parquet Table (alternative columnar format)
-- ============================================================
DROP TABLE IF EXISTS orders_parquet;

CREATE TABLE orders_parquet
STORED AS PARQUET
AS SELECT * FROM orders_text;

-- ============================================================
-- Summary: Check table sizes and storage
-- ============================================================
DESCRIBE FORMATTED orders_text;
DESCRIBE FORMATTED orders_orc;
DESCRIBE FORMATTED orders_partitioned;
