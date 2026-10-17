-- ============================================================
-- Pig Script: Data Cleaning & ETL
-- Phase 2: Load raw CSV, clean, derive fields, store to HDFS
-- ============================================================
-- Run: pig -x mapreduce pig/clean_data.pig
-- Or:  pig -x local pig/clean_data.pig  (for local testing)

-- ============================================================
-- Step 1: Load raw data
-- ============================================================
raw_data = LOAD '/fooddelivery/raw/food_delivery_data.csv'
    USING PigStorage(',')
    AS (
        order_id:int,
        customer_id:chararray,
        restaurant_id:chararray,
        cuisine:chararray,
        city:chararray,
        area:chararray,
        order_time:chararray,
        delivery_time:chararray,
        delivery_duration_min:float,
        distance_km:float,
        order_value:int,
        discount:int,
        payment_method:chararray,
        delivery_partner_id:chararray,
        partner_rating:float,
        weather:chararray,
        traffic_level:chararray,
        status:chararray,
        customer_rating:float,
        order_hour:int,
        order_day_of_week:chararray,
        is_weekend:int,
        is_delayed:int
    );

-- ============================================================
-- Step 2: Remove header row
-- ============================================================
no_header = FILTER raw_data BY order_id IS NOT NULL AND order_id > 0;

-- ============================================================
-- Step 3: Remove nulls and invalid records
-- ============================================================
-- Filter out records with null critical fields
valid_records = FILTER no_header BY
    customer_id IS NOT NULL AND
    restaurant_id IS NOT NULL AND
    cuisine IS NOT NULL AND
    city IS NOT NULL AND
    order_time IS NOT NULL AND
    distance_km IS NOT NULL AND
    order_value IS NOT NULL AND
    payment_method IS NOT NULL AND
    weather IS NOT NULL AND
    traffic_level IS NOT NULL AND
    status IS NOT NULL;

-- ============================================================
-- Step 4: Filter invalid values
-- ============================================================
-- Remove negative distances, zero/negative order values
-- Remove impossible delivery durations
clean_records = FILTER valid_records BY
    distance_km > 0.0 AND
    distance_km <= 50.0 AND
    order_value > 0 AND
    order_value <= 10000 AND
    discount >= 0 AND
    discount <= order_value AND
    (delivery_duration_min >= 0.0 OR status == 'Cancelled') AND
    delivery_duration_min <= 180.0;

-- ============================================================
-- Step 5: Derive additional fields using FOREACH ... GENERATE
-- ============================================================
enriched = FOREACH clean_records GENERATE
    order_id,
    customer_id,
    restaurant_id,
    cuisine,
    city,
    area,
    order_time,
    delivery_time,
    delivery_duration_min,
    distance_km,
    order_value,
    discount,
    (order_value - discount) AS net_revenue:int,
    payment_method,
    delivery_partner_id,
    partner_rating,
    weather,
    traffic_level,
    status,
    customer_rating,
    order_hour,
    order_day_of_week,
    is_weekend,
    is_delayed,
    -- Derived: speed in km/h (for delivered orders only)
    (delivery_duration_min > 0 ?
        (float)(distance_km / (delivery_duration_min / 60.0)) : 0.0f)
        AS delivery_speed_kmph:float,
    -- Derived: order value bucket
    (order_value < 200 ? 'Low' :
        (order_value < 500 ? 'Medium' :
            (order_value < 1000 ? 'High' : 'Premium')))
        AS order_value_bucket:chararray,
    -- Derived: distance bucket
    (distance_km < 3.0 ? 'Short' :
        (distance_km < 7.0 ? 'Medium' :
            (distance_km < 12.0 ? 'Long' : 'Very Long')))
        AS distance_bucket:chararray,
    -- Derived: time of day category
    (order_hour >= 6 AND order_hour < 11 ? 'Morning' :
        (order_hour >= 11 AND order_hour < 14 ? 'Lunch' :
            (order_hour >= 14 AND order_hour < 17 ? 'Afternoon' :
                (order_hour >= 17 AND order_hour < 22 ? 'Evening' : 'Night'))))
        AS time_of_day:chararray;

-- ============================================================
-- Step 6: Remove duplicates by order_id
-- ============================================================
grouped_by_id = GROUP enriched BY order_id;
deduplicated = FOREACH grouped_by_id {
    first_record = LIMIT enriched 1;
    GENERATE FLATTEN(first_record);
};

-- ============================================================
-- Step 7: Store clean data back to HDFS
-- ============================================================
STORE deduplicated INTO '/fooddelivery/clean'
    USING PigStorage(',');

-- ============================================================
-- Step 8: Generate summary statistics
-- ============================================================
-- Total record count
grouped_all = GROUP deduplicated ALL;
total_count = FOREACH grouped_all GENERATE
    'TOTAL_RECORDS' AS metric:chararray,
    COUNT(deduplicated) AS value:long;
STORE total_count INTO '/fooddelivery/output/pig_stats/total_count'
    USING PigStorage('\t');

-- Records per status
status_groups = GROUP deduplicated BY status;
status_counts = FOREACH status_groups GENERATE
    group AS status:chararray,
    COUNT(deduplicated) AS count:long;
STORE status_counts INTO '/fooddelivery/output/pig_stats/status_counts'
    USING PigStorage('\t');

-- Records per city
city_groups = GROUP deduplicated BY city;
city_counts = FOREACH city_groups GENERATE
    group AS city:chararray,
    COUNT(deduplicated) AS count:long;
STORE city_counts INTO '/fooddelivery/output/pig_stats/city_counts'
    USING PigStorage('\t');

DUMP total_count;
DUMP status_counts;
