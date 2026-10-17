-- ============================================================
-- Hive: Analytical Queries
-- Phase 4: SQL analytics on the food delivery warehouse
-- ============================================================
-- Run: hive -f hive/analytics_queries.hql

USE fooddelivery;

-- ============================================================
-- Query 1: Top 10 Restaurants by Revenue
-- ============================================================
SELECT
    restaurant_id,
    cuisine,
    city,
    area,
    COUNT(*) AS total_orders,
    SUM(net_revenue) AS total_revenue,
    ROUND(AVG(net_revenue), 2) AS avg_order_value,
    ROUND(AVG(customer_rating), 2) AS avg_rating
FROM orders_orc
WHERE status = 'Delivered'
GROUP BY restaurant_id, cuisine, city, area
ORDER BY total_revenue DESC
LIMIT 10;

-- ============================================================
-- Query 2: Peak Hours Analysis (GROUP BY hour)
-- ============================================================
SELECT
    order_hour,
    time_of_day,
    COUNT(*) AS total_orders,
    SUM(net_revenue) AS total_revenue,
    ROUND(AVG(delivery_duration_min), 2) AS avg_delivery_time,
    ROUND(AVG(distance_km), 2) AS avg_distance,
    SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_orders,
    ROUND(SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        AS delay_rate_pct
FROM orders_orc
WHERE status = 'Delivered'
GROUP BY order_hour, time_of_day
ORDER BY order_hour;

-- ============================================================
-- Query 3: Revenue by Cuisine
-- ============================================================
SELECT
    cuisine,
    COUNT(*) AS total_orders,
    SUM(net_revenue) AS total_revenue,
    ROUND(AVG(net_revenue), 2) AS avg_revenue,
    ROUND(SUM(net_revenue) * 100.0 / 
        (SELECT SUM(net_revenue) FROM orders_orc WHERE status = 'Delivered'), 2)
        AS revenue_share_pct
FROM orders_orc
WHERE status = 'Delivered'
GROUP BY cuisine
ORDER BY total_revenue DESC;

-- ============================================================
-- Query 4: Cancellation Rate by Payment Method
-- ============================================================
SELECT
    payment_method,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
    SUM(CASE WHEN status = 'Delivered' THEN 1 ELSE 0 END) AS delivered_orders,
    ROUND(SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        AS cancellation_rate_pct
FROM orders_orc
GROUP BY payment_method
ORDER BY cancellation_rate_pct DESC;

-- ============================================================
-- Query 5: Delay Rate by Weather and Traffic Conditions
-- ============================================================
SELECT
    weather,
    traffic_level,
    COUNT(*) AS total_orders,
    ROUND(AVG(delivery_duration_min), 2) AS avg_delivery_min,
    SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_orders,
    ROUND(SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        AS delay_rate_pct
FROM orders_orc
WHERE status = 'Delivered'
GROUP BY weather, traffic_level
ORDER BY delay_rate_pct DESC;

-- ============================================================
-- Query 6: City-wise Performance Summary
-- ============================================================
SELECT
    city,
    COUNT(*) AS total_orders,
    SUM(net_revenue) AS total_revenue,
    ROUND(AVG(delivery_duration_min), 2) AS avg_delivery_min,
    ROUND(AVG(customer_rating), 2) AS avg_customer_rating,
    ROUND(SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        AS cancellation_rate_pct,
    ROUND(SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) * 100.0 / 
        SUM(CASE WHEN status = 'Delivered' THEN 1 ELSE 0 END), 2)
        AS delay_rate_pct
FROM orders_orc
GROUP BY city
ORDER BY total_revenue DESC;

-- ============================================================
-- Query 7: Day of Week Analysis
-- ============================================================
SELECT
    order_day_of_week,
    is_weekend,
    COUNT(*) AS total_orders,
    SUM(net_revenue) AS total_revenue,
    ROUND(AVG(net_revenue), 2) AS avg_order_value,
    ROUND(AVG(delivery_duration_min), 2) AS avg_delivery_min
FROM orders_orc
WHERE status = 'Delivered'
GROUP BY order_day_of_week, is_weekend
ORDER BY total_orders DESC;

-- ============================================================
-- Query 8: Window Function - Customer Running Spend
--          Shows each customer's cumulative spend over time
-- ============================================================
SELECT
    customer_id,
    order_id,
    order_time,
    net_revenue,
    SUM(net_revenue) OVER (
        PARTITION BY customer_id
        ORDER BY order_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_spend,
    COUNT(*) OVER (
        PARTITION BY customer_id
        ORDER BY order_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_order_count,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY order_time
    ) AS order_sequence
FROM orders_orc
WHERE status = 'Delivered'
    AND customer_id IN (
        SELECT customer_id 
        FROM orders_orc 
        GROUP BY customer_id 
        HAVING COUNT(*) > 20
    )
ORDER BY customer_id, order_time
LIMIT 100;

-- ============================================================
-- Query 9: Window Function - Restaurant Rank Within City
--          Ranks restaurants by revenue within each city
-- ============================================================
SELECT
    city,
    restaurant_id,
    cuisine,
    total_revenue,
    total_orders,
    avg_rating,
    RANK() OVER (
        PARTITION BY city 
        ORDER BY total_revenue DESC
    ) AS revenue_rank,
    DENSE_RANK() OVER (
        PARTITION BY city 
        ORDER BY avg_rating DESC
    ) AS rating_rank
FROM (
    SELECT
        city,
        restaurant_id,
        cuisine,
        SUM(net_revenue) AS total_revenue,
        COUNT(*) AS total_orders,
        ROUND(AVG(customer_rating), 2) AS avg_rating
    FROM orders_orc
    WHERE status = 'Delivered'
    GROUP BY city, restaurant_id, cuisine
) restaurant_stats
ORDER BY city, revenue_rank
LIMIT 50;

-- ============================================================
-- Query 10: Window Function - Moving Average of Delivery Time
--           per hour (trend analysis)
-- ============================================================
SELECT
    order_hour,
    order_count,
    avg_delivery_min,
    ROUND(AVG(avg_delivery_min) OVER (
        ORDER BY order_hour
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ), 2) AS moving_avg_3hr
FROM (
    SELECT
        order_hour,
        COUNT(*) AS order_count,
        ROUND(AVG(delivery_duration_min), 2) AS avg_delivery_min
    FROM orders_orc
    WHERE status = 'Delivered'
    GROUP BY order_hour
) hourly_stats
ORDER BY order_hour;

-- ============================================================
-- Query 11: Discount Effectiveness Analysis
-- ============================================================
SELECT
    CASE
        WHEN discount = 0 THEN 'No Discount'
        WHEN discount > 0 AND discount <= order_value * 0.1 THEN '1-10%'
        WHEN discount > order_value * 0.1 AND discount <= order_value * 0.2 THEN '11-20%'
        ELSE '21%+'
    END AS discount_bracket,
    COUNT(*) AS total_orders,
    ROUND(AVG(net_revenue), 2) AS avg_net_revenue,
    ROUND(AVG(customer_rating), 2) AS avg_rating,
    ROUND(SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        AS cancellation_rate_pct
FROM orders_orc
GROUP BY
    CASE
        WHEN discount = 0 THEN 'No Discount'
        WHEN discount > 0 AND discount <= order_value * 0.1 THEN '1-10%'
        WHEN discount > order_value * 0.1 AND discount <= order_value * 0.2 THEN '11-20%'
        ELSE '21%+'
    END
ORDER BY total_orders DESC;

-- ============================================================
-- Query 12: Delivery Partner Performance
-- ============================================================
SELECT
    delivery_partner_id,
    partner_rating,
    COUNT(*) AS total_deliveries,
    ROUND(AVG(delivery_duration_min), 2) AS avg_delivery_min,
    ROUND(AVG(customer_rating), 2) AS avg_customer_rating,
    SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_deliveries,
    ROUND(SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        AS delay_rate_pct
FROM orders_orc
WHERE status = 'Delivered'
GROUP BY delivery_partner_id, partner_rating
HAVING COUNT(*) > 50
ORDER BY avg_delivery_min ASC
LIMIT 20;
