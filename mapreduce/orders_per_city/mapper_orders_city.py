#!/usr/bin/env python3
"""
Hadoop Streaming: Orders Per City (Python alternative to Java MapReduce)
If you prefer Python over Java for MapReduce, use these mapper/reducer
scripts with Hadoop Streaming.

IMPORTANT: This mapper expects PIG-CLEANED data (28 columns), NOT the
raw CSV (23 columns). The column indices below match the Pig output schema:

  0:order_id, 1:customer_id, 2:restaurant_id, 3:cuisine, 4:city, 5:area,
  6:order_time, 7:delivery_time, 8:delivery_duration_min, 9:distance_km,
  10:order_value, 11:discount, 12:net_revenue, 13:payment_method,
  14:delivery_partner_id, 15:partner_rating, 16:weather, 17:traffic_level,
  18:status, 19:customer_rating, 20:order_hour, 21:order_day_of_week,
  22:is_weekend, 23:is_delayed, 24:delivery_speed_kmph,
  25:order_value_bucket, 26:distance_bucket, 27:time_of_day

Usage:
  hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \\
    -input /fooddelivery/clean \\
    -output /fooddelivery/output/orders_per_city_py \\
    -mapper mapper_orders_city.py \\
    -reducer reducer_orders_city.py \\
    -file mapper_orders_city.py \\
    -file reducer_orders_city.py
"""

import sys

# Mapper: emit (city, 1) for each record
for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith('order_id'):
        continue
    
    fields = line.split(',')
    if len(fields) < 6:
        continue
    
    city = fields[4].strip()
    area = fields[5].strip()
    
    if city:
        print(f"CITY:{city}\t1")
    if city and area:
        print(f"AREA:{city}-{area}\t1")
