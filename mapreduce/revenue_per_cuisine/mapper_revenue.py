#!/usr/bin/env python3
"""
Hadoop Streaming: Revenue Per Cuisine Mapper

IMPORTANT: This mapper expects PIG-CLEANED data (28 columns), NOT raw CSV (23 columns).
In raw CSV, 'net_revenue' does not exist yet (Pig adds it as col 12), shifting subsequent columns.

Pig-cleaned schema (28 columns):
  0:order_id, 1:customer_id, 2:restaurant_id, 3:cuisine, 4:city, 5:area,
  6:order_time, 7:delivery_time, 8:delivery_duration_min, 9:distance_km,
  10:order_value, 11:discount, 12:net_revenue, 13:payment_method,
  14:delivery_partner_id, 15:partner_rating, 16:weather, 17:traffic_level,
  18:status, 19:customer_rating, 20:order_hour, 21:order_day_of_week,
  22:is_weekend, 23:is_delayed, 24:delivery_speed_kmph,
  25:order_value_bucket, 26:distance_bucket, 27:time_of_day

Columns used:
  fields[3]:  cuisine
  fields[12]: net_revenue
  fields[18]: status (filter for 'Delivered')
"""

import sys

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith('order_id'):
        continue
    
    fields = line.split(',')
    if len(fields) < 19:
        continue
    
    cuisine = fields[3].strip()
    status = fields[18].strip()
    
    # Only count delivered orders
    if status.lower() != 'delivered':
        continue
    
    try:
        net_revenue = float(fields[12].strip())
        print(f"{cuisine}\t{net_revenue}")
    except (ValueError, IndexError):
        continue
