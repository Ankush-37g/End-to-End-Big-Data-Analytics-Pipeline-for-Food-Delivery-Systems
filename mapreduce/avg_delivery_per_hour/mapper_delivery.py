#!/usr/bin/env python3
"""
Hadoop Streaming: Average Delivery Time Per Hour - Mapper

IMPORTANT: This mapper expects PIG-CLEANED data (28 columns), NOT raw CSV (23 columns).
In raw CSV, Pig has not yet inserted 'net_revenue' (col 12), so column offsets differ!

Pig-cleaned schema (28 columns):
  0:order_id, 1:customer_id, 2:restaurant_id, 3:cuisine, 4:city, 5:area,
  6:order_time, 7:delivery_time, 8:delivery_duration_min, 9:distance_km,
  10:order_value, 11:discount, 12:net_revenue, 13:payment_method,
  14:delivery_partner_id, 15:partner_rating, 16:weather, 17:traffic_level,
  18:status, 19:customer_rating, 20:order_hour, 21:order_day_of_week,
  22:is_weekend, 23:is_delayed, 24:delivery_speed_kmph,
  25:order_value_bucket, 26:distance_bucket, 27:time_of_day

Columns used:
  fields[8]:  delivery_duration_min
  fields[18]: status (filter for 'Delivered')
  fields[20]: order_hour (0-23)
"""

import sys

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith('order_id'):
        continue
    
    fields = line.split(',')
    if len(fields) < 21:
        continue
    
    status = fields[18].strip()
    if status.lower() != 'delivered':
        continue
    
    try:
        duration = float(fields[8].strip())
        hour = int(fields[20].strip())
        
        if duration > 0 and 0 <= hour <= 23:
            # Emit hour and duration (sum:count:min:max format for combiner)
            print(f"{hour:02d}\t{duration:.2f}")
    except (ValueError, IndexError):
        continue
