#!/usr/bin/env python3
"""
Hadoop Streaming: Revenue Per Cuisine Reducer
Computes total revenue, order count, and average revenue per cuisine
"""

import sys

current_cuisine = None
total_revenue = 0.0
order_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    try:
        cuisine, revenue = line.split('\t')
        revenue = float(revenue)
    except ValueError:
        continue
    
    if cuisine == current_cuisine:
        total_revenue += revenue
        order_count += 1
    else:
        if current_cuisine is not None:
            avg_revenue = total_revenue / order_count if order_count > 0 else 0
            print(f"{current_cuisine}\t{total_revenue:.2f}\t{order_count}\t{avg_revenue:.2f}")
        current_cuisine = cuisine
        total_revenue = revenue
        order_count = 1

# Last cuisine
if current_cuisine is not None:
    avg_revenue = total_revenue / order_count if order_count > 0 else 0
    print(f"{current_cuisine}\t{total_revenue:.2f}\t{order_count}\t{avg_revenue:.2f}")
