#!/usr/bin/env python3
"""
Hadoop Streaming: Average Delivery Time Per Hour - Reducer
Computes average, min, max, and count per hour
"""

import sys

current_hour = None
total_sum = 0.0
total_count = 0
current_min = float('inf')
current_max = float('-inf')

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    try:
        hour, duration = line.split('\t')
        duration = float(duration)
    except ValueError:
        continue
    
    if hour == current_hour:
        total_sum += duration
        total_count += 1
        current_min = min(current_min, duration)
        current_max = max(current_max, duration)
    else:
        if current_hour is not None:
            avg = total_sum / total_count
            print(f"{current_hour}:00\t{avg:.2f}\t{total_count}\t{current_min:.2f}\t{current_max:.2f}")
        
        current_hour = hour
        total_sum = duration
        total_count = 1
        current_min = duration
        current_max = duration

# Last hour
if current_hour is not None:
    avg = total_sum / total_count
    print(f"{current_hour}:00\t{avg:.2f}\t{total_count}\t{current_min:.2f}\t{current_max:.2f}")
