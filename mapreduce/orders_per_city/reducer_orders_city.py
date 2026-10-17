#!/usr/bin/env python3
"""
Hadoop Streaming: Reducer for Orders Per City
Aggregates counts per city/area key
"""

import sys

current_key = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    try:
        key, count = line.split('\t')
        count = int(count)
    except ValueError:
        continue
    
    if key == current_key:
        current_count += count
    else:
        if current_key is not None:
            print(f"{current_key}\t{current_count}")
        current_key = key
        current_count = count

# Don't forget the last key
if current_key is not None:
    print(f"{current_key}\t{current_count}")
