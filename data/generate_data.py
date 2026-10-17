#!/usr/bin/env python3
"""
Food Delivery Synthetic Data Generator
Generates 1-5 million realistic food delivery records using Faker.
Patterns are intentionally planted for ML discovery:
  - Longer delays during rain/heavy traffic
  - Peak hours at lunch (12-2PM) and dinner (7-10PM)
  - Higher cancellation rates for COD
  - Distance correlates with delivery time
"""

import argparse
import csv
import os
import random
import sys

# Configure UTF-8 encoding for Windows console/redirection compatibility
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

from datetime import datetime, timedelta

try:
    from faker import Faker
except ImportError:
    print("Installing faker...")
    os.system(f"{sys.executable} -m pip install faker")
    from faker import Faker

try:
    import numpy as np
except ImportError:
    print("Installing numpy...")
    os.system(f"{sys.executable} -m pip install numpy")
    import numpy as np

fake = Faker('en_IN')
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# ============================================================
# Configuration - Indian cities, cuisines, areas
# ============================================================

CITIES_AREAS = {
    'Mumbai': ['Andheri', 'Bandra', 'Juhu', 'Powai', 'Dadar', 'Worli', 'Malad', 'Goregaon', 'Borivali', 'Thane'],
    'Delhi': ['Connaught Place', 'Hauz Khas', 'Saket', 'Karol Bagh', 'Dwarka', 'Rohini', 'Lajpat Nagar', 'Rajouri Garden', 'Janakpuri', 'Pitampura'],
    'Bangalore': ['Koramangala', 'Indiranagar', 'HSR Layout', 'Whitefield', 'MG Road', 'Jayanagar', 'BTM Layout', 'Electronic City', 'Marathahalli', 'Hebbal'],
    'Hyderabad': ['Banjara Hills', 'Jubilee Hills', 'Madhapur', 'Hitech City', 'Gachibowli', 'Ameerpet', 'Secunderabad', 'Kukatpally', 'Kondapur', 'Begumpet'],
    'Chennai': ['T Nagar', 'Anna Nagar', 'Adyar', 'Velachery', 'Besant Nagar', 'Guindy', 'Nungambakkam', 'Mylapore', 'Porur', 'Tambaram'],
    'Kolkata': ['Salt Lake', 'Park Street', 'New Town', 'Howrah', 'Ballygunge', 'Alipore', 'Gariahat', 'Dum Dum', 'Behala', 'Jadavpur'],
    'Pune': ['Koregaon Park', 'Viman Nagar', 'Hinjewadi', 'Kothrud', 'Baner', 'Aundh', 'Hadapsar', 'Wakad', 'Pimpri', 'Deccan'],
    'Kochi': ['Edappally', 'Kakkanad', 'Fort Kochi', 'Kaloor', 'Palarivattom', 'Aluva', 'Vytilla', 'Thripunithura', 'Maradu', 'Kalamassery'],
    'Jaipur': ['MI Road', 'Vaishali Nagar', 'Malviya Nagar', 'Mansarovar', 'C-Scheme', 'Raja Park', 'Tonk Road', 'Jagatpura', 'Sitapura', 'Sanganer'],
    'Ahmedabad': ['CG Road', 'SG Highway', 'Satellite', 'Navrangpura', 'Prahlad Nagar', 'Bodakdev', 'Maninagar', 'Vastrapur', 'Ambawadi', 'Gota'],
}

CUISINES = [
    'North Indian', 'South Indian', 'Chinese', 'Italian', 'Biryani',
    'Fast Food', 'Street Food', 'Mughlai', 'Continental', 'Thai',
    'Mexican', 'Japanese', 'Korean', 'Desserts', 'Beverages',
    'Healthy Food', 'Seafood', 'Bengali', 'Punjabi', 'Rajasthani'
]

PAYMENT_METHODS = ['UPI', 'Credit Card', 'Debit Card', 'COD', 'Wallet']
WEATHER_CONDITIONS = ['Clear', 'Cloudy', 'Rainy', 'Stormy', 'Foggy']
TRAFFIC_LEVELS = ['Low', 'Medium', 'High', 'Jam']
STATUSES = ['Delivered', 'Cancelled']

# Weights for realistic distributions
CUISINE_WEIGHTS = [15, 12, 10, 7, 14, 12, 8, 6, 4, 3, 2, 1, 1, 3, 2, 3, 2, 2, 5, 3]
PAYMENT_WEIGHTS = [40, 15, 15, 20, 10]
WEATHER_WEIGHTS = [45, 25, 18, 5, 7]
TRAFFIC_WEIGHTS = [25, 35, 28, 12]

# Restaurant data
NUM_RESTAURANTS = 500
NUM_CUSTOMERS = 50000
NUM_DELIVERY_PARTNERS = 2000


def generate_restaurants():
    """Pre-generate restaurant pool with fixed attributes."""
    restaurants = []
    for i in range(1, NUM_RESTAURANTS + 1):
        city = random.choice(list(CITIES_AREAS.keys()))
        area = random.choice(CITIES_AREAS[city])
        cuisine = random.choices(CUISINES, weights=CUISINE_WEIGHTS, k=1)[0]
        avg_prep_time = random.randint(10, 35)  # minutes
        restaurants.append({
            'id': f'R{i:04d}',
            'city': city,
            'area': area,
            'cuisine': cuisine,
            'avg_prep_time': avg_prep_time,
            'base_rating': round(random.uniform(3.0, 5.0), 1)
        })
    return restaurants


def generate_hour_probability():
    """
    Generate order probability by hour.
    Peaks at lunch (12-14) and dinner (19-22).
    """
    probs = np.zeros(24)
    for h in range(24):
        if 0 <= h < 6:
            probs[h] = 0.5
        elif 6 <= h < 8:
            probs[h] = 2.0
        elif 8 <= h < 11:
            probs[h] = 5.0
        elif 11 <= h < 14:
            probs[h] = 15.0  # Lunch peak
        elif 14 <= h < 17:
            probs[h] = 6.0
        elif 17 <= h < 19:
            probs[h] = 10.0
        elif 19 <= h < 22:
            probs[h] = 18.0  # Dinner peak
        elif 22 <= h < 24:
            probs[h] = 4.0
    probs = probs / probs.sum()
    return probs


def compute_delivery_time(distance_km, weather, traffic, prep_time, hour):
    """
    Compute realistic delivery time with planted patterns:
    - Base time = prep_time + distance * speed_factor
    - Rain adds 5-15 min
    - Heavy traffic adds 5-20 min
    - Peak hours add 3-10 min
    - Random noise
    """
    # Base speed: 15-25 km/h in city
    speed = random.uniform(15, 25)
    travel_time = (distance_km / speed) * 60  # minutes

    base_time = prep_time + travel_time

    # Weather impact
    weather_delay = 0
    if weather == 'Rainy':
        weather_delay = random.uniform(5, 15)
    elif weather == 'Stormy':
        weather_delay = random.uniform(10, 25)
    elif weather == 'Foggy':
        weather_delay = random.uniform(3, 10)

    # Traffic impact
    traffic_delay = 0
    if traffic == 'High':
        traffic_delay = random.uniform(5, 15)
    elif traffic == 'Jam':
        traffic_delay = random.uniform(10, 25)
    elif traffic == 'Medium':
        traffic_delay = random.uniform(2, 7)

    # Peak hour impact
    peak_delay = 0
    if 12 <= hour <= 14 or 19 <= hour <= 22:
        peak_delay = random.uniform(3, 10)

    # Random noise
    noise = random.gauss(0, 3)

    total = base_time + weather_delay + traffic_delay + peak_delay + noise
    return max(10, round(total, 1))


def should_cancel(payment_method, weather, traffic, distance_km):
    """
    Determine if order is cancelled. Planted patterns:
    - COD has higher cancellation (8%)
    - Stormy weather increases cancellation
    - Very long distances increase cancellation
    """
    base_rate = 0.03  # 3% base

    if payment_method == 'COD':
        base_rate += 0.05
    if weather in ('Stormy', 'Rainy'):
        base_rate += 0.03
    if traffic == 'Jam':
        base_rate += 0.02
    if distance_km > 12:
        base_rate += 0.03

    return random.random() < base_rate


def generate_data(num_rows, output_file, chunk_size=100000):
    """Generate the dataset and write to CSV in chunks."""
    restaurants = generate_restaurants()
    hour_probs = generate_hour_probability()

    # Date range: 1 year of data
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 12, 31)
    date_range_days = (end_date - start_date).days

    fieldnames = [
        'order_id', 'customer_id', 'restaurant_id', 'cuisine', 'city', 'area',
        'order_time', 'delivery_time', 'delivery_duration_min', 'distance_km',
        'order_value', 'discount', 'payment_method', 'delivery_partner_id',
        'partner_rating', 'weather', 'traffic_level', 'status',
        'customer_rating', 'order_hour', 'order_day_of_week', 'is_weekend',
        'is_delayed'
    ]

    print(f"Generating {num_rows:,} rows to {output_file}...")
    print(f"Restaurants: {NUM_RESTAURANTS}, Customers: {NUM_CUSTOMERS:,}, Partners: {NUM_DELIVERY_PARTNERS:,}")

    rows_written = 0
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        while rows_written < num_rows:
            chunk = min(chunk_size, num_rows - rows_written)
            rows = []

            for i in range(chunk):
                order_id = rows_written + i + 1

                # Pick restaurant
                rest = random.choice(restaurants)

                # Pick customer and delivery partner
                customer_id = f'C{random.randint(1, NUM_CUSTOMERS):05d}'
                partner_id = f'D{random.randint(1, NUM_DELIVERY_PARTNERS):04d}'
                partner_rating = round(random.uniform(3.5, 5.0), 1)

                # Generate order time with realistic hour distribution
                day_offset = random.randint(0, date_range_days)
                order_hour = int(np.random.choice(24, p=hour_probs))
                order_minute = random.randint(0, 59)
                order_second = random.randint(0, 59)
                order_dt = start_date + timedelta(
                    days=day_offset,
                    hours=order_hour,
                    minutes=order_minute,
                    seconds=order_second
                )

                # Day of week (0=Monday, 6=Sunday)
                day_of_week = order_dt.strftime('%A')
                is_weekend = 1 if order_dt.weekday() >= 5 else 0

                # Distance (1-20 km, most orders 2-8 km)
                distance_km = round(max(0.5, random.lognormvariate(1.5, 0.5)), 1)
                distance_km = min(distance_km, 25.0)

                # Order value (100-2000 INR)
                base_value = random.randint(100, 1500)
                if rest['cuisine'] in ('Italian', 'Japanese', 'Korean', 'Continental'):
                    base_value = int(base_value * 1.4)  # Premium cuisines
                order_value = min(base_value, 3000)

                # Discount (0-30% of order value, with probability)
                if random.random() < 0.4:
                    discount = int(order_value * random.uniform(0.05, 0.30))
                else:
                    discount = 0

                # Payment method
                payment_method = random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS, k=1)[0]

                # Weather and traffic
                weather = random.choices(WEATHER_CONDITIONS, weights=WEATHER_WEIGHTS, k=1)[0]
                traffic = random.choices(TRAFFIC_LEVELS, weights=TRAFFIC_WEIGHTS, k=1)[0]

                # Delivery time computation
                delivery_duration = compute_delivery_time(
                    distance_km, weather, traffic, rest['avg_prep_time'], order_hour
                )

                # Is delayed? (>45 min is considered delayed)
                is_delayed = 1 if delivery_duration > 45 else 0

                # Check cancellation
                if should_cancel(payment_method, weather, traffic, distance_km):
                    status = 'Cancelled'
                    delivery_time_str = ''
                    delivery_duration = 0
                    customer_rating = 0
                    is_delayed = 0
                else:
                    status = 'Delivered'
                    delivery_dt = order_dt + timedelta(minutes=delivery_duration)
                    delivery_time_str = delivery_dt.strftime('%Y-%m-%d %H:%M:%S')

                    # Customer rating: correlated with delivery time and restaurant quality
                    if delivery_duration < 30:
                        rating_base = random.uniform(3.5, 5.0)
                    elif delivery_duration < 45:
                        rating_base = random.uniform(3.0, 4.5)
                    elif delivery_duration < 60:
                        rating_base = random.uniform(2.0, 4.0)
                    else:
                        rating_base = random.uniform(1.0, 3.5)

                    # Adjust by restaurant quality
                    rating_base = rating_base * 0.7 + rest['base_rating'] * 0.3
                    customer_rating = round(min(5.0, max(1.0, rating_base)), 1)

                row = {
                    'order_id': order_id,
                    'customer_id': customer_id,
                    'restaurant_id': rest['id'],
                    'cuisine': rest['cuisine'],
                    'city': rest['city'],
                    'area': rest['area'],
                    'order_time': order_dt.strftime('%Y-%m-%d %H:%M:%S'),
                    'delivery_time': delivery_time_str,
                    'delivery_duration_min': delivery_duration,
                    'distance_km': distance_km,
                    'order_value': order_value,
                    'discount': discount,
                    'payment_method': payment_method,
                    'delivery_partner_id': partner_id,
                    'partner_rating': partner_rating,
                    'weather': weather,
                    'traffic_level': traffic,
                    'status': status,
                    'customer_rating': customer_rating,
                    'order_hour': order_hour,
                    'order_day_of_week': day_of_week,
                    'is_weekend': is_weekend,
                    'is_delayed': is_delayed
                }
                rows.append(row)

            writer.writerows(rows)
            rows_written += len(rows)

            # Progress
            pct = (rows_written / num_rows) * 100
            print(f"  Progress: {rows_written:,}/{num_rows:,} ({pct:.1f}%)")

    # File size
    file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"\n✅ Done! Generated {rows_written:,} rows")
    print(f"📄 File: {output_file}")
    print(f"📦 Size: {file_size_mb:.1f} MB")


def generate_sample(output_file, num_rows=1000):
    """Generate a small sample for testing."""
    generate_data(num_rows, output_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Food Delivery Synthetic Data Generator')
    parser.add_argument('--rows', type=int, default=2000000,
                        help='Number of rows to generate (default: 2,000,000)')
    parser.add_argument('--output', type=str, default='food_delivery_data.csv',
                        help='Output CSV file path')
    parser.add_argument('--sample', action='store_true',
                        help='Generate a small 1000-row sample instead')

    args = parser.parse_args()

    if args.sample:
        sample_dir = os.path.join(os.path.dirname(args.output) or '.', 'sample')
        os.makedirs(sample_dir, exist_ok=True)
        sample_file = os.path.join(sample_dir, 'food_delivery_sample.csv')
        generate_sample(sample_file)
    else:
        generate_data(args.rows, args.output)
