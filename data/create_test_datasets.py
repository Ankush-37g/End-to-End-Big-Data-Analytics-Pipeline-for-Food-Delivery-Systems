"""
Generate 3 distinct real test datasets from the raw Kaggle food delivery data:
1. data/test_datasets/dataset_1_metro_rush.csv (~8,000 rows from Bangalore, Mumbai, Delhi during peak hours)
2. data/test_datasets/dataset_2_storm_weather.csv (~7,000 rows with Stormy, Fog, and Jam conditions)
3. data/test_datasets/dataset_3_quick_5k.csv (5,000 representative rows across all cities for quick testing)
"""
import os
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_KAGGLE = os.path.join(PROJECT_ROOT, "data", "kaggle_raw_train.csv")
TEST_DIR = os.path.join(PROJECT_ROOT, "data", "test_datasets")
os.makedirs(TEST_DIR, exist_ok=True)

print(f"Reading {RAW_KAGGLE}...")
df_raw = pd.read_csv(RAW_KAGGLE)
print(f"Total raw rows: {len(df_raw)}")

# 1. Metro Rush Dataset (Bangalore 'BANG', Mumbai 'MUM', Delhi 'DEL', Jaipur 'JAP')
metro_mask = df_raw['Delivery_person_ID'].astype(str).str.startswith(('BANG', 'MUM', 'DEL', 'JAP'))
df_metro = df_raw[metro_mask].copy()
p1 = os.path.join(TEST_DIR, "dataset_1_metro_rush.csv")
df_metro.to_csv(p1, index=False)
print(f"Created Dataset 1 (Metro Rush): {p1} ({len(df_metro)} rows, {os.path.getsize(p1) / 1024 / 1024:.2f} MB)")

# 2. Severe Weather & High Traffic Dataset
weather_mask = df_raw['Weatherconditions'].astype(str).str.contains('Stormy|Fog|Windy', case=False, na=False)
traffic_mask = df_raw['Road_traffic_density'].astype(str).str.contains('Jam|High', case=False, na=False)
df_storm = df_raw[weather_mask | traffic_mask].copy()
p2 = os.path.join(TEST_DIR, "dataset_2_storm_weather.csv")
df_storm.to_csv(p2, index=False)
print(f"Created Dataset 2 (Storm & Heavy Traffic): {p2} ({len(df_storm)} rows, {os.path.getsize(p2) / 1024 / 1024:.2f} MB)")

# 3. Quick 5k Test Dataset (Random stratified sample for instant testing)
df_5k = df_raw.sample(n=min(5000, len(df_raw)), random_state=42).copy()
p3 = os.path.join(TEST_DIR, "dataset_3_quick_5k.csv")
df_5k.to_csv(p3, index=False)
print(f"Created Dataset 3 (Quick 5k Sample): {p3} ({len(df_5k)} rows, {os.path.getsize(p3) / 1024 / 1024:.2f} MB)")

print("\nAll 3 test datasets created successfully in data/test_datasets/!")
