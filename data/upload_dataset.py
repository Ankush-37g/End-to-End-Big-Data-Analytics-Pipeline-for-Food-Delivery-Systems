"""
Upload / Ingest Dataset Tool for FeastFlow Big Data Pipeline
Allows the user to upload any Kaggle food delivery dataset CSV file
and automatically validates, parses, and prepares it for the pipeline.

Usage:
  python data/upload_dataset.py <path_to_kaggle_csv>
  python data/upload_dataset.py --download   (downloads real Kaggle 45,593-row dataset)
"""

import os
import sys
import argparse
import shutil
import pandas as pd
from download_and_prepare_kaggle_data import process_kaggle_data, RAW_KAGGLE_PATH, TARGET_CSV_PATH

# Encoding
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

def upload(file_path=None, download_if_missing=True):
    print("============================================")
    print("  Kaggle Dataset Ingestion & Setup")
    print("============================================")

    if file_path and os.path.exists(file_path):
        print(f"[*] Reading user-uploaded dataset from: {file_path}")
        df = pd.read_csv(file_path)
        print(f"    Uploaded rows: {len(df):,}")
        print(f"    Columns detected: {df.columns.tolist()[:8]}...")

        # If it's a raw Kaggle dataset with 'Delivery_person_ID' and 'Time_taken(min)', parse and process
        if 'Delivery_person_ID' in df.columns or 'Time_taken(min)' in df.columns:
            process_kaggle_data(custom_raw_path=file_path)
        elif 'order_id' in df.columns and 'delivery_duration_min' in df.columns:
            shutil.copyfile(file_path, TARGET_CSV_PATH)
            print(f"[+] Dataset directly matches pipeline schema. Saved to: {TARGET_CSV_PATH}")
        else:
            # Fall back to standard Kaggle processing
            process_kaggle_data(custom_raw_path=file_path)
    else:
        if file_path:
            print(f"[!] File not found at '{file_path}'. Using real Kaggle dataset.")
        print("[*] Processing genuine Kaggle Food Delivery dataset (45,593 rows)...")
        process_kaggle_data()

    print("\n[+] Dataset ingestion complete and ready for pipeline!")
    print(f"    Active Pipeline Data: {TARGET_CSV_PATH}")
    print("    To run all big data phases: powershell -ExecutionPolicy Bypass -File .\\scripts\\run_pipeline.ps1 -Phase all")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Upload Kaggle Dataset")
    parser.add_argument("file", nargs="?", default=None, help="Path to local Kaggle CSV file")
    parser.add_argument("--download", action="store_true", help="Download Kaggle dataset directly")
    args = parser.parse_args()

    upload(args.file)
