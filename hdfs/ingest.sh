#!/bin/bash
# ============================================================
# HDFS Data Ingestion Script
# Uploads food delivery data to HDFS raw zone
# ============================================================

if [ -z "$1" ]; then
    echo "Usage: ./ingest.sh <path_to_csv_file>"
    echo "Example: ./ingest.sh ../data/food_delivery_data.csv"
    exit 1
fi

INPUT_FILE=$1
HDFS_RAW="/fooddelivery/raw"

echo "============================================"
echo "  Food Delivery Pipeline - Data Ingestion"
echo "============================================"
echo ""

# Check if file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "❌ Error: File '$INPUT_FILE' not found!"
    exit 1
fi

# File info
FILE_SIZE=$(du -h "$INPUT_FILE" | cut -f1)
LINE_COUNT=$(wc -l < "$INPUT_FILE")
echo "📄 Input file: $INPUT_FILE"
echo "📦 File size: $FILE_SIZE"
echo "📊 Line count: $LINE_COUNT"
echo ""

# Upload to HDFS
echo "⬆️  Uploading to HDFS: ${HDFS_RAW}/"
hdfs dfs -put -f "$INPUT_FILE" ${HDFS_RAW}/

echo ""
echo "✅ Upload complete!"
echo ""

# Verify upload
echo "📁 HDFS raw zone contents:"
hdfs dfs -ls -h ${HDFS_RAW}/
echo ""

# Show block information
FILENAME=$(basename "$INPUT_FILE")
echo "🧱 Block information for ${FILENAME}:"
hdfs fsck ${HDFS_RAW}/${FILENAME} -files -blocks -locations 2>/dev/null | tail -10
echo ""

# Show replication
echo "🔄 Replication info:"
hdfs dfs -stat "%r" ${HDFS_RAW}/${FILENAME} 2>/dev/null && echo " replicas"
echo ""

echo "============================================"
echo "  Ingestion complete! Data ready in HDFS."
echo "  Path: ${HDFS_RAW}/${FILENAME}"
echo "============================================"
