#!/bin/bash
# ============================================================
# HDFS Directory Setup Script
# Creates the directory structure for the food delivery pipeline
# ============================================================

echo "============================================"
echo "  Food Delivery Pipeline - HDFS Setup"
echo "============================================"

# Base directory
BASE_DIR="/fooddelivery"

echo ""
echo "📁 Creating HDFS directory structure..."

# Create directories
hdfs dfs -mkdir -p ${BASE_DIR}/raw
hdfs dfs -mkdir -p ${BASE_DIR}/clean
hdfs dfs -mkdir -p ${BASE_DIR}/output/orders_per_city
hdfs dfs -mkdir -p ${BASE_DIR}/output/revenue_per_cuisine
hdfs dfs -mkdir -p ${BASE_DIR}/output/avg_delivery_per_hour
hdfs dfs -mkdir -p ${BASE_DIR}/output/spark
hdfs dfs -mkdir -p ${BASE_DIR}/output/mllib
hdfs dfs -mkdir -p ${BASE_DIR}/hive/text
hdfs dfs -mkdir -p ${BASE_DIR}/hive/orc
hdfs dfs -mkdir -p ${BASE_DIR}/hive/partitioned

echo ""
echo "✅ Directory structure created:"
hdfs dfs -ls -R ${BASE_DIR} | head -20

echo ""
echo "📊 HDFS Status:"
hdfs dfs -df -h /

echo ""
echo "🔧 HDFS Configuration:"
echo "  Block size: $(hdfs getconf -confKey dfs.blocksize 2>/dev/null || echo '128MB (default)')"
echo "  Replication: $(hdfs getconf -confKey dfs.replication 2>/dev/null || echo '1 (pseudo-distributed)')"

echo ""
echo "============================================"
echo "  Setup complete! Ready for data ingestion."
echo "============================================"
