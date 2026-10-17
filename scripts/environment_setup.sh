#!/bin/bash
# ============================================================
# Environment Setup Script
# Sets up the development environment for the food delivery pipeline
# ============================================================

echo "============================================"
echo "  Environment Setup"
echo "============================================"

# ============================================================
# 1. Check Java
# ============================================================
echo ""
echo "📋 Checking Java..."
if command -v java &> /dev/null; then
    java -version 2>&1 | head -1
    echo "  ✅ Java is installed"
else
    echo "  ❌ Java not found. Install Java 8 or 11."
    echo "     Ubuntu: sudo apt install openjdk-11-jdk"
    echo "     CentOS: sudo yum install java-11-openjdk-devel"
fi

# ============================================================
# 2. Check Hadoop
# ============================================================
echo ""
echo "📋 Checking Hadoop..."
if command -v hadoop &> /dev/null; then
    hadoop version 2>&1 | head -1
    echo "  ✅ Hadoop is installed"
    echo "  HADOOP_HOME: $HADOOP_HOME"
else
    echo "  ❌ Hadoop not found."
    echo "     Download from: https://hadoop.apache.org/releases.html"
    echo "     Or use Docker: docker pull apache/hadoop:3"
fi

# ============================================================
# 3. Check Pig
# ============================================================
echo ""
echo "📋 Checking Pig..."
if command -v pig &> /dev/null; then
    pig --version 2>&1 | head -1
    echo "  ✅ Pig is installed"
else
    echo "  ❌ Pig not found."
    echo "     Download from: https://pig.apache.org/releases.html"
fi

# ============================================================
# 4. Check Hive
# ============================================================
echo ""
echo "📋 Checking Hive..."
if command -v hive &> /dev/null; then
    hive --version 2>&1 | head -1
    echo "  ✅ Hive is installed"
else
    echo "  ❌ Hive not found."
    echo "     Download from: https://hive.apache.org/downloads.html"
fi

# ============================================================
# 5. Check Spark
# ============================================================
echo ""
echo "📋 Checking Spark..."
if command -v spark-submit &> /dev/null; then
    spark-submit --version 2>&1 | head -3
    echo "  ✅ Spark is installed"
    echo "  SPARK_HOME: $SPARK_HOME"
else
    echo "  ❌ Spark not found."
    echo "     Download from: https://spark.apache.org/downloads.html"
fi

# ============================================================
# 6. Check Python
# ============================================================
echo ""
echo "📋 Checking Python..."
if command -v python3 &> /dev/null; then
    python3 --version
    echo "  ✅ Python3 is installed"
elif command -v python &> /dev/null; then
    python --version
    echo "  ✅ Python is installed"
else
    echo "  ❌ Python not found."
fi

# ============================================================
# 7. Install Python packages
# ============================================================
echo ""
echo "📋 Installing Python packages..."
pip install faker pandas numpy matplotlib seaborn scikit-learn 2>/dev/null || \
pip3 install faker pandas numpy matplotlib seaborn scikit-learn 2>/dev/null || \
python -m pip install faker pandas numpy matplotlib seaborn scikit-learn 2>/dev/null

echo ""
echo "  ✅ Python packages installed"

# ============================================================
# 8. Verify HDFS
# ============================================================
echo ""
echo "📋 Checking HDFS connectivity..."
if hdfs dfs -ls / &> /dev/null; then
    echo "  ✅ HDFS is accessible"
    hdfs dfs -df -h /
else
    echo "  ❌ Cannot connect to HDFS. Ensure Hadoop services are running:"
    echo "     start-dfs.sh"
    echo "     start-yarn.sh"
fi

echo ""
echo "============================================"
echo "  Environment check complete!"
echo "============================================"
echo ""
echo "  To start the pipeline:"
echo "    1. cd $(pwd)/.."
echo "    2. python data/generate_data.py"
echo "    3. ./scripts/run_pipeline.sh all"
echo ""
echo "  Or run individual phases:"
echo "    ./scripts/run_pipeline.sh generate"
echo "    ./scripts/run_pipeline.sh hdfs"
echo "    ./scripts/run_pipeline.sh pig"
echo "    ./scripts/run_pipeline.sh mapreduce"
echo "    ./scripts/run_pipeline.sh hive"
echo "    ./scripts/run_pipeline.sh spark"
echo "    ./scripts/run_pipeline.sh mllib"
echo "    ./scripts/run_pipeline.sh viz"
echo "============================================"
