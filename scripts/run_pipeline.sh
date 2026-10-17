#!/bin/bash
# ============================================================
# End-to-End Pipeline Runner
# Executes all phases of the food delivery analytics pipeline
# ============================================================
# Usage: ./scripts/run_pipeline.sh [phase]
# Phases: all, generate, hdfs, pig, mapreduce, hive, spark, mllib, viz

set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

PHASE=${1:-all}

echo "============================================"
echo "  🍔 Food Delivery Big Data Pipeline"
echo "============================================"
echo "  Project Dir: $PROJECT_DIR"
echo "  Phase: $PHASE"
echo "  Time: $(date)"
echo "============================================"

# ============================================================
# Phase 1: Generate Data
# ============================================================
run_generate() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  PHASE 1: Generate Synthetic Data"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    cd "$PROJECT_DIR/data"
    
    if [ -f "food_delivery_data.csv" ]; then
        echo "  📄 Data file already exists. Skipping generation."
        echo "     To regenerate: rm data/food_delivery_data.csv && rerun"
    else
        echo "  🔄 Generating 2M rows..."
        python generate_data.py --rows 2000000 --output food_delivery_data.csv
    fi
    
    # Also generate a small sample
    if [ ! -d "sample" ]; then
        echo "  🔄 Generating 1K sample..."
        python generate_data.py --sample --output food_delivery_data.csv
    fi
    
    echo "  ✅ Phase 1 complete!"
}

# ============================================================
# Phase 2: HDFS Setup & Ingestion
# ============================================================
run_hdfs() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  PHASE 2: HDFS Setup & Ingestion"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    cd "$PROJECT_DIR/hdfs"
    chmod +x setup_hdfs.sh ingest.sh
    
    echo "  🔄 Setting up HDFS directories..."
    ./setup_hdfs.sh
    
    echo "  🔄 Ingesting data..."
    ./ingest.sh "$PROJECT_DIR/data/food_delivery_data.csv"
    
    echo "  ✅ Phase 2 complete!"
}

# ============================================================
# Phase 3: Pig Cleaning
# ============================================================
run_pig() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  PHASE 3: Pig Data Cleaning & ETL"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    echo "  🔄 Running Pig cleaning script..."
    pig -x mapreduce "$PROJECT_DIR/pig/clean_data.pig"
    
    echo "  📊 Clean data in HDFS:"
    hdfs dfs -ls -h /fooddelivery/clean/
    
    echo "  ✅ Phase 3 complete!"
}

# ============================================================
# Phase 4: MapReduce Jobs
# ============================================================
run_mapreduce() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  PHASE 4: MapReduce Batch Jobs"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    MR_DIR="$PROJECT_DIR/mapreduce"
    
    # Compile and run each MapReduce job
    for job_dir in orders_per_city revenue_per_cuisine avg_delivery_per_hour; do
        echo ""
        echo "  🔄 Building: $job_dir"
        cd "$MR_DIR/$job_dir"
        
        # Find the Java file
        JAVA_FILE=$(ls *.java 2>/dev/null | head -1)
        CLASS_NAME=$(basename "$JAVA_FILE" .java)
        
        # Compile
        mkdir -p build
        javac -classpath "$(hadoop classpath)" -d build "$JAVA_FILE"
        jar cf "${CLASS_NAME}.jar" -C build .
        
        # Remove old output
        hdfs dfs -rm -r -f "/fooddelivery/output/$job_dir"
        
        # Run
        echo "  🔄 Running: $CLASS_NAME"
        START_TIME=$(date +%s)
        hadoop jar "${CLASS_NAME}.jar" "$CLASS_NAME" /fooddelivery/clean "/fooddelivery/output/$job_dir"
        END_TIME=$(date +%s)
        ELAPSED=$((END_TIME - START_TIME))
        
        echo "  ⏱️  Execution time: ${ELAPSED}s"
        echo "  📊 Output:"
        hdfs dfs -cat "/fooddelivery/output/$job_dir/part-r-00000" | head -10
    done
    
    echo ""
    echo "  ✅ Phase 4 complete!"
}

# ============================================================
# Phase 5: Hive
# ============================================================
run_hive() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  PHASE 5: Hive Tables & Analytics"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    echo "  🔄 Creating tables..."
    hive -f "$PROJECT_DIR/hive/create_tables.hql"
    
    echo ""
    echo "  🔄 Running analytics queries..."
    hive -f "$PROJECT_DIR/hive/analytics_queries.hql"
    
    echo ""
    echo "  🔄 Running performance comparison..."
    hive -f "$PROJECT_DIR/hive/performance_compare.hql"
    
    echo "  ✅ Phase 5 complete!"
}

# ============================================================
# Phase 6: Spark Analytics
# ============================================================
run_spark() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  PHASE 6: Spark Analytics"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    echo "  🔄 Running Spark analytics..."
    spark-submit "$PROJECT_DIR/spark/spark_analytics.py"
    
    echo ""
    echo "  🔄 Running feature engineering..."
    spark-submit "$PROJECT_DIR/spark/feature_engineering.py"
    
    echo ""
    echo "  🔄 Running performance comparison..."
    spark-submit "$PROJECT_DIR/spark/performance_compare.py"
    
    echo "  ✅ Phase 6 complete!"
}

# ============================================================
# Phase 7: MLlib Models
# ============================================================
run_mllib() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  PHASE 7: Spark MLlib Models"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    echo "  🔄 Training regression models..."
    spark-submit "$PROJECT_DIR/mllib/regression_model.py"
    
    echo ""
    echo "  🔄 Training classification models..."
    spark-submit "$PROJECT_DIR/mllib/classification_model.py"
    
    echo ""
    echo "  🔄 Running customer clustering..."
    spark-submit "$PROJECT_DIR/mllib/clustering_model.py"
    
    echo ""
    echo "  🔄 Training recommendation model..."
    spark-submit "$PROJECT_DIR/mllib/recommendation_model.py"
    
    echo "  ✅ Phase 7 complete!"
}

# ============================================================
# Phase 8: Visualization
# ============================================================
run_viz() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  PHASE 8: Visualization"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    cd "$PROJECT_DIR"
    python visualization/generate_charts.py
    
    echo "  ✅ Phase 8 complete!"
}

# ============================================================
# Run selected phase(s)
# ============================================================
case $PHASE in
    all)
        run_generate
        run_hdfs
        run_pig
        run_mapreduce
        run_hive
        run_spark
        run_mllib
        run_viz
        ;;
    generate)  run_generate ;;
    hdfs)      run_hdfs ;;
    pig)       run_pig ;;
    mapreduce) run_mapreduce ;;
    hive)      run_hive ;;
    spark)     run_spark ;;
    mllib)     run_mllib ;;
    viz)       run_viz ;;
    *)
        echo "  ❌ Unknown phase: $PHASE"
        echo "  Usage: $0 [all|generate|hdfs|pig|mapreduce|hive|spark|mllib|viz]"
        exit 1
        ;;
esac

echo ""
echo "============================================"
echo "  🎉 Pipeline execution complete!"
echo "  Time: $(date)"
echo "============================================"
