# Food Delivery Big Data Pipeline - Makefile

.PHONY: all generate hdfs pig mr hive spark mllib viz clean docker-up docker-down help

PYTHON ?= python3
SHELL := /bin/bash

help:
	@echo "=========================================================="
	@echo "  Food Delivery Big Data Analytics Pipeline"
	@echo "=========================================================="
	@echo "Available commands:"
	@echo "  make generate      - Generate synthetic dataset (Faker)"
	@echo "  make sample        - Generate 1,000-row sample dataset"
	@echo "  make hdfs          - Ingest data into HDFS"
	@echo "  make pig           - Run Pig ETL cleaning script"
	@echo "  make mr            - Compile & execute MapReduce batch jobs"
	@echo "  make hive          - Run Hive table setup & analytics queries"
	@echo "  make spark         - Run Apache Spark analytics & feature engineering"
	@echo "  make mllib         - Train Spark MLlib regression, classification & clustering"
	@echo "  make viz           - Generate publication-quality visualization charts"
	@echo "  make all           - Execute complete pipeline end-to-end"
	@echo "  make docker-up     - Launch Hadoop/Spark multi-container cluster"
	@echo "  make docker-down   - Tear down Docker containers"
	@echo "  make clean         - Remove generated CSVs, JAR builds and charts"

generate:
	$(PYTHON) data/generate_data.py --rows 2000000 --output data/food_delivery_data.csv

sample:
	$(PYTHON) data/generate_data.py --sample --output data/food_delivery_data.csv

hdfs:
	./hdfs/setup_hdfs.sh
	./hdfs/ingest.sh data/food_delivery_data.csv

pig:
	pig -x mapreduce pig/clean_data.pig

mr:
	./scripts/run_pipeline.sh mapreduce

hive:
	hive -f hive/create_tables.hql
	hive -f hive/analytics_queries.hql
	hive -f hive/performance_compare.hql

spark:
	spark-submit spark/spark_analytics.py
	spark-submit spark/feature_engineering.py
	spark-submit spark/performance_compare.py

mllib:
	spark-submit mllib/regression_model.py
	spark-submit mllib/classification_model.py
	spark-submit mllib/clustering_model.py
	spark-submit mllib/recommendation_model.py

viz:
	$(PYTHON) visualization/generate_charts.py

all:
	./scripts/run_pipeline.sh all

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

clean:
	rm -rf data/*.csv data/sample/*.csv mapreduce/*/build mapreduce/*/*.jar visualization/charts/*.png
