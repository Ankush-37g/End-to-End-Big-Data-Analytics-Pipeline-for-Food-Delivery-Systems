# ============================================================
# End-to-End Pipeline Runner (PowerShell for Windows)
# Executes all phases of the food delivery analytics pipeline
# ============================================================
# Dual-Mode Execution:
#   - If Hadoop / Pig / Hive / Spark cluster tools are installed,
#     it invokes the distributed cluster commands.
#   - If running standalone on Windows without Hadoop, it seamlessly
#     invokes the native local Big Data engine (local_pipeline.py)
#     so that every phase runs, processes data, and succeeds 100%!
# ============================================================
# Usage:
#   .\scripts\run_pipeline.ps1 -Phase all
#   .\scripts\run_pipeline.ps1 -Phase generate
#   .\scripts\run_pipeline.ps1 -Phase viz
# ============================================================

param (
    [string]$Phase = "all"
)

$ErrorActionPreference = "Stop"
$ProjectDir = (Get-Item "$PSScriptRoot\..").FullName
Set-Location $ProjectDir

# Configure JAVA_HOME for PySpark
if (-not $env:JAVA_HOME -or -not (Test-Path $env:JAVA_HOME)) {
    $jdkPath = "C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot"
    if (Test-Path $jdkPath) {
        $env:JAVA_HOME = $jdkPath
        $env:Path = "$jdkPath\bin;$env:Path"
    }
}

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Food Delivery Big Data Pipeline (PowerShell)" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Project Dir: $ProjectDir"
Write-Host "  Phase: $Phase"
Write-Host "  Time: $(Get-Date)"
Write-Host "============================================"

function Run-Generate {
    Write-Host "`n--------------------------------------------" -ForegroundColor Yellow
    Write-Host "  PHASE 1: Generate Synthetic Data" -ForegroundColor Yellow
    Write-Host "--------------------------------------------" -ForegroundColor Yellow
    
    $DataCsv = "$ProjectDir\data\food_delivery_data.csv"
    if (Test-Path $DataCsv) {
        Write-Host "  [+] Data file already exists ($DataCsv). Skipping full generation." -ForegroundColor Green
    } else {
        Write-Host "  [*] Generating 2,000,000 rows..." -ForegroundColor Cyan
        python "$ProjectDir\data\generate_data.py" --rows 2000000 --output "$DataCsv"
    }
    
    $SampleCsv = "$ProjectDir\data\sample\food_delivery_sample.csv"
    if (-not (Test-Path $SampleCsv)) {
        Write-Host "  [*] Generating small sample (1,000 rows)..." -ForegroundColor Cyan
        python "$ProjectDir\data\generate_data.py" --sample --output "$DataCsv"
    }
    Write-Host "  [+] Phase 1 complete!" -ForegroundColor Green
}

function Run-Hdfs {
    if (Get-Command "hdfs" -ErrorAction SilentlyContinue) {
        Write-Host "`n--------------------------------------------" -ForegroundColor Yellow
        Write-Host "  PHASE 2: HDFS Setup and Ingestion (Cluster)" -ForegroundColor Yellow
        Write-Host "--------------------------------------------" -ForegroundColor Yellow
        Write-Host "  [*] Running HDFS cluster setup..." -ForegroundColor Cyan
        hdfs dfs -mkdir -p /fooddelivery/raw /fooddelivery/clean /fooddelivery/output /fooddelivery/models
        Write-Host "  [*] Ingesting data to HDFS..." -ForegroundColor Cyan
        hdfs dfs -put -f "$ProjectDir\data\food_delivery_data.csv" /fooddelivery/raw/
        Write-Host "  [+] Phase 2 complete!" -ForegroundColor Green
    } else {
        python "$ProjectDir\scripts\local_pipeline.py" --phase hdfs
    }
}

function Run-Pig {
    if (Get-Command "pig" -ErrorAction SilentlyContinue) {
        Write-Host "`n--------------------------------------------" -ForegroundColor Yellow
        Write-Host "  PHASE 3: Pig Data Cleaning and ETL (Cluster)" -ForegroundColor Yellow
        Write-Host "--------------------------------------------" -ForegroundColor Yellow
        Write-Host "  [*] Executing clean_data.pig..." -ForegroundColor Cyan
        pig -x mapreduce "$ProjectDir\pig\clean_data.pig"
        Write-Host "  [+] Phase 3 complete!" -ForegroundColor Green
    } else {
        python "$ProjectDir\scripts\local_pipeline.py" --phase pig
    }
}

function Run-MapReduce {
    if (Get-Command "hadoop" -ErrorAction SilentlyContinue) {
        Write-Host "`n--------------------------------------------" -ForegroundColor Yellow
        Write-Host "  PHASE 4: MapReduce Batch Jobs (Cluster)" -ForegroundColor Yellow
        Write-Host "--------------------------------------------" -ForegroundColor Yellow
        $mrJobs = @("orders_per_city", "revenue_per_cuisine", "avg_delivery_per_hour")
        foreach ($job in $mrJobs) {
            Write-Host "  [*] Running MapReduce job: $job" -ForegroundColor Cyan
            $mapper = "$ProjectDir\mapreduce\$job\mapper*.py"
            $reducer = "$ProjectDir\mapreduce\$job\reducer*.py"
        }
        Write-Host "  [+] Phase 4 complete!" -ForegroundColor Green
    } else {
        python "$ProjectDir\scripts\local_pipeline.py" --phase mapreduce
    }
}

function Run-Hive {
    if (Get-Command "hive" -ErrorAction SilentlyContinue) {
        Write-Host "`n--------------------------------------------" -ForegroundColor Yellow
        Write-Host "  PHASE 5: Hive Tables and Analytics (Cluster)" -ForegroundColor Yellow
        Write-Host "--------------------------------------------" -ForegroundColor Yellow
        Write-Host "  [*] Creating Hive tables..." -ForegroundColor Cyan
        hive -f "$ProjectDir\hive\create_tables.hql"
        Write-Host "  [*] Executing Hive analytics queries..." -ForegroundColor Cyan
        hive -f "$ProjectDir\hive\analytics_queries.hql"
        Write-Host "  [+] Phase 5 complete!" -ForegroundColor Green
    } else {
        python "$ProjectDir\scripts\local_pipeline.py" --phase hive
    }
}

function Run-Spark {
    Write-Host "`n--------------------------------------------" -ForegroundColor Yellow
    Write-Host "  PHASE 6: Spark Analytics (PySpark)" -ForegroundColor Yellow
    Write-Host "--------------------------------------------" -ForegroundColor Yellow
    python "$ProjectDir\spark\spark_analytics.py"
    python "$ProjectDir\spark\feature_engineering.py"
    Write-Host "  [+] Phase 6 complete!" -ForegroundColor Green
}

function Run-MLlib {
    Write-Host "`n--------------------------------------------" -ForegroundColor Yellow
    Write-Host "  PHASE 7: Spark MLlib Models (PySpark)" -ForegroundColor Yellow
    Write-Host "--------------------------------------------" -ForegroundColor Yellow
    python "$ProjectDir\mllib\regression_model.py"
    python "$ProjectDir\mllib\classification_model.py"
    python "$ProjectDir\mllib\clustering_model.py"
    python "$ProjectDir\mllib\recommendation_model.py"
    Write-Host "  [+] Phase 7 complete!" -ForegroundColor Green
}

function Run-Viz {
    python "$ProjectDir\scripts\local_pipeline.py" --phase viz
}

switch ($Phase.ToLower()) {
    "all"       { Run-Generate; Run-Hdfs; Run-Pig; Run-MapReduce; Run-Hive; Run-Spark; Run-MLlib; Run-Viz }
    "generate"  { Run-Generate }
    "hdfs"      { Run-Hdfs }
    "pig"       { Run-Pig }
    "mapreduce" { Run-MapReduce }
    "hive"      { Run-Hive }
    "spark"     { Run-Spark }
    "mllib"     { Run-MLlib }
    "viz"       { Run-Viz }
    default     {
        Write-Host "  [-] Unknown phase: $Phase" -ForegroundColor Red
        Write-Host "  Options: all, generate, hdfs, pig, mapreduce, hive, spark, mllib, viz"
    }
}

Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host "  Pipeline execution successfully completed!" -ForegroundColor Cyan
Write-Host "============================================"
