Copier

# =============================================================
# JOB INTELLIGENT - Pipeline Runner (One-Shot)
# Usage : .\run_pipeline.ps1
# =============================================================
 
param(
    [string]$Date = (Get-Date -Format "yyyy-MM-dd"),
    [switch]$SkipETL,
    [switch]$ETLOnly
)
 
$ErrorActionPreference = "Stop"
$AIRFLOW_WORKER = "airflow-worker"
$DAG_ID         = "scraping_dag"
$LOG_FILE       = "pipeline_run_$Date.log"
 
function Write-Step { param($msg) Write-Host "`n[STEP] $msg" -ForegroundColor Cyan }
function Write-OK   { param($msg) Write-Host "[OK]   $msg" -ForegroundColor Green }
function Write-Fail { param($msg) Write-Host "[FAIL] $msg" -ForegroundColor Red }
function Write-Info { param($msg) Write-Host "[INFO] $msg" -ForegroundColor Yellow }
 
function Log {
    param($msg)
    $ts = Get-Date -Format "HH:mm:ss"
    $line = "$ts | $msg"
    Add-Content -Path $LOG_FILE -Value $line
    Write-Host $line
}
 
# =============================================================
# ETAPE 0 - Verification Docker
# =============================================================
 
Write-Step "ETAPE 0 - Verification des conteneurs Docker"
Log "=== PIPELINE START | date=$Date ==="
 
$containers = @($AIRFLOW_WORKER, "minio", "postgres-dwh", "kafka")
foreach ($c in $containers) {
    $status = docker inspect --format="{{.State.Running}}" $c 2>$null
    if ($status -ne "true") {
        Write-Fail "Conteneur '$c' non demarre. Lance 'docker-compose up -d' d'abord."
        Log "FAIL: conteneur $c non demarre"
        exit 1
    }
    Write-OK "$c est running"
    Log "OK: $c running"
}
 
# =============================================================
# ETAPE 1 - SCRAPING
# =============================================================
 
if (-not $ETLOnly) {
 
    docker exec airflow-scheduler airflow dags trigger $DAG_ID
    Log "--- SCRAPING START ---"
 
    $tasks = @(
        @{ id = "check_scripts_exist";   desc = "Verification des scripts" },
        @{ id = "scrape_adzuna";         desc = "Scraping Adzuna" },
        @{ id = "scrape_france_travail"; desc = "Scraping France Travail" }
        # @{ id = "scrape_welcometothejungle"; desc = "Scraping WTTJ" }   # <-- DESACTIVE POUR TEST
    )
 
    foreach ($task in $tasks) {
        Write-Info "$($task.desc) [$($task.id)]..."
        Log "TASK START: $($task.id)"
 
        try {
            docker exec $AIRFLOW_WORKER airflow tasks test $DAG_ID $task.id $Date
            if ($LASTEXITCODE -ne 0) { throw "Exit code $LASTEXITCODE" }
            Write-OK "$($task.id) termine"
            Log "TASK OK: $($task.id)"
        }
        catch {
            Write-Fail "$($task.id) a echoue : $_"
            Log "TASK FAIL: $($task.id) | $_"
            $rep = Read-Host "Continuer malgre l'erreur ? (o/N)"
            if ($rep -ne "o") { exit 1 }
        }
    }
 
    Log "--- SCRAPING END ---"
 
} else {
    Write-Info "Mode ETLOnly - scraping ignore."
}
 
# =============================================================
# ETAPE 2 - ETL Bronze Silver Gold PostgreSQL
# =============================================================
 
if (-not $SkipETL) {
 
    Write-Step "ETAPE 2 - ETL Medallion Bronze Silver Gold PostgreSQL"
    Log "--- ETL START ---"
 
    $etlScripts = @(
        @{ script = "scripts\etl_adzuna.py";          desc = "ETL Adzuna" },
        @{ script = "scripts\etl_france_travail.py";  desc = "ETL France Travail" },
        # @{ script = "scripts\etl_welcomeJungle.py"; desc = "ETL WTTJ" },   # <-- DESACTIVE POUR TEST
        @{ script = "scripts\load_to_postgresql.py";  desc = "Chargement Gold vers PostgreSQL" }
    )
 
    foreach ($etl in $etlScripts) {
        Write-Info "$($etl.desc) [$($etl.script)]..."
        Log "ETL START: $($etl.script)"
 
        if (-not (Test-Path $etl.script)) {
            Write-Fail "Script introuvable : $($etl.script)"
            Log "ETL SKIP (not found): $($etl.script)"
            continue
        }
 
        try {
            python $etl.script
            if ($LASTEXITCODE -ne 0) { throw "Exit code $LASTEXITCODE" }
            Write-OK "$($etl.desc) termine"
            Log "ETL OK: $($etl.script)"
        }
        catch {
            Write-Fail "$($etl.desc) a echoue : $_"
            Log "ETL FAIL: $($etl.script) | $_"
            $rep = Read-Host "Continuer malgre l'erreur ? (o/N)"
            if ($rep -ne "o") { exit 1 }
        }
    }
 
    Log "--- ETL END ---"
 
} else {
    Write-Info "Mode SkipETL - ETL ignore."
}
 
# =============================================================
# ETAPE 3 - Verification PostgreSQL
# =============================================================
 
Write-Step "ETAPE 3 - Verification PostgreSQL"
Log "--- PG CHECK ---"
 
try {
    $query = "SELECT source, COUNT(*) as nb FROM gold.offres GROUP BY source ORDER BY nb DESC;"
    docker exec postgres-dwh psql -U dwh_user -d job_intelligent_dwh -c $query
    Write-OK "Donnees PostgreSQL verifiees"
    Log "PG CHECK OK"
}
catch {
    Write-Info "Impossible de verifier PostgreSQL : $_"
    Log "PG CHECK WARN: $_"
}
 
# =============================================================
# ETAPE 4 - Verification MinIO
# =============================================================
 
Write-Step "ETAPE 4 - Verification MinIO"
Log "--- MINIO CHECK ---"
 
try {
    docker exec minio-init mc ls local/bronze/raw/ --recursive --summarize
    Write-OK "Fichiers Bronze presents dans MinIO"
    Log "MINIO CHECK OK"
}
catch {
    Write-Info "Verification MinIO ignoree : $_"
}
 
# =============================================================
# RESUME FINAL
# =============================================================
 
Write-Host ""
Write-Host "=============================================" -ForegroundColor Magenta
Write-Host "  PIPELINE TERMINE - $Date"                   -ForegroundColor Magenta
Write-Host "  Log : $LOG_FILE"                            -ForegroundColor Magenta
Write-Host "  Airflow  -> http://localhost:8081"          -ForegroundColor White
Write-Host "  MinIO    -> http://localhost:9001"          -ForegroundColor White
Write-Host "  Kafka UI -> http://localhost:8085"          -ForegroundColor White
Write-Host "  API REST -> http://localhost:8000/health"   -ForegroundColor White
Write-Host "=============================================" -ForegroundColor Magenta
 
Log "=== PIPELINE END ==="