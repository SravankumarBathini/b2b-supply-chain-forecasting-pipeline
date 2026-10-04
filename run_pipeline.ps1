# ==============================================================================
# 🏛️ ENTERPRISE TIME-SERIES PREDICTIVE FORECASTING PIPELINE ORCHESTRATOR
# CORE LAYOUT: End-to-End Automation & Stop-Gate Data Validation
# ==============================================================================
\$ErrorActionPreference = "Stop"
Clear-Host

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "  LAUNCHING B2B SUPPLY CHAIN FORECASTING AUTOMATION WORKFLOW " -ForegroundColor Cyan
Write-Host "=========================================================" -ForegroundColor Cyan

# 1. Environment Verification Step
\$VenvScript = ".venv\Scripts\Activate.ps1"
if (-Not (Test-Path \$VenvScript)) {
    Write-Host "[FATAL ERROR] Isolated Virtual Environment (.venv) not found!" -ForegroundColor Red
    Write-Host "Please build your environment via: python -m venv .venv" -ForegroundColor Yellow
    Exit 1
}

Write-Host "[INFO] Activating isolated Python virtual environment core..." -ForegroundColor Green
. \$VenvScript

# 2. STEP 1: Execute High-Velocity Transactional Log Generation
Write-Host "`n[STAGE 1] Running Module 1: Synthetic Demand Log Generation Engine..." -ForegroundColor Cyan
python src/generate_logs.py

$RawDataPath = "data/01_raw/demand_logs.parquet"
if (-Not (Test-Path $RawDataPath)) {
    Write-Host "[STOP-GATE ERROR] Stage 1 failed to generate output file: $RawDataPath" -ForegroundColor Red
    Exit 1
}
Write-Host "[SUCCESS] Stage 1 Validation Passed. Raw Parquet database verified on disk." -ForegroundColor Green

# 3. STEP 2: Execute Multi-Threaded Feature Engineering
Write-Host "`n[STAGE 2] Running Module 2: Multi-Threaded Lag & Window Transformation Core..." -ForegroundColor Cyan
python src/transform_lags.py

\$ProcessedDataPath = "data/02_processed/engineered_features.parquet"
if (-Not (Test-Path \$ProcessedDataPath)) {
    Write-Host "[STOP-GATE ERROR] Stage 2 failed to generate feature matrix: \$ProcessedDataPath" -ForegroundColor Red
    Exit 1
}
Write-Host "[SUCCESS] Stage 2 Validation Passed. Leakage-free feature matrix verified on disk." -ForegroundColor Green

# 4. STEP 3: Execute Chronological Model Training & Regularization Calibration
Write-Host "`n[STAGE 3] Running Module 3: Chronological Cross-Validation & Model Training..." -ForegroundColor Cyan
python src/train_models.py

# 5. Pipeline Completion Execution Summary
Write-Host "`n=========================================================" -ForegroundColor Green
Write-Host "  SUCCESS: END-TO-END PREDICTIVE FORECASTING WORKFLOW PIPELINE RUN COMPLETE" -ForegroundColor Green
Write-Host "=========================================================" -ForegroundColor Green
