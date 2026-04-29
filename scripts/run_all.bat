@echo off
echo ========================================
echo Starting Olist Pipeline
echo ========================================

echo Checking Docker...
docker ps > nul 2>&1
if errorlevel 1 (
    echo Docker is not running. Please start Docker Desktop.
    pause
    exit /b 1
)

echo Starting Kafka...
docker-compose up -d
timeout /t 15 /nobreak > nul

echo Step 1: Sampling data...
python data_sampler.py
if errorlevel 1 (echo ERROR in data_sampler.py & pause & exit /b 1)

echo Step 2: Creating Kafka topics...
python kafka/topics_setup.py
if errorlevel 1 (echo ERROR in topics_setup.py & pause & exit /b 1)

echo Step 3: Starting Bronze layer...
start "Bronze" python spark/streaming_bronze.py
timeout /t 20 /nobreak > nul

echo Step 4: Starting Kafka producer...
start "Producer" python kafka/producer.py
timeout /t 40 /nobreak > nul

echo Step 5: Starting Silver layer...
start "Silver" python spark/streaming_silver.py
timeout /t 40 /nobreak > nul

echo Step 6: Starting Gold layer...
start "Gold" python spark/streaming_gold.py
timeout /t 40 /nobreak > nul

echo Step 7: Running tests...
python -m pytest tests/ -v
if errorlevel 1 (echo WARNING: Some tests failed. Check logs.)

echo Step 8: Starting Dashboard...
streamlit run dashboard/app.py

pause

