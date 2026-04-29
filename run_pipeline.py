import os
import sys
import subprocess
import time
from logger import get_logger

logger = get_logger("pipeline")

def check_docker():
    result = subprocess.run("docker ps", shell=True, capture_output=True)
    if result.returncode != 0:
        logger.error("Docker is not running. Please start Docker Desktop.")
        sys.exit(1)
    logger.info("Docker is running.")

def check_data_ready():
    from config.local_config import DATA_DIR
    sample_path = os.path.join(DATA_DIR, "sample_orders.csv")
    return os.path.exists(sample_path)

def run_step(name, command):
    logger.info(f"Starting: {name}")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        logger.error(f"Failed at step: {name}")
        sys.exit(1)
    logger.info(f"Completed: {name}")

def run_background(name, command):
    logger.info(f"Starting background process: {name}")
    return subprocess.Popen(command, shell=True)

def run_tests():
    logger.info("Running validation tests...")
    result = subprocess.run("python -m pytest tests/ -v", shell=True)
    if result.returncode != 0:
        logger.warning("Some tests failed. Check logs for details.")
    else:
        logger.info("All tests passed.")

def main():
    logger.info("Olist Pipeline Starting...")
    
    check_docker()
    
    if not check_data_ready():
        logger.info("Sample data not found. Running data sampler...")
        run_step("Data Sampler", "python data_sampler.py")
    else:
        logger.info("Sample data already exists. Skipping download.")
    
    run_step("Kafka Topics Setup", "python kafka_mod/topics_setup.py")
    
    bronze = run_background("Bronze Layer", "python spark/streaming_bronze.py")
    logger.info("Waiting for Bronze layer to initialize...")
    time.sleep(20)
    
    producer = run_background("Kafka Producer", "python kafka_mod/producer.py")
    logger.info("Waiting for Producer to finish sending...")
    time.sleep(40)
    
    silver = run_background("Silver Layer", "python spark/streaming_silver.py")
    logger.info("Waiting for Silver layer to process...")
    time.sleep(40)
    
    gold = run_background("Gold Layer", "python spark/streaming_gold.py")
    logger.info("Waiting for Gold layer to aggregate...")
    time.sleep(40)
    
    run_tests()
    
    logger.info("Starting Dashboard on http://localhost:8501")
    run_step("Dashboard", "streamlit run dashboard/app.py")

if __name__ == "__main__":
    main()