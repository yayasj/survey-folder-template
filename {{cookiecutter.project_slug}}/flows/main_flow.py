"""
Main Prefect Flow for Survey Pipeline
Orchestrates the complete data pipeline from ingestion to publication
"""

from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from datetime import datetime, timedelta
from pathlib import Path
import yaml
import os
from typing import Dict, Any, List
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

@task(name="load_config", retries=1)
def load_config() -> Dict[str, Any]:
    """Load configuration from config.yml"""
    logger = get_run_logger()
    
    config_path = project_root / "config.yml"
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    logger.info(f"Loaded configuration for project: {config.get('project', {}).get('name', 'Unknown')}")
    return config

@task(name="ingest_data", retries=3, retry_delay_seconds=60)
def ingest_data(config: Dict[str, Any]) -> str:
    """Ingest data from ODK Central"""
    logger = get_run_logger()
    
    # Create run timestamp
    run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Create staging directory for this run
    staging_path = project_root / "staging" / "raw"
    staging_path.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Starting data ingestion for run: {run_timestamp}")
    
    # This will be implemented in the next iteration
    # For now, just create a placeholder
    logger.info("Data ingestion placeholder - will implement ODK integration next")
    
    # Create run metadata
    metadata = {
        "run_timestamp": run_timestamp,
        "pipeline_version": "1.0.0",
        "config_hash": str(hash(str(config))),
        "status": "completed",
        "records_ingested": 0  # Placeholder
    }
    
    # Save metadata
    metadata_path = staging_path / f"run_metadata_{run_timestamp}.json"
    import json
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    logger.info(f"Ingestion completed. Metadata saved to: {metadata_path}")
    return run_timestamp

@task(name="validate_data", retries=2)
def validate_data(config: Dict[str, Any], run_timestamp: str) -> bool:
    """Validate data using Great Expectations"""
    logger = get_run_logger()
    
    logger.info(f"Starting validation for run: {run_timestamp}")
    
    # This will be implemented in the next iteration
    logger.info("Data validation placeholder - will implement Great Expectations next")
    
    # Create validation results directory
    validation_path = project_root / "validation_results" / run_timestamp
    validation_path.mkdir(parents=True, exist_ok=True)
    
    # Placeholder validation results
    validation_results = {
        "run_timestamp": run_timestamp,
        "overall_pass_rate": 95.0,
        "datasets_validated": 0,
        "critical_failures": 0,
        "warnings": 0
    }
    
    # Save validation results
    results_path = validation_path / "validation_summary.json"
    import json
    with open(results_path, 'w') as f:
        json.dump(validation_results, f, indent=2)
    
    # Check if validation passes minimum threshold
    min_pass_rate = config.get('validation', {}).get('minimum_pass_rate', 85)
    passes_validation = validation_results['overall_pass_rate'] >= min_pass_rate
    
    logger.info(f"Validation completed. Pass rate: {validation_results['overall_pass_rate']}%")
    return passes_validation

@task(name="clean_data", retries=2)
def clean_data(config: Dict[str, Any], run_timestamp: str, validation_passed: bool) -> bool:
    """Clean data using rules engine"""
    logger = get_run_logger()
    
    if not validation_passed:
        logger.warning("Validation failed, skipping cleaning step")
        return False
    
    logger.info(f"Starting data cleaning for run: {run_timestamp}")
    
    # This will be implemented in the next iteration
    logger.info("Data cleaning placeholder - will implement rules engine next")
    
    # Create cleaned directory
    cleaned_path = project_root / "staging" / "cleaned"
    cleaned_path.mkdir(parents=True, exist_ok=True)
    
    # Placeholder cleaning results
    cleaning_results = {
        "run_timestamp": run_timestamp,
        "rules_applied": 0,
        "records_modified": 0,
        "cleaning_successful": True
    }
    
    # Save cleaning log
    log_path = project_root / "logs" / f"cleaning_{run_timestamp}.json"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    import json
    with open(log_path, 'w') as f:
        json.dump(cleaning_results, f, indent=2)
    
    logger.info("Data cleaning completed successfully")
    return cleaning_results['cleaning_successful']

@task(name="publish_data", retries=1)
def publish_data(config: Dict[str, Any], run_timestamp: str, cleaning_passed: bool) -> bool:
    """Atomically publish cleaned data to stable directory"""
    logger = get_run_logger()
    
    if not cleaning_passed:
        logger.warning("Cleaning failed, skipping publish step")
        return False
    
    logger.info(f"Starting data publication for run: {run_timestamp}")
    
    staging_cleaned = project_root / "staging" / "cleaned"
    stable_path = project_root / "cleaned_stable"
    
    # Create backup of current stable data if it exists
    if stable_path.exists() and any(stable_path.iterdir()):
        backup_path = project_root / f"backup_stable_{run_timestamp}"
        logger.info(f"Creating backup: {backup_path}")
        
        import shutil
        shutil.copytree(stable_path, backup_path)
    
    # Atomic swap - create new stable directory
    stable_temp = project_root / f"cleaned_stable_temp_{run_timestamp}"
    
    if staging_cleaned.exists():
        import shutil
        shutil.copytree(staging_cleaned, stable_temp)
        
        # Remove old stable and rename temp to stable
        if stable_path.exists():
            shutil.rmtree(stable_path)
        stable_temp.rename(stable_path)
        
        logger.info("Data published successfully to cleaned_stable/")
        return True
    else:
        logger.error("No cleaned data found in staging area")
        return False

@flow(
    name="survey_pipeline_main",
    description="Main survey data pipeline flow",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True
)
def main_pipeline_flow() -> Dict[str, Any]:
    """
    Main pipeline flow that orchestrates all tasks
    """
    logger = get_run_logger()
    logger.info("Starting survey pipeline execution")
    
    # Load configuration
    config = load_config()
    
    # Run pipeline tasks in sequence
    run_timestamp = ingest_data(config)
    validation_passed = validate_data(config, run_timestamp)
    cleaning_passed = clean_data(config, run_timestamp, validation_passed)
    publish_success = publish_data(config, run_timestamp, cleaning_passed)
    
    # Summary results
    results = {
        "run_timestamp": run_timestamp,
        "validation_passed": validation_passed,
        "cleaning_passed": cleaning_passed,
        "publish_success": publish_success,
        "pipeline_success": all([validation_passed, cleaning_passed, publish_success])
    }
    
    if results["pipeline_success"]:
        logger.info("✅ Pipeline completed successfully!")
    else:
        logger.error("❌ Pipeline completed with errors")
    
    return results

if __name__ == "__main__":
    # Run the flow locally
    result = main_pipeline_flow()
    print(f"Pipeline result: {result}")
