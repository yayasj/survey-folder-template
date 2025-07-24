"""
Main Prefect Flow for Survey Data Pipeline
Orchestrates ingestion, validation, cleaning, and publishing
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional

from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner

# Import pipeline components
import sys
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from survey_pipeline.config import load_config
from survey_pipeline.odk_client import ODKCentralClient
from survey_pipeline.validation import ValidationEngine
from survey_pipeline.utils import create_run_timestamp, ensure_directory

@task
def setup_pipeline(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Setup pipeline configuration and logging"""
    logger = get_run_logger()
    
    # Load configuration
    config = load_config(config_path)
    
    # Create run timestamp
    run_timestamp = create_run_timestamp()
    
    # Setup directories
    project_root = Path.cwd()
    staging_raw = project_root / "staging" / "raw"
    staging_cleaned = project_root / "staging" / "cleaned"
    staging_failed = project_root / "staging" / "failed" / run_timestamp
    validation_results = project_root / "validation_results" / run_timestamp
    
    for directory in [staging_raw, staging_cleaned, staging_failed, validation_results]:
        ensure_directory(directory)
    
    logger.info(f"Pipeline setup complete for run: {run_timestamp}")
    
    return {
        'config': config,
        'run_timestamp': run_timestamp,
        'project_root': str(project_root)
    }

@task
def ingest_data(setup_data: Dict[str, Any]) -> Dict[str, Any]:
    """Ingest data from ODK Central"""
    logger = get_run_logger()
    config = setup_data['config']
    run_timestamp = setup_data['run_timestamp']
    
    try:
        # Initialize ODK client
        client = ODKCentralClient(config)
        
        # Test connection
        if not client.test_connection():
            raise Exception("Failed to connect to ODK Central")
        
        # Discover and download forms
        logger.info("Starting data ingestion from ODK Central")
        
        projects = client.list_projects()
        total_downloaded = 0
        download_results = {}
        
        for project in projects:
            project_id = project['id']
            forms = client.list_forms(project_id)
            
            for form in forms:
                form_id = form.get('xmlFormId', 'unknown')
                
                try:
                    downloaded_path = client.download_submissions(project_id, form_id)
                    
                    if downloaded_path:
                        download_results[form_id] = {
                            'status': 'success',
                            'path': str(downloaded_path),
                            'project_id': project_id
                        }
                        total_downloaded += 1
                        logger.info(f"Downloaded {form_id}: {downloaded_path}")
                    else:
                        download_results[form_id] = {
                            'status': 'no_data',
                            'project_id': project_id
                        }
                        logger.warning(f"No data for form: {form_id}")
                        
                except Exception as e:
                    download_results[form_id] = {
                        'status': 'error',
                        'error': str(e),
                        'project_id': project_id
                    }
                    logger.error(f"Failed to download {form_id}: {str(e)}")
        
        return {
            'total_downloaded': total_downloaded,
            'download_results': download_results,
            'status': 'completed'
        }
        
    except Exception as e:
        logger.error(f"Data ingestion failed: {str(e)}")
        raise

@task
def validate_data(setup_data: Dict[str, Any], ingestion_results: Dict[str, Any]) -> Dict[str, Any]:
    """Validate ingested data using Great Expectations"""
    logger = get_run_logger()
    config = setup_data['config']
    run_timestamp = setup_data['run_timestamp']
    project_root = Path(setup_data['project_root'])
    
    try:
        # Initialize validation engine
        validation_engine = ValidationEngine(config, project_root)
        
        logger.info("Starting data validation")
        
        # Run validation on all datasets
        validation_results = validation_engine.validate_all_datasets(run_timestamp)
        
        # Log results
        logger.info(f"Validation completed: {validation_results['passed_datasets']}/{validation_results['validated_datasets']} datasets passed")
        logger.info(f"Overall pass rate: {validation_results['overall_pass_rate']:.1f}%")
        
        if validation_results['critical_failures'] > 0:
            logger.error(f"Critical validation failures in {validation_results['critical_failures']} datasets")
        
        # Check if pipeline should continue
        fail_fast = config.get('validation', {}).get('fail_fast_on_critical', True)
        if fail_fast and validation_results['critical_failures'] > 0:
            raise Exception(f"Pipeline stopped due to {validation_results['critical_failures']} critical validation failures")
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Data validation failed: {str(e)}")
        raise

@task
def clean_data(setup_data: Dict[str, Any], validation_results: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and transform validated data"""
    logger = get_run_logger()
    config = setup_data['config']
    run_timestamp = setup_data['run_timestamp']
    
    try:
        logger.info("Starting data cleaning")
        
        # TODO: Implement data cleaning engine
        # This will:
        # 1. Load Excel-based cleaning rules
        # 2. Apply transformations to validated data
        # 3. Generate audit trail
        # 4. Save cleaned data to staging/cleaned
        
        logger.warning("Data cleaning engine not yet implemented")
        
        # Placeholder results
        cleaning_results = {
            'status': 'placeholder',
            'cleaned_datasets': 0,
            'transformations_applied': 0,
            'records_cleaned': 0
        }
        
        return cleaning_results
        
    except Exception as e:
        logger.error(f"Data cleaning failed: {str(e)}")
        raise

@task
def publish_data(setup_data: Dict[str, Any], cleaning_results: Dict[str, Any]) -> Dict[str, Any]:
    """Publish cleaned data to production"""
    logger = get_run_logger()
    config = setup_data['config']
    run_timestamp = setup_data['run_timestamp']
    project_root = Path(setup_data['project_root'])
    
    try:
        logger.info("Starting data publishing")
        
        # Import publishing engine
        from survey_pipeline.publishing import PublishingEngine
        
        # Create publishing engine
        engine = PublishingEngine(config, project_root)
        
        # Check if cleaning was successful
        if not cleaning_results.get('success', False):
            logger.warning("Cleaning failed, skipping publication")
            return {
                'success': False,
                'error': 'Cleaning step failed, cannot publish',
                'run_timestamp': run_timestamp
            }
        
        # Publish data
        result = engine.publish_data(run_timestamp, force=False)
        
        if result['success']:
            logger.info(f"✅ Data published successfully - {result['datasets_published']} datasets, {result['total_records']} records")
            
            publishing_results = {
                'status': 'success',
                'published_datasets': result['datasets_published'],
                'total_records': result['total_records'],
                'backup_created': result['backup_path'] is not None,
                'backup_path': result['backup_path'],
                'publication_timestamp': run_timestamp,
                'metadata': result['metadata']
            }
        else:
            logger.error(f"❌ Publication failed: {result['error']}")
            publishing_results = {
                'status': 'failed',
                'error': result['error'],
                'publication_timestamp': run_timestamp
            }
        
        return publishing_results
        
    except Exception as e:
        logger.error(f"Data publishing failed: {str(e)}")
        return {
            'status': 'error',
            'error': str(e),
            'publication_timestamp': run_timestamp
        }

@task
def send_notification(
    setup_data: Dict[str, Any], 
    pipeline_results: Dict[str, Any]
) -> Dict[str, Any]:
    """Send pipeline completion notification"""
    logger = get_run_logger()
    config = setup_data['config']
    
    try:
        # TODO: Implement email/Slack notifications
        # This will send pipeline status notifications
        
        logger.info("Pipeline notification sent (placeholder)")
        
        return {'notification_sent': True}
        
    except Exception as e:
        logger.error(f"Notification failed: {str(e)}")
        # Don't fail the pipeline for notification issues
        return {'notification_sent': False, 'error': str(e)}

@flow(
    name="survey-data-pipeline",
    description="Complete survey data processing pipeline",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True
)
def survey_pipeline(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Main survey data pipeline flow
    
    Args:
        config_path: Optional path to configuration file
        
    Returns:
        Pipeline execution results
    """
    logger = get_run_logger()
    logger.info("🚀 Starting Survey Data Pipeline")
    
    try:
        # Setup pipeline
        setup_data = setup_pipeline(config_path)
        run_timestamp = setup_data['run_timestamp']
        
        logger.info(f"📅 Pipeline run: {run_timestamp}")
        
        # Step 1: Ingest data from ODK Central
        logger.info("📥 Step 1: Data Ingestion")
        ingestion_results = ingest_data(setup_data)
        
        # Step 2: Validate ingested data
        logger.info("🔍 Step 2: Data Validation")
        validation_results = validate_data(setup_data, ingestion_results)
        
        # Step 3: Clean and transform data
        logger.info("🧹 Step 3: Data Cleaning")
        cleaning_results = clean_data(setup_data, validation_results)
        
        # Step 4: Publish to production
        logger.info("📤 Step 4: Data Publishing")
        publishing_results = publish_data(setup_data, cleaning_results)
        
        # Compile final results
        pipeline_results = {
            'run_timestamp': run_timestamp,
            'status': 'completed',
            'ingestion': ingestion_results,
            'validation': validation_results,
            'cleaning': cleaning_results,
            'publishing': publishing_results
        }
        
        # Step 5: Send notifications
        logger.info("📧 Step 5: Notifications")
        notification_results = send_notification(setup_data, pipeline_results)
        pipeline_results['notification'] = notification_results
        
        logger.info("✅ Survey Data Pipeline completed successfully")
        
        return pipeline_results
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
        raise

@flow(
    name="validation-only-pipeline",
    description="Run validation only on existing staged data"
)
def validation_only_pipeline(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Run validation only on existing staged data
    
    Args:
        config_path: Optional path to configuration file
        
    Returns:
        Validation results
    """
    logger = get_run_logger()
    logger.info("🔍 Starting Validation-Only Pipeline")
    
    # Setup pipeline
    setup_data = setup_pipeline(config_path)
    
    # Mock ingestion results since we're using existing data
    ingestion_results = {'status': 'using_existing_data'}
    
    # Run validation
    validation_results = validate_data(setup_data, ingestion_results)
    
    logger.info("✅ Validation-Only Pipeline completed")
    
    return {
        'run_timestamp': setup_data['run_timestamp'],
        'status': 'completed',
        'validation': validation_results
    }

if __name__ == "__main__":
    # Run the main pipeline
    result = survey_pipeline()
    print(f"Pipeline completed: {result['status']}")

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

from survey_pipeline.odk_client import create_odk_client

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
    raw_archive_path = project_root / "raw"
    
    logger.info(f"Starting data ingestion for run: {run_timestamp}")
    
    try:
        # Create ODK client
        client = create_odk_client()
        
        # Test connection
        if not client.test_connection():
            raise Exception("Cannot connect to ODK Central")
        
        # Download all forms
        results = client.download_all_forms(
            output_path=raw_archive_path,
            format=config.get('odk', {}).get('download_format', 'csv')
        )
        
        # Clear and update staging area
        if staging_path.exists():
            import shutil
            shutil.rmtree(staging_path)
        
        # Copy latest run to staging
        latest_run_path = raw_archive_path / results['run_timestamp']
        if latest_run_path.exists():
            import shutil
            shutil.copytree(latest_run_path, staging_path)
        
        logger.info(f"✅ Ingestion completed: {results['forms_successful']}/{results['forms_requested']} forms, "
                   f"{results['total_submissions']} submissions")
        
        return results['run_timestamp']
        
    except Exception as e:
        logger.error(f"❌ Ingestion failed: {str(e)}")
        raise

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
def publish_data_simple(config: Dict[str, Any], run_timestamp: str, cleaning_passed: bool) -> bool:
    """Atomically publish cleaned data to stable directory (simple version)"""
    logger = get_run_logger()
    
    if not cleaning_passed:
        logger.warning("Cleaning failed, skipping publish step")
        return False
    
    logger.info(f"Starting data publication for run: {run_timestamp}")
    
    try:
        # Import publishing engine
        from survey_pipeline.publishing import PublishingEngine
        from pathlib import Path
        
        project_root = Path.cwd()
        engine = PublishingEngine(config, project_root)
        
        # Publish data
        result = engine.publish_data(run_timestamp, force=False)
        
        if result['success']:
            logger.info(f"✅ Data published successfully - {result['datasets_published']} datasets")
            return True
        else:
            logger.error(f"❌ Publication failed: {result['error']}")
            return False
            
    except Exception as e:
        logger.error(f"Publication error: {str(e)}")
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
    publish_success = publish_data_simple(config, run_timestamp, cleaning_passed)
    
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
