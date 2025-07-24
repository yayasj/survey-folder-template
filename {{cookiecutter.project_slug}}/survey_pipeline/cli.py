"""
Command Line Interface for Survey Pipeline
Provides commands for ingestion, validation, cleaning, and publishing
"""

import click
import sys
from pathlib import Path
from typing import Optional

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from survey_pipeline.config import load_config, validate_config
from survey_pipeline.utils import setup_logging
from survey_pipeline.odk_client import create_odk_client, test_odk_connection

@click.group()
@click.option('--config', '-c', default=None, help='Path to config file')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx, config, verbose):
    """Survey Pipeline CLI - Automate ODK data processing"""
    
    # Setup logging
    log_level = "DEBUG" if verbose else "INFO"
    setup_logging(level=log_level)
    
    # Load and validate configuration
    try:
        config_data = load_config(config)
        validate_config(config_data)
        
        # Store config in context for subcommands
        ctx.ensure_object(dict)
        ctx.obj['config'] = config_data
        
    except Exception as e:
        click.echo(f"❌ Configuration error: {str(e)}", err=True)
        sys.exit(1)

@cli.command()
@click.pass_context
def test_connection(ctx):
    """Test connection to ODK Central"""
    config = ctx.obj['config']
    
    click.echo("🔌 Testing ODK Central connection...")
    
    try:
        client = create_odk_client()
        success = client.test_connection()
        
        if success:
            click.echo("✅ Connection successful!")
            
            # Show project details
            forms = client.discover_forms()
            click.echo(f"\n📋 Project Information:")
            click.echo(f"  URL: {config['odk']['base_url']}")
            click.echo(f"  Project ID: {config['odk']['project_id']}")
            click.echo(f"  Forms available: {len(forms)}")
            
            if forms:
                click.echo("\n📝 Available forms:")
                for form in forms:
                    submission_count = client.get_form_submissions_count(form['xmlFormId'])
                    click.echo(f"  - {form['xmlFormId']}: {submission_count} submissions")
        else:
            click.echo("❌ Connection failed!")
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"❌ Connection error: {str(e)}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--dry-run', is_flag=True, help='Show what would be downloaded without actually downloading')
@click.option('--format', '-f', default='csv', type=click.Choice(['csv', 'json']), help='Download format')
@click.option('--forms', help='Comma-separated list of specific form IDs to download')
@click.pass_context
def ingest(ctx, dry_run, format, forms):
    """Download data from ODK Central"""
    config = ctx.obj['config']
    
    try:
        # Create ODK client
        client = create_odk_client()
        
        # Test connection first
        if not client.test_connection():
            click.echo("❌ Cannot connect to ODK Central. Please check your credentials.", err=True)
            sys.exit(1)
        
        # Discover forms
        all_forms = client.discover_forms()
        
        # Parse forms filter if provided
        forms_filter = None
        if forms:
            forms_filter = [f.strip() for f in forms.split(',')]
            click.echo(f"🔍 Filtering to specific forms: {', '.join(forms_filter)}")
        
        if dry_run:
            click.echo("🔍 Dry run mode - showing what would be downloaded:")
            click.echo(f"  Source: {config['odk']['base_url']}")
            click.echo(f"  Project: {config['odk']['project_id']}")
            click.echo(f"  Format: {format.upper()}")
            
            forms_to_show = [f for f in all_forms if not forms_filter or f['xmlFormId'] in forms_filter]
            click.echo(f"\n📝 Forms to download ({len(forms_to_show)}):")
            
            total_submissions = 0
            for form in forms_to_show:
                submission_count = client.get_form_submissions_count(form['xmlFormId'])
                total_submissions += submission_count
                click.echo(f"  - {form['xmlFormId']}: {submission_count} submissions")
            
            click.echo(f"\n📊 Total submissions: {total_submissions}")
            return
        
        click.echo("📥 Starting data ingestion...")
        
        # Set up output path
        project_root = Path.cwd()
        raw_data_path = project_root / "raw"
        
        # Download all forms
        results = client.download_all_forms(
            output_path=raw_data_path,
            format=format,
            forms_filter=forms_filter
        )
        
        # Display results
        click.echo(f"\n✅ Ingestion completed!")
        click.echo(f"  📁 Run timestamp: {results['run_timestamp']}")
        click.echo(f"  📋 Forms processed: {results['forms_successful']}/{results['forms_requested']}")
        click.echo(f"  📊 Total submissions: {results['total_submissions']}")
        
        if results['forms_failed'] > 0:
            click.echo(f"  ⚠️  Failed forms: {results['forms_failed']}")
            
        # Show individual form results
        click.echo(f"\n📝 Form details:")
        for form_id, result in results['download_results'].items():
            if result['status'] == 'success':
                metadata = result['metadata']
                click.echo(f"  ✅ {form_id}: {metadata['submission_count']} submissions "
                          f"({metadata['file_size_bytes']} bytes)")
            else:
                click.echo(f"  ❌ {form_id}: {result['error']}")
        
        # Update staging area
        staging_path = project_root / "staging" / "raw"
        if staging_path.exists():
            import shutil
            shutil.rmtree(staging_path)
        
        # Copy latest run to staging
        latest_run_path = raw_data_path / results['run_timestamp']
        if latest_run_path.exists():
            import shutil
            shutil.copytree(latest_run_path, staging_path)
            click.echo(f"  📂 Data copied to staging/raw/")
        
        click.echo(f"\n🎯 Next step: python -m survey_pipeline.cli validate")
        
    except Exception as e:
        click.echo(f"❌ Ingestion failed: {str(e)}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--dataset', '-d', help='Validate specific dataset only')
@click.option('--suite', '-s', help='Use specific validation suite')
@click.pass_context
def validate(ctx, dataset, suite):
    """Validate datasets using Great Expectations"""
    config = ctx.obj['config']
    
    try:
        from survey_pipeline.validation import ValidationEngine
        from survey_pipeline.utils import create_run_timestamp
        from datetime import datetime
        
        run_timestamp = create_run_timestamp()
        
        # Set up validation-specific logging
        log_files = config.get('log_files', {})
        validation_log = log_files.get('validation', 'logs/validate_{date}.log')
        validation_log_path = validation_log.format(date=datetime.now().strftime('%Y-%m-%d'))
        
        # Reconfigure logging to include validation log file
        log_level = "DEBUG" if ctx.parent.params.get('verbose') else "INFO"
        setup_logging(level=log_level, log_file=validation_log_path)
        
        # Initialize validation engine
        project_root = Path.cwd()
        validation_engine = ValidationEngine(config, project_root)
        
        click.echo("🔍 Starting data validation...")
        click.echo(f"📝 Logging to: {validation_log_path}")
        
        if dataset:
            # Validate specific dataset
            dataset_path = Path("staging/raw") / f"{dataset}.csv"
            
            if not dataset_path.exists():
                click.echo(f"❌ Dataset not found: {dataset_path}")
                sys.exit(1)
            
            if not suite:
                # Try to find suite from configuration
                datasets_config = config.get('datasets', {})
                for config_name, config_data in datasets_config.items():
                    file_pattern = config_data.get('file_pattern', '')
                    import fnmatch
                    if fnmatch.fnmatch(dataset_path.name, file_pattern):
                        suite = config_data.get('validation_suite', '')
                        break
                
                if not suite:
                    click.echo(f"❌ No validation suite found for {dataset}")
                    sys.exit(1)
            
            click.echo(f"📊 Validating {dataset} with suite '{suite}'")
            
            validation_result, failed_rows = validation_engine.validate_dataset(
                dataset_path, suite, run_timestamp
            )
            
            # Show results
            if validation_result['overall_success']:
                click.echo(f"✅ Validation passed: {validation_result['passed_expectations']}/{validation_result['total_expectations']} expectations")
            else:
                click.echo(f"❌ Validation failed: {validation_result['failed_expectations']} expectations failed")
                
                if validation_result['critical_failures'] > 0:
                    click.echo(f"🚨 Critical failures: {validation_result['critical_failures']}")
                
                if validation_result.get('failed_rows_count', 0) > 0:
                    click.echo(f"📄 Failed rows saved: {validation_result['failed_rows_count']} rows")
                    
                sys.exit(1)
        
        else:
            # Validate all datasets
            click.echo("📊 Validating all datasets in staging area...")
            
            overall_results = validation_engine.validate_all_datasets(run_timestamp)
            
            # Show summary results
            click.echo(f"\n📈 Validation Summary:")
            click.echo(f"   Total datasets: {overall_results['total_datasets']}")
            click.echo(f"   Validated: {overall_results['validated_datasets']}")
            click.echo(f"   Passed: {overall_results['passed_datasets']}")
            click.echo(f"   Failed: {overall_results['failed_datasets']}")
            click.echo(f"   Overall pass rate: {overall_results['overall_pass_rate']:.1f}%")
            
            if overall_results['overall_success']:
                click.echo("✅ Overall validation: PASSED")
                click.echo(f"\n🎯 Next step: python -m survey_pipeline.cli clean")
            else:
                click.echo("❌ Overall validation: FAILED")
                
                if overall_results['critical_failures'] > 0:
                    click.echo(f"🚨 Critical failures in {overall_results['critical_failures']} datasets")
                
                sys.exit(1)
                
    except Exception as e:
        click.echo(f"❌ Validation failed: {str(e)}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--rules-file', '-r', help='Path to cleaning rules file')
@click.option('--max-iterations', '-i', default=5, help='Maximum cleaning iterations')
@click.option('--dry-run', is_flag=True, help='Show what would be cleaned without making changes')
@click.pass_context
def clean(ctx, rules_file, max_iterations, dry_run):
    """Apply data cleaning rules"""
    config = ctx.obj['config']
    
    if rules_file is None:
        rules_file = config.get('cleaning', {}).get('rules_file', 'cleaning_rules.xlsx')
    
    try:
        from survey_pipeline.cleaning import DataCleaningEngine
        from survey_pipeline.utils import create_run_timestamp
        from datetime import datetime
        
        run_timestamp = create_run_timestamp()
        
        # Set up cleaning-specific logging
        log_files = config.get('log_files', {})
        cleaning_log = log_files.get('cleaning', 'logs/clean_{date}.log')
        cleaning_log_path = cleaning_log.format(date=datetime.now().strftime('%Y-%m-%d'))
        
        # Reconfigure logging to include cleaning log file
        log_level = "DEBUG" if ctx.parent.params.get('verbose') else "INFO"
        setup_logging(level=log_level, log_file=cleaning_log_path)
        
        # Initialize cleaning engine
        project_root = Path.cwd()
        cleaning_engine = DataCleaningEngine(config, project_root)
        
        click.echo("🧹 Starting data cleaning...")
        click.echo(f"📝 Logging to: {cleaning_log_path}")
        click.echo(f"Rules file: {rules_file}")
        click.echo(f"Max iterations: {max_iterations}")
        
        if dry_run:
            click.echo("🔍 DRY RUN MODE - No changes will be made")
            
            # Load and display rules
            try:
                rules_df = cleaning_engine.load_cleaning_rules(rules_file)
                click.echo(f"\n📋 Found {len(rules_df)} active cleaning rules:")
                
                for _, rule in rules_df.iterrows():
                    priority = rule.get('priority', 'N/A')
                    note = rule.get('note', '')[:50] + '...' if len(str(rule.get('note', ''))) > 50 else rule.get('note', '')
                    click.echo(f"  • {rule['variable']}: {rule['rule_type']} (priority: {priority}) - {note}")
                
                # Show which datasets would be processed
                staging_path = project_root / "staging" / "raw"
                csv_files = list(staging_path.glob("*.csv"))
                click.echo(f"\n📁 Datasets to process: {len(csv_files)}")
                for csv_file in csv_files:
                    click.echo(f"  • {csv_file.name}")
                    
                click.echo(f"\n💡 Run without --dry-run to apply cleaning rules")
                
            except Exception as e:
                click.echo(f"❌ Failed to load rules: {str(e)}", err=True)
                sys.exit(1)
        
        else:
            # Apply cleaning to all datasets
            overall_results = cleaning_engine.clean_all_datasets(
                run_timestamp=run_timestamp,
                rules_file=rules_file,
                max_iterations=max_iterations
            )
            
            # Show results
            click.echo(f"\n📈 Cleaning Summary:")
            click.echo(f"   Total datasets: {overall_results['total_datasets']}")
            click.echo(f"   Cleaned: {overall_results['cleaned_datasets']}")
            click.echo(f"   Failed: {overall_results['failed_datasets']}")
            click.echo(f"   Total records modified: {overall_results['total_records_modified']}")
            click.echo(f"   Total rules applied: {overall_results['total_rules_applied']}")
            
            if overall_results['overall_success']:
                click.echo("✅ Overall cleaning: SUCCESS")
                click.echo(f"\n📁 Cleaned data saved to: staging/cleaned/{run_timestamp}/")
                click.echo(f"🎯 Next step: python -m survey_pipeline.cli publish")
            else:
                click.echo("❌ Overall cleaning: PARTIAL SUCCESS")
                click.echo(f"⚠️  {overall_results['failed_datasets']} datasets failed to clean")
                
                if overall_results['failed_datasets'] > 0:
                    click.echo(f"\n📄 Check cleaning logs for details")
                
    except Exception as e:
        click.echo(f"❌ Cleaning failed: {str(e)}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--force', is_flag=True, help='Force publish even if validation fails')
@click.pass_context
def publish(ctx, force):
    """Publish cleaned data to stable directory"""
    config = ctx.obj['config']
    
    click.echo("📤 Publishing cleaned data...")
    
    if force:
        click.echo("⚠️  Force mode enabled - skipping validation checks")
    
    # This will be implemented in the next iteration
    click.echo("⚠️  Data publishing not yet implemented")
    click.echo("Will implement atomic directory swap in next iteration")

@cli.command()
@click.option('--skip-validation', is_flag=True, help='Skip validation step')
@click.option('--skip-cleaning', is_flag=True, help='Skip cleaning step')
@click.pass_context
def run_pipeline(ctx, skip_validation, skip_cleaning):
    """Run the complete pipeline (ingest -> validate -> clean -> publish)"""
    config = ctx.obj['config']
    
    click.echo("🚀 Starting complete pipeline...")
    
    # This will be implemented in the next iteration using Prefect
    click.echo("⚠️  Full pipeline not yet implemented")
    click.echo("Will implement Prefect orchestration in next iteration")
    
    steps = ["ingest"]
    if not skip_validation:
        steps.append("validate")
    if not skip_cleaning:
        steps.append("clean")
    steps.append("publish")
    
    click.echo(f"Pipeline steps: {' -> '.join(steps)}")

@cli.command()
@click.option('--to', help='Rollback to specific date (YYYY-MM-DD)')
@click.pass_context
def rollback(ctx, to):
    """Rollback to previous stable data"""
    
    if not to:
        click.echo("❌ Please specify rollback date with --to YYYY-MM-DD")
        return
    
    click.echo(f"🔄 Rolling back to: {to}")
    
    # This will be implemented in the next iteration
    click.echo("⚠️  Rollback not yet implemented")
    click.echo("Will implement backup/restore functionality in next iteration")

@cli.command()
@click.pass_context
def status(ctx):
    """Show pipeline status and recent activity"""
    config = ctx.obj['config']
    
    click.echo("📊 Pipeline Status")
    click.echo("=" * 50)
    
    # Check directory status
    directories = [
        ("Raw Data", "raw"),
        ("Staging", "staging"),
        ("Cleaned Stable", "cleaned_stable"),
        ("Validation Results", "validation_results"),
        ("Logs", "logs")
    ]
    
    project_root = Path.cwd()
    
    for name, dirname in directories:
        dir_path = project_root / dirname
        if dir_path.exists():
            files = list(dir_path.glob("*"))
            click.echo(f"✅ {name}: {len(files)} items")
        else:
            click.echo(f"❌ {name}: Directory not found")
    
    click.echo("\n📈 Recent Activity")
    click.echo("-" * 30)
    click.echo("⚠️  Activity tracking not yet implemented")

if __name__ == '__main__':
    cli()
