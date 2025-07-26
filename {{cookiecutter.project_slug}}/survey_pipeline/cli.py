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
@click.option('--dry-run', is_flag=True, help='Show what would be published without actually publishing')
@click.pass_context
def publish(ctx, force, dry_run):
    """Publish cleaned data to stable directory"""
    import sys
    from pathlib import Path
    from .publishing import create_publishing_engine
    from .utils import create_run_timestamp, setup_logging
    
    config = ctx.obj['config']
    
    # Setup logging
    setup_logging(level="INFO")
    
    click.echo("📤 Publishing cleaned data...")
    
    if force:
        click.echo("⚠️  Force mode enabled - skipping validation checks")
    
    if dry_run:
        click.echo("🔍 Dry run mode - no actual changes will be made")
    
    try:
        # Create publishing engine
        project_root = Path.cwd()
        engine = create_publishing_engine(project_root=project_root)
        
        # Create run timestamp
        run_timestamp = create_run_timestamp()
        
        if dry_run:
            # Show what would be published
            validation_results = engine.validate_staging_data()
            
            click.echo("\n📋 Publication Preview:")
            click.echo("=" * 50)
            
            if validation_results['valid']:
                click.echo(f"✅ Staging data is valid for publication")
                click.echo(f"📊 Total datasets: {len(validation_results['datasets_found'])}")
                click.echo(f"📈 Total records: {validation_results['total_records']}")
                
                for dataset in validation_results['datasets_found']:
                    click.echo(f"  📄 {dataset['file']}: {dataset['records']} records")
            else:
                click.echo("❌ Staging data has validation issues:")
                for issue in validation_results['issues']:
                    click.echo(f"  ⚠️  {issue}")
            
            # Show publication status
            status = engine.get_publication_status()
            click.echo(f"\n📁 Target directory: {status['stable_directory_path']}")
            click.echo(f"🔄 Would backup existing data: {engine.backup_previous}")
            
            return
        
        # Perform actual publication
        result = engine.publish_data(run_timestamp, force=force)
        
        if result['success']:
            click.echo("✅ Data published successfully!")
            click.echo(f"📊 Datasets published: {result['datasets_published']}")
            click.echo(f"📈 Total records: {result['total_records']}")
            
            if result['backup_path']:
                click.echo(f"💾 Backup created: {result['backup_path']}")
        else:
            click.echo(f"❌ Publication failed: {result['error']}")
            if 'issues' in result:
                click.echo("Issues found:")
                for issue in result['issues']:
                    click.echo(f"  ⚠️  {issue}")
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"❌ Publication error: {str(e)}")

def _run_pipeline_sequential(ctx, steps, force):
    """Run pipeline using sequential CLI command execution"""
    from datetime import datetime
    
    pipeline_start = datetime.now()
    results = {'steps': {}, 'overall_success': True, 'start_time': pipeline_start.isoformat()}
    
    click.echo(f"\n📋 Executing {len(steps)} pipeline steps sequentially...")
    
    try:
        # Step 1: Ingest
        if "ingest" in steps:
            click.echo("\n" + "="*50)
            click.echo("📥 Step 1/4: Data Ingestion")
            click.echo("="*50)
            
            try:
                ctx.invoke(ingest, dry_run=False, format='csv', forms=None)
                results['steps']['ingest'] = {'status': 'success', 'timestamp': datetime.now().isoformat()}
                click.echo("✅ Ingestion completed successfully")
            except Exception as e:
                click.echo(f"❌ Ingestion failed: {str(e)}")
                results['steps']['ingest'] = {'status': 'failed', 'error': str(e), 'timestamp': datetime.now().isoformat()}
                results['overall_success'] = False
                if not force:
                    raise click.ClickException(f"Pipeline stopped due to ingestion failure: {str(e)}")
        
        # Step 2: Validate
        if "validate" in steps and results['overall_success']:
            click.echo("\n" + "="*50)
            click.echo("🔍 Step 2/4: Data Validation")
            click.echo("="*50)
            
            try:
                ctx.invoke(validate, dataset=None, suite=None)
                results['steps']['validate'] = {'status': 'success', 'timestamp': datetime.now().isoformat()}
                click.echo("✅ Validation completed successfully")
            except Exception as e:
                click.echo(f"❌ Validation failed: {str(e)}")
                results['steps']['validate'] = {'status': 'failed', 'error': str(e), 'timestamp': datetime.now().isoformat()}
                results['overall_success'] = False
                if not force:
                    raise click.ClickException(f"Pipeline stopped due to validation failure: {str(e)}")
        
        # Step 3: Clean
        if "clean" in steps and (results['overall_success'] or force):
            click.echo("\n" + "="*50)
            click.echo("🧹 Step 3/4: Data Cleaning")
            click.echo("="*50)
            
            try:
                ctx.invoke(clean, rules_file=None, max_iterations=5, dry_run=False)
                results['steps']['clean'] = {'status': 'success', 'timestamp': datetime.now().isoformat()}
                click.echo("✅ Cleaning completed successfully")
            except Exception as e:
                click.echo(f"❌ Cleaning failed: {str(e)}")
                results['steps']['clean'] = {'status': 'failed', 'error': str(e), 'timestamp': datetime.now().isoformat()}
                results['overall_success'] = False
                if not force:
                    raise click.ClickException(f"Pipeline stopped due to cleaning failure: {str(e)}")
        
        # Step 4: Publish
        if "publish" in steps and (results['overall_success'] or force):
            click.echo("\n" + "="*50)
            click.echo("📤 Step 4/4: Data Publishing")
            click.echo("="*50)
            
            try:
                ctx.invoke(publish, force=force, dry_run=False)
                results['steps']['publish'] = {'status': 'success', 'timestamp': datetime.now().isoformat()}
                click.echo("✅ Publishing completed successfully")
            except Exception as e:
                click.echo(f"❌ Publishing failed: {str(e)}")
                results['steps']['publish'] = {'status': 'failed', 'error': str(e), 'timestamp': datetime.now().isoformat()}
                results['overall_success'] = False
        
        # Pipeline Summary
        pipeline_end = datetime.now()
        duration = pipeline_end - pipeline_start
        results['end_time'] = pipeline_end.isoformat()
        results['duration_seconds'] = duration.total_seconds()
        
        click.echo("\n" + "="*60)
        click.echo("📊 PIPELINE SUMMARY")
        click.echo("="*60)
        click.echo(f"⏱️  Duration: {duration}")
        click.echo(f"📈 Steps completed: {len([s for s in results['steps'].values() if s['status'] == 'success'])}/{len(steps)}")
        
        for step_name, step_result in results['steps'].items():
            status_icon = "✅" if step_result['status'] == 'success' else "❌"
            click.echo(f"  {status_icon} {step_name.title()}: {step_result['status']}")
            if step_result['status'] == 'failed':
                click.echo(f"    Error: {step_result.get('error', 'Unknown error')}")
        
        if results['overall_success']:
            click.echo(f"\n🎉 Pipeline completed successfully in {duration}!")
            click.echo("📁 Data is now available in cleaned_stable/ directory")
        else:
            failed_steps = [name for name, result in results['steps'].items() if result['status'] == 'failed']
            click.echo(f"\n⚠️  Pipeline completed with errors in: {', '.join(failed_steps)}")
            if force:
                click.echo("📁 Some data may still be available despite errors (force mode)")
        
        return results
        
    except click.ClickException:
        raise
    except Exception as e:
        results['overall_success'] = False
        results['unexpected_error'] = str(e)
        click.echo(f"\n💥 Unexpected pipeline error: {str(e)}")
        raise click.ClickException(f"Pipeline failed with unexpected error: {str(e)}")

@cli.command()
@click.option('--skip-validation', is_flag=True, help='Skip validation step')
@click.option('--skip-cleaning', is_flag=True, help='Skip cleaning step')
@click.option('--force', is_flag=True, help='Force publish even if validation fails')
@click.pass_context
def run_pipeline(ctx, skip_validation, skip_cleaning, force):
    """Run the complete pipeline (ingest -> validate -> clean -> publish)"""
    config = ctx.obj['config']
    
    click.echo("🚀 Starting complete pipeline...")
    
    # Determine pipeline steps
    steps = ["ingest"]
    if not skip_validation:
        steps.append("validate")
    if not skip_cleaning:
        steps.append("clean")
    steps.append("publish")
    
    click.echo(f"Pipeline steps: {' -> '.join(steps)}")
    
    # Use direct CLI command execution
    return _run_pipeline_sequential(ctx, steps, force)

@cli.command()
@click.option('--to', help='Rollback to specific timestamp (YYYY-MM-DD_HH-MM-SS)')
@click.option('--list-backups', is_flag=True, help='List available backups')
@click.pass_context
def rollback(ctx, to, list_backups):
    """Rollback to previous stable data"""
    from pathlib import Path
    from .publishing import create_publishing_engine
    
    config = ctx.obj['config']
    project_root = Path.cwd()
    
    if list_backups:
        click.echo("📦 Available backups:")
        
        # List backup directories
        backup_pattern = "stable_backup_*"
        backups = list(project_root.glob(f"**/{backup_pattern}"))
        
        if not backups:
            click.echo("  No backups found")
            return
        
        # Sort by modification time
        backups.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        for backup in backups:
            timestamp = backup.name.replace('stable_backup_', '')
            modified = backup.stat().st_mtime
            from datetime import datetime
            mod_date = datetime.fromtimestamp(modified).strftime('%Y-%m-%d %H:%M:%S')
            click.echo(f"  📅 {timestamp} (created: {mod_date})")
        
        return
    
    if not to:
        click.echo("❌ Please specify a timestamp to rollback to or use --list-backups")
        click.echo("Example: python -m survey_pipeline.cli rollback --to 2025-07-24_14-30-15")
        return
    
    try:
        engine = create_publishing_engine(project_root=project_root)
        result = engine.rollback_publication(to)
        
        if result['success']:
            click.echo(f"✅ Successfully rolled back to: {to}")
            if result['current_backup']:
                click.echo(f"💾 Current data backed up to: {result['current_backup']}")
        else:
            click.echo(f"❌ Rollback failed: {result['error']}")
            
    except Exception as e:
        click.echo(f"❌ Rollback error: {str(e)}")

@cli.command()
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']), default='table', help='Output format')
@click.pass_context
def status(ctx, output_format):
    """Show pipeline status and recent activity"""
    from pathlib import Path
    from .publishing import create_publishing_engine
    
    config = ctx.obj['config']
    project_root = Path.cwd()
    
    try:
        # Get publication status
        engine = create_publishing_engine(project_root=project_root)
        pub_status = engine.get_publication_status()
        publications = engine.list_publications()
        
        if output_format == 'json':
            import json
            status_data = {
                'directories': {},
                'publication_status': pub_status,
                'recent_publications': publications[:3]
            }
            
            # Directory status
            directories = [
                ("raw", "raw"),
                ("staging", "staging"),
                ("cleaned_stable", "cleaned_stable"),
                ("validation_results", "validation_results"),
                ("logs", "logs")
            ]
            
            for name, dirname in directories:
                dir_path = project_root / dirname
                status_data['directories'][name] = {
                    'exists': dir_path.exists(),
                    'path': str(dir_path),
                    'item_count': len(list(dir_path.glob("*"))) if dir_path.exists() else 0
                }
            
            click.echo(json.dumps(status_data, indent=2, default=str))
            return
        
        # Table format
        click.echo("📊 Pipeline Status")
        click.echo("=" * 50)
        
        # Directory status
        directories = [
            ("Raw Data", "raw"),
            ("Staging", "staging"),
            ("Cleaned Stable", "cleaned_stable"),
            ("Validation Results", "validation_results"),
            ("Logs", "logs")
        ]
        
        click.echo("\n📁 Directory Status:")
        for name, dirname in directories:
            dir_path = project_root / dirname
            if dir_path.exists():
                files = list(dir_path.glob("*"))
                click.echo(f"  ✅ {name}: {len(files)} items")
            else:
                click.echo(f"  ❌ {name}: Directory not found")
        
        # Publication status
        click.echo(f"\n🚀 Publication Status:")
        click.echo(f"  📁 Stable directory: {'✅' if pub_status['stable_directory_exists'] else '❌'}")
        click.echo(f"  🚀 Staging ready: {'✅' if pub_status['staging_ready'] else '❌'}")
        click.echo(f"  📈 Current records: {pub_status['total_records']}")
        
        # Last publication
        if pub_status['last_publication']:
            last_pub = pub_status['last_publication']
            pub_date = last_pub['publication_date'][:19]
            click.echo(f"  🕒 Last publication: {pub_date}")
        else:
            click.echo(f"  🕒 Last publication: None")
        
        # Recent activity
        if publications:
            click.echo(f"\n📋 Recent Publications ({min(3, len(publications))}):")
            for pub in publications[:3]:
                pub_date = pub['publication_date'][:19]
                click.echo(f"  📅 {pub_date}: {len(pub['datasets_published'])} datasets")
        
    except Exception as e:
        click.echo(f"❌ Status error: {str(e)}")

# Add new commands for publication management
@cli.command()
@click.pass_context  
def list_publications(ctx):
    """List recent publications with metadata"""
    from pathlib import Path
    from .publishing import create_publishing_engine
    
    try:
        project_root = Path.cwd()
        engine = create_publishing_engine(project_root=project_root)
        
        publications = engine.list_publications()
        
        if not publications:
            click.echo("📭 No publications found")
            return
        
        click.echo(f"📋 Publication History ({len(publications)} publications)")
        click.echo("=" * 70)
        
        for i, pub in enumerate(publications):
            click.echo(f"\n{i+1}. Publication: {pub['publication_timestamp']}")
            click.echo(f"   📅 Date: {pub['publication_date']}")
            click.echo(f"   📊 Datasets: {len(pub['datasets_published'])}")
            click.echo(f"   📈 Records: {pub['total_records_published']}")
            click.echo(f"   💾 Backup: {'Yes' if pub['backup_created'] else 'No'}")
            
            if pub['datasets_published']:
                click.echo("   📄 Files:")
                for dataset in pub['datasets_published']:
                    click.echo(f"     • {dataset['file']}: {dataset['records']} records")
        
    except Exception as e:
        click.echo(f"❌ Error listing publications: {str(e)}")

if __name__ == '__main__':
    cli()
