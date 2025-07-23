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
    
    # This will be implemented in the next iteration
    click.echo("⚠️  Connection test not yet implemented")
    click.echo(f"Will test connection to: {config['odk']['base_url']}")
    click.echo(f"Project ID: {config['odk'].get('project_id', 'Not configured')}")

@cli.command()
@click.option('--dry-run', is_flag=True, help='Show what would be downloaded without actually downloading')
@click.pass_context
def ingest(ctx, dry_run):
    """Download data from ODK Central"""
    config = ctx.obj['config']
    
    if dry_run:
        click.echo("🔍 Dry run mode - showing what would be downloaded:")
        click.echo(f"Source: {config['odk']['base_url']}")
        click.echo(f"Project: {config['odk'].get('project_id')}")
        click.echo("Forms: [Will be auto-discovered]")
        return
    
    click.echo("📥 Starting data ingestion...")
    
    # This will be implemented in the next iteration
    click.echo("⚠️  Data ingestion not yet implemented")
    click.echo("Will integrate with pyODK in next iteration")

@cli.command()
@click.option('--suite', '-s', help='Specific validation suite to run')
@click.pass_context
def validate(ctx, suite):
    """Run data validation with Great Expectations"""
    config = ctx.obj['config']
    
    click.echo("🔍 Starting data validation...")
    
    if suite:
        click.echo(f"Running specific suite: {suite}")
    else:
        click.echo("Running all validation suites")
    
    # This will be implemented in the next iteration
    click.echo("⚠️  Data validation not yet implemented")
    click.echo("Will integrate with Great Expectations in next iteration")

@cli.command()
@click.option('--rules-file', '-r', help='Path to cleaning rules file')
@click.option('--max-iterations', '-i', default=5, help='Maximum cleaning iterations')
@click.pass_context
def clean(ctx, rules_file, max_iterations):
    """Apply data cleaning rules"""
    config = ctx.obj['config']
    
    if rules_file is None:
        rules_file = config.get('cleaning', {}).get('rules_file', 'cleaning_rules.xlsx')
    
    click.echo("🧹 Starting data cleaning...")
    click.echo(f"Rules file: {rules_file}")
    click.echo(f"Max iterations: {max_iterations}")
    
    # This will be implemented in the next iteration
    click.echo("⚠️  Data cleaning not yet implemented")
    click.echo("Will implement rules engine in next iteration")

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
