"""
Survey Data Dashboard
Main Streamlit application for monitoring survey data quality and progress
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import sys
from pathlib import Path
import yaml
import json
import subprocess

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import CLI modules for pipeline integration
try:
    from survey_pipeline.cli import get_status
    from survey_pipeline.publishing import PublishingEngine
    CLI_AVAILABLE = True
except ImportError:
    CLI_AVAILABLE = False

# Page configuration
st.set_page_config(
    page_title="{{cookiecutter.project_name}} - Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def load_config():
    """Load configuration from config.yml"""
    config_path = project_root / "config.yml"
    if config_path.exists():
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    return {}

def load_stable_data():
    """Load data from cleaned_stable directory"""
    stable_path = project_root / "cleaned_stable"
    data_files = {}
    
    if stable_path.exists():
        for csv_file in stable_path.glob("*.csv"):
            try:
                df = pd.read_csv(csv_file)
                data_files[csv_file.stem] = df
            except Exception as e:
                st.error(f"Error loading {csv_file.name}: {str(e)}")
    
    return data_files

def load_validation_results():
    """Load latest validation results"""
    validation_path = project_root / "validation_results"
    latest_results = {}
    
    if validation_path.exists():
        # Get most recent validation run
        run_dirs = [d for d in validation_path.iterdir() if d.is_dir()]
        if run_dirs:
            latest_run = max(run_dirs, key=lambda x: x.stat().st_mtime)
            
            # Load validation summary
            summary_file = latest_run / "validation_summary.json"
            if summary_file.exists():
                try:
                    with open(summary_file, 'r') as f:
                        latest_results = json.load(f)
                except Exception as e:
                    st.error(f"Error loading validation results: {str(e)}")
    
    return latest_results

def get_pipeline_status():
    """Get current pipeline status using CLI"""
    if CLI_AVAILABLE:
        try:
            status = get_status()
            return status
        except Exception as e:
            st.error(f"Error getting pipeline status: {str(e)}")
            return None
    else:
        # Fallback: basic directory checks
        status = {
            "raw_items": len(list((project_root / "raw").glob("*"))) if (project_root / "raw").exists() else 0,
            "staging_items": len(list((project_root / "staging").glob("*"))) if (project_root / "staging").exists() else 0,
            "stable_items": len(list((project_root / "cleaned_stable").glob("*.csv"))) if (project_root / "cleaned_stable").exists() else 0,
        }
        return status

def run_pipeline_command(command):
    """Execute pipeline command"""
    try:
        cmd = ["python", "-m", "survey_pipeline.cli", command]
        result = subprocess.run(cmd, cwd=project_root, capture_output=True, text=True, timeout=300)
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Command timed out after 5 minutes"
    except Exception as e:
        return False, "", str(e)

def main():
    """Main dashboard application"""
    
    # Load configuration
    config = load_config()
    project_info = config.get('project', {})
    dashboard_config = config.get('dashboard', {})
    
    # Header
    st.title(dashboard_config.get('title', '{{cookiecutter.project_name}} - Data Dashboard'))
    
    # Sidebar
    st.sidebar.header("Navigation")
    
    # Dashboard sections
    sections = [
        "📈 Overview",
        "📊 Data Quality", 
        "🗺️ Geographic View",
        "👥 Enumerator Performance",
        "🚀 Pipeline Control",
        "⚙️ System Status"
    ]
    
    selected_section = st.sidebar.selectbox("Select Section", sections)
    
    # Project info in sidebar
    if project_info:
        st.sidebar.markdown("---")
        st.sidebar.markdown("### Project Information")
        st.sidebar.markdown(f"**Client:** {project_info.get('client', 'N/A')}")
        st.sidebar.markdown(f"**Period:** {project_info.get('start_date', 'N/A')} to {project_info.get('end_date', 'N/A')}")
    
    # Load data
    data_files = load_stable_data()
    validation_results = load_validation_results()
    pipeline_status = get_pipeline_status()
    
    # Last updated
    st.sidebar.markdown("---")
    stable_path = project_root / "cleaned_stable"
    if stable_path.exists():
        last_updated = datetime.fromtimestamp(stable_path.stat().st_mtime)
        st.sidebar.markdown(f"**Last Updated:** {last_updated.strftime('%Y-%m-%d %H:%M')}")
    
    # Pipeline status summary in sidebar
    if pipeline_status:
        st.sidebar.markdown("### 🔧 Pipeline Status")
        if isinstance(pipeline_status, dict):
            if 'raw_items' in pipeline_status:
                st.sidebar.metric("Raw Files", pipeline_status['raw_items'])
            if 'staging_items' in pipeline_status:
                st.sidebar.metric("Staging Files", pipeline_status['staging_items'])  
            if 'stable_items' in pipeline_status:
                st.sidebar.metric("Published Records", pipeline_status['stable_items'])
    
    # Auto-refresh
    refresh_interval = dashboard_config.get('refresh_interval', 300)
    if st.sidebar.button("🔄 Refresh Data"):
        st.rerun()
    
    st.sidebar.markdown(f"*Auto-refresh every {refresh_interval//60} minutes*")
    
    # Main content based on selected section
    if selected_section == "📈 Overview":
        show_overview(data_files, validation_results, config, pipeline_status)
    elif selected_section == "📊 Data Quality":
        show_data_quality(data_files, validation_results, config)
    elif selected_section == "🗺️ Geographic View":
        show_geographic_view(data_files, config)
    elif selected_section == "👥 Enumerator Performance":
        show_enumerator_performance(data_files, config)
    elif selected_section == "🚀 Pipeline Control":
        show_pipeline_control(config)
    elif selected_section == "⚙️ System Status":
        show_system_status(config, pipeline_status)

def show_overview(data_files, validation_results, config, pipeline_status):
    """Show overview dashboard"""
    st.header("📈 Survey Overview")
    
    if not data_files:
        st.warning("No data available in cleaned_stable directory. Please run the pipeline first.")
        return
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    total_records = sum(len(df) for df in data_files.values())
    total_datasets = len(data_files)
    
    with col1:
        st.metric("Total Records", f"{total_records:,}")
    
    with col2:
        st.metric("Active Datasets", total_datasets)
    
    with col3:
        # Calculate actual pass rate from validation results
        if validation_results and 'dataset_results' in validation_results:
            passed_datasets = validation_results.get('passed_datasets', 0)
            total_validated = validation_results.get('validated_datasets', 1)
            pass_rate = (passed_datasets / total_validated * 100) if total_validated > 0 else 0
            st.metric("Dataset Pass Rate", f"{pass_rate:.1f}%")
        else:
            st.metric("Quality Score", "No validation data")
    
    with col4:
        # Calculate survey days from data
        if data_files:
            all_dates = []
            for df in data_files.values():
                if 'SubmissionDate' in df.columns:
                    dates = pd.to_datetime(df['SubmissionDate'], errors='coerce').dropna()
                    all_dates.extend(dates)
            
            if all_dates:
                min_date = min(all_dates)
                max_date = max(all_dates)
                days_active = (max_date - min_date).days + 1
                st.metric("Survey Days", days_active)
            else:
                st.metric("Survey Days", "N/A")
    
    # Validation Summary
    if validation_results:
        st.subheader("🔍 Latest Validation Summary")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Total Datasets Validated", validation_results.get('total_datasets', 0))
            st.metric("Passed Datasets", validation_results.get('passed_datasets', 0))
            
        with col2:
            st.metric("Failed Datasets", validation_results.get('failed_datasets', 0))
            st.metric("Critical Failures", validation_results.get('critical_failures', 0))
        
        # Show validation timestamp
        if 'run_timestamp' in validation_results:
            st.caption(f"Last validation: {validation_results['run_timestamp']}")
    
    # Dataset summary
    st.subheader("📊 Dataset Summary")
    
    if data_files:
        summary_data = []
        for name, df in data_files.items():
            # Get latest submission date for this dataset
            latest_submission = "N/A"
            if 'SubmissionDate' in df.columns:
                dates = pd.to_datetime(df['SubmissionDate'], errors='coerce').dropna()
                if len(dates) > 0:
                    latest_submission = dates.max().strftime("%Y-%m-%d %H:%M")
            
            summary_data.append({
                "Dataset": name.replace('_', ' ').title(),
                "Records": len(df),
                "Columns": len(df.columns),
                "Latest Submission": latest_submission
            })
        
        summary_df = pd.DataFrame(summary_data)
        st.dataframe(summary_df, use_container_width=True)
    
    # Submission trends using real data
    st.subheader("📈 Submission Trends")
    
    if data_files:
        all_submissions = []
        for name, df in data_files.items():
            if 'SubmissionDate' in df.columns:
                df_copy = df.copy()
                df_copy['dataset'] = name
                df_copy['date'] = pd.to_datetime(df_copy['SubmissionDate'], errors='coerce')
                df_copy = df_copy.dropna(subset=['date'])
                all_submissions.append(df_copy[['date', 'dataset']])
        
        if all_submissions:
            combined_df = pd.concat(all_submissions, ignore_index=True)
            
            # Group by date
            daily_counts = combined_df.groupby([combined_df['date'].dt.date, 'dataset']).size().reset_index(name='submissions')
            daily_counts['date'] = pd.to_datetime(daily_counts['date'])
            
            if len(daily_counts) > 0:
                fig = px.line(daily_counts, x='date', y='submissions', color='dataset',
                              title="Daily Submissions by Dataset", 
                              markers=True)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No submission date data available for trend analysis")
        else:
            st.info("No submission date data available for trend analysis")
    else:
        st.info("No data available for trend analysis")

def show_data_quality(data_files, validation_results, config):
    """Show data quality dashboard"""
    st.header("📊 Data Quality Monitoring")
    
    if not data_files:
        st.warning("No data available in cleaned_stable directory.")
        return
    
    # Quality metrics from real validation results
    if validation_results and 'dataset_results' in validation_results:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Validation Results")
            
            # Overall metrics
            st.metric("Total Datasets", validation_results.get('total_datasets', 0))
            st.metric("Passed Datasets", validation_results.get('passed_datasets', 0))
            st.metric("Failed Datasets", validation_results.get('failed_datasets', 0))
            st.metric("Critical Failures", validation_results.get('critical_failures', 0))
            
            # Dataset-specific results
            for dataset_name, results in validation_results.get('dataset_results', {}).items():
                with st.expander(f"📋 {dataset_name.replace('_', ' ').title()}"):
                    total_exp = results.get('total_expectations', 0)
                    passed_exp = results.get('passed_expectations', 0)
                    failed_exp = results.get('failed_expectations', 0)
                    
                    if total_exp > 0:
                        pass_rate = (passed_exp / total_exp) * 100
                        st.metric("Pass Rate", f"{pass_rate:.1f}%")
                    else:
                        st.metric("Pass Rate", "N/A")
                    
                    st.metric("Total Expectations", total_exp)
                    st.metric("Failed Expectations", failed_exp)
                    st.metric("Critical Failures", results.get('critical_failures', 0))
                    st.metric("Warning Failures", results.get('warning_failures', 0))
        
        with col2:
            st.subheader("Validation Issues")
            
            # Extract specific issues from validation results
            all_issues = []
            for dataset_name, results in validation_results.get('dataset_results', {}).items():
                for expectation in results.get('expectation_results', []):
                    if not expectation.get('success', True):
                        issue_type = expectation.get('expectation_type', 'Unknown')
                        column = expectation.get('kwargs', {}).get('column', 'Unknown')
                        severity = expectation.get('severity', 'warning')
                        
                        # Simplify issue description
                        if 'null' in issue_type:
                            description = f"Missing values in {column}"
                        elif 'between' in issue_type:
                            description = f"Values out of range in {column}"
                        elif 'unique' in issue_type:
                            description = f"Duplicate values in {column}"
                        else:
                            description = f"Data quality issue in {column}"
                        
                        all_issues.append({
                            "Dataset": dataset_name.replace('_', ' ').title(),
                            "Issue": description,
                            "Column": column,
                            "Severity": severity.title()
                        })
            
            if all_issues:
                issues_df = pd.DataFrame(all_issues)
                
                # Color code by severity
                def color_severity(val):
                    if val == "Error":
                        return "color: red"
                    elif val == "Warning":
                        return "color: orange"
                    else:
                        return "color: blue"
                
                styled_df = issues_df.style.map(color_severity, subset=['Severity'])
                st.dataframe(styled_df, use_container_width=True)
            else:
                st.success("No validation issues found!")
        
        # Quality trends based on validation timestamps
        st.subheader("Quality Trends")
        
        # For now, show the current validation result
        # In a production system, you'd track validation results over time
        if validation_results.get('dataset_results'):
            datasets = []
            pass_rates = []
            
            for dataset_name, results in validation_results['dataset_results'].items():
                total_exp = results.get('total_expectations', 0)
                passed_exp = results.get('passed_expectations', 0)
                
                if total_exp > 0:
                    pass_rate = (passed_exp / total_exp) * 100
                    datasets.append(dataset_name.replace('_', ' ').title())
                    pass_rates.append(pass_rate)
            
            if datasets:
                fig = px.bar(x=datasets, y=pass_rates,
                            title="Validation Pass Rate by Dataset",
                            labels={"x": "Dataset", "y": "Pass Rate (%)"})
                fig.add_hline(y=90, line_dash="dash", line_color="red", 
                             annotation_text="Target Threshold (90%)")
                st.plotly_chart(fig, use_container_width=True)
    
    else:
        st.info("No validation results available. Run validation to see quality metrics.")
        
        # Show basic data quality checks
        st.subheader("Basic Data Quality Overview")
        
        for name, df in data_files.items():
            with st.expander(f"📊 {name.replace('_', ' ').title()}"):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    # Missing values
                    missing_count = df.isnull().sum().sum()
                    total_cells = len(df) * len(df.columns)
                    missing_pct = (missing_count / total_cells) * 100 if total_cells > 0 else 0
                    st.metric("Missing Values", f"{missing_pct:.1f}%")
                
                with col2:
                    # Duplicate rows
                    duplicate_count = df.duplicated().sum()
                    st.metric("Duplicate Rows", duplicate_count)
                
                with col3:
                    # Data completeness
                    completeness = 100 - missing_pct
                    st.metric("Completeness", f"{completeness:.1f}%")

def show_geographic_view(data_files, config):
    """Show geographic dashboard"""
    st.header("🗺️ Geographic Distribution")
    
    # Check if we have GPS data
    gps_data = None
    for name, df in data_files.items():
        if 'gps_latitude' in df.columns and 'gps_longitude' in df.columns:
            gps_data = df
            break
    
    if gps_data is not None:
        # Filter valid GPS coordinates
        valid_gps = gps_data.dropna(subset=['gps_latitude', 'gps_longitude'])
        
        if len(valid_gps) > 0:
            st.subheader(f"Household Locations ({len(valid_gps)} households)")
            
            # Create map using Plotly
            fig = px.scatter_mapbox(
                valid_gps,
                lat="gps_latitude",
                lon="gps_longitude",
                hover_name="household_id" if "household_id" in valid_gps.columns else None,
                zoom=8,
                height=600,
                mapbox_style="open-street-map"
            )
            
            fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
            st.plotly_chart(fig, use_container_width=True)
            
            # GPS quality metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                coverage = len(valid_gps) / len(gps_data) * 100
                st.metric("GPS Coverage", f"{coverage:.1f}%")
            
            with col2:
                if 'gps_accuracy' in valid_gps.columns:
                    avg_accuracy = valid_gps['gps_accuracy'].mean()
                    st.metric("Avg Accuracy", f"{avg_accuracy:.1f}m")
                else:
                    st.metric("Avg Accuracy", "N/A")
            
            with col3:
                st.metric("Valid Coordinates", len(valid_gps))
        else:
            st.warning("No valid GPS coordinates found in the data.")
    else:
        st.warning("No GPS data available. Ensure your data includes 'gps_latitude' and 'gps_longitude' columns.")

def show_enumerator_performance(data_files, config):
    """Show enumerator performance dashboard"""
    st.header("👥 Enumerator Performance")
    
    # Check if we have enumerator data
    enum_data = None
    for name, df in data_files.items():
        if 'enumerator' in df.columns:
            enum_data = df
            break
    
    if enum_data is not None:
        # Enumerator summary
        enum_summary = enum_data.groupby('enumerator').agg({
            'household_id': 'count',
            'submission_date': ['min', 'max'] if 'submission_date' in enum_data.columns else 'count'
        }).reset_index()
        
        enum_summary.columns = ['Enumerator', 'Total Submissions', 'First Submission', 'Last Submission']
        
        st.subheader("Enumerator Summary")
        st.dataframe(enum_summary, use_container_width=True)
        
        # Performance chart
        fig = px.bar(enum_summary, x='Enumerator', y='Total Submissions',
                     title="Submissions by Enumerator")
        st.plotly_chart(fig, use_container_width=True)
        
    else:
        st.warning("No enumerator data available. Ensure your data includes an 'enumerator' column.")

def show_pipeline_control(config):
    """Show pipeline control dashboard"""
    st.header("🚀 Pipeline Control")
    
    st.markdown("""
    Control and monitor the survey data pipeline. Execute individual pipeline stages 
    or run the complete workflow from this interface.
    """)
    
    # Pipeline stages
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔧 Pipeline Stages")
        
        # Individual stage controls
        st.markdown("#### 1. Data Ingestion")
        if st.button("📥 Run Ingest", key="ingest_btn", help="Download latest data from ODK"):
            with st.spinner("Running data ingestion..."):
                success, stdout, stderr = run_pipeline_command("ingest")
                if success:
                    st.success("✅ Data ingestion completed successfully!")
                    st.code(stdout, language="text")
                else:
                    st.error("❌ Data ingestion failed!")
                    st.code(stderr, language="text")
        
        st.markdown("#### 2. Data Validation")
        if st.button("🔍 Run Validation", key="validate_btn", help="Run Great Expectations validation"):
            with st.spinner("Running data validation..."):
                success, stdout, stderr = run_pipeline_command("validate")
                if success:
                    st.success("✅ Data validation completed successfully!")
                    st.code(stdout, language="text")
                else:
                    st.error("❌ Data validation failed!")
                    st.code(stderr, language="text")
        
        st.markdown("#### 3. Data Cleaning")
        if st.button("🧹 Run Cleaning", key="clean_btn", help="Apply cleaning rules to data"):
            with st.spinner("Running data cleaning..."):
                success, stdout, stderr = run_pipeline_command("clean")
                if success:
                    st.success("✅ Data cleaning completed successfully!")
                    st.code(stdout, language="text")
                else:
                    st.error("❌ Data cleaning failed!")
                    st.code(stderr, language="text")
        
        st.markdown("#### 4. Data Publishing")
        col_pub1, col_pub2 = st.columns(2)
        
        with col_pub1:
            if st.button("👀 Publish Preview", key="publish_preview_btn", help="Preview what will be published"):
                with st.spinner("Generating publish preview..."):
                    success, stdout, stderr = run_pipeline_command("publish --dry-run")
                    if success:
                        st.success("✅ Publish preview generated!")
                        st.code(stdout, language="text")
                    else:
                        st.error("❌ Publish preview failed!")
                        st.code(stderr, language="text")
        
        with col_pub2:
            if st.button("🚀 Publish Data", key="publish_btn", help="Publish cleaned data atomically"):
                with st.spinner("Publishing data..."):
                    success, stdout, stderr = run_pipeline_command("publish")
                    if success:
                        st.success("✅ Data published successfully!")
                        st.code(stdout, language="text")
                    else:
                        st.error("❌ Data publishing failed!")
                        st.code(stderr, language="text")
    
    with col2:
        st.subheader("📊 Current Status")
        
        # Get current pipeline status
        if st.button("🔄 Refresh Status", key="status_refresh"):
            st.rerun()
        
        # Show current status
        with st.spinner("Getting pipeline status..."):
            success, stdout, stderr = run_pipeline_command("status")
            if success:
                st.code(stdout, language="text")
            else:
                st.error("Failed to get pipeline status")
                st.code(stderr, language="text")
        
        # Publication management
        st.subheader("📦 Publication Management")
        
        if st.button("📋 List Publications", key="list_pubs_btn"):
            with st.spinner("Getting publication list..."):
                success, stdout, stderr = run_pipeline_command("list-publications")
                if success:
                    st.code(stdout, language="text")
                else:
                    st.error("Failed to list publications")
                    st.code(stderr, language="text")
        
        # Rollback functionality
        st.markdown("#### ⏪ Rollback")
        if st.button("📝 List Backups", key="list_backups_btn"):
            with st.spinner("Getting backup list..."):
                success, stdout, stderr = run_pipeline_command("rollback --list-backups")
                if success:
                    st.code(stdout, language="text")
                else:
                    st.error("Failed to list backups")
                    st.code(stderr, language="text")
        
        # Warning for rollback
        st.warning("⚠️ Rollback operations should be used with caution in production!")
    
    # Full pipeline execution
    st.markdown("---")
    st.subheader("🔄 Complete Pipeline")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🚀 Run Full Pipeline", key="full_pipeline_btn", help="Execute complete pipeline: ingest → validate → clean → publish"):
            st.warning("This will run the complete pipeline sequence. This may take several minutes.")
            
            if st.button("✅ Confirm Full Pipeline", key="confirm_full_pipeline"):
                with st.spinner("Running complete pipeline..."):
                    # Run each stage in sequence
                    stages = ["ingest", "validate", "clean", "publish"]
                    all_success = True
                    
                    for stage in stages:
                        st.info(f"Running {stage}...")
                        success, stdout, stderr = run_pipeline_command(stage)
                        
                        if success:
                            st.success(f"✅ {stage.title()} completed")
                        else:
                            st.error(f"❌ {stage.title()} failed")
                            st.code(stderr, language="text")
                            all_success = False
                            break
                    
                    if all_success:
                        st.success("🎉 Complete pipeline executed successfully!")
                    else:
                        st.error("❌ Pipeline execution failed at one of the stages.")
    
    with col2:
        st.info("""
        **Pipeline Stages:**
        1. **Ingest**: Downloads latest data from ODK Central
        2. **Validate**: Runs data quality checks using Great Expectations
        3. **Clean**: Applies cleaning rules and transformations
        4. **Publish**: Atomically publishes cleaned data to stable directory
        
        Each stage can be run individually or as part of the complete pipeline.
        """)

def show_system_status(config, pipeline_status):
    """Show system status dashboard"""
    st.header("⚙️ System Status")
    
    # Pipeline status
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Pipeline Status")
        
        # Use pipeline_status if available, otherwise check directories
        if pipeline_status and isinstance(pipeline_status, dict):
            if 'raw_items' in pipeline_status:
                st.metric("Raw Data Files", pipeline_status['raw_items'])
            if 'staging_items' in pipeline_status:
                st.metric("Staging Files", pipeline_status['staging_items'])
            if 'stable_items' in pipeline_status:
                st.metric("Published Files", pipeline_status['stable_items'])
        else:
            # Fallback: Check directories manually
            directories = [
                ("Raw Data", "raw"),
                ("Staging", "staging"),
                ("Cleaned Stable", "cleaned_stable"),
                ("Validation Results", "validation_results"),
                ("Logs", "logs")
            ]
            
            for name, dirname in directories:
                dir_path = project_root / dirname
                if dir_path.exists():
                    files = list(dir_path.glob("*"))
                    if files:
                        latest_file = max(files, key=lambda x: x.stat().st_mtime)
                        last_modified = datetime.fromtimestamp(latest_file.stat().st_mtime)
                        st.success(f"✅ {name}: Last updated {last_modified.strftime('%Y-%m-%d %H:%M')}")
                    else:
                        st.warning(f"⚠️ {name}: Directory empty")
                else:
                    st.error(f"❌ {name}: Directory not found")
    
    with col2:
        st.subheader("Configuration")
        
        # Show key configuration settings
        if config:
            # Display project info
            project_info = config.get('project', {})
            if project_info:
                st.json(project_info)
            
            # Display dashboard config
            dashboard_info = config.get('dashboard', {})
            if dashboard_info:
                st.markdown("**Dashboard Settings:**")
                st.json(dashboard_info)
        else:
            st.warning("No configuration loaded")
    
    # Show detailed pipeline status using CLI
    st.subheader("Detailed Pipeline Status")
    
    if st.button("🔄 Get Detailed Status"):
        with st.spinner("Getting pipeline status..."):
            success, stdout, stderr = run_pipeline_command("status")
            if success:
                st.code(stdout, language="text")
            else:
                st.error("Failed to get pipeline status")
                st.code(stderr, language="text")
    
    # Recent logs
    st.subheader("Recent Activity")
    
    logs_path = project_root / "logs"
    if logs_path.exists():
        log_files = sorted(logs_path.glob("*.log"), 
                          key=lambda x: x.stat().st_mtime, 
                          reverse=True)[:5]
        
        if log_files:
            for log_file in log_files:
                with st.expander(f"📝 {log_file.name}"):
                    try:
                        with open(log_file, 'r') as f:
                            # Show last 20 lines
                            lines = f.readlines()[-20:]
                            st.text(''.join(lines))
                    except Exception as e:
                        st.error(f"Cannot read log file: {str(e)}")
        else:
            st.info("No log files found")
    else:
        st.warning("Logs directory not found")
    
    # System health checks
    st.subheader("System Health")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Check if CLI is available
        if CLI_AVAILABLE:
            st.success("✅ CLI Available")
        else:
            st.error("❌ CLI Not Available")
    
    with col2:
        # Check if config is loaded
        if config:
            st.success("✅ Configuration Loaded")
        else:
            st.error("❌ Configuration Missing")
    
    with col3:
        # Check if required directories exist
        required_dirs = ["raw", "staging", "cleaned_stable", "logs"]
        missing_dirs = [d for d in required_dirs if not (project_root / d).exists()]
        
        if not missing_dirs:
            st.success("✅ All Directories Present")
        else:
            st.error(f"❌ Missing: {', '.join(missing_dirs)}")

if __name__ == "__main__":
    main()
