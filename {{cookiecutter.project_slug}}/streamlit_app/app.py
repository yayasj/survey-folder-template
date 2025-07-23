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

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

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
            
            for json_file in latest_run.glob("*.json"):
                try:
                    import json
                    with open(json_file, 'r') as f:
                        latest_results[json_file.stem] = json.load(f)
                except Exception as e:
                    st.error(f"Error loading validation results: {str(e)}")
    
    return latest_results

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
    
    # Last updated
    st.sidebar.markdown("---")
    stable_path = project_root / "cleaned_stable"
    if stable_path.exists():
        last_updated = datetime.fromtimestamp(stable_path.stat().st_mtime)
        st.sidebar.markdown(f"**Last Updated:** {last_updated.strftime('%Y-%m-%d %H:%M')}")
    
    # Auto-refresh
    refresh_interval = dashboard_config.get('refresh_interval', 300)
    if st.sidebar.button("🔄 Refresh Data"):
        st.rerun()
    
    st.sidebar.markdown(f"*Auto-refresh every {refresh_interval//60} minutes*")
    
    # Main content based on selected section
    if selected_section == "📈 Overview":
        show_overview(data_files, validation_results, config)
    elif selected_section == "📊 Data Quality":
        show_data_quality(data_files, validation_results, config)
    elif selected_section == "🗺️ Geographic View":
        show_geographic_view(data_files, config)
    elif selected_section == "👥 Enumerator Performance":
        show_enumerator_performance(data_files, config)
    elif selected_section == "⚙️ System Status":
        show_system_status(config)

def show_overview(data_files, validation_results, config):
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
        # Calculate pass rate from validation results
        pass_rate = 95  # Placeholder
        st.metric("Quality Score", f"{pass_rate}%", delta="2%")
    
    with col4:
        st.metric("Days Active", "15", delta="1")
    
    # Dataset summary
    st.subheader("Dataset Summary")
    
    if data_files:
        summary_data = []
        for name, df in data_files.items():
            summary_data.append({
                "Dataset": name,
                "Records": len(df),
                "Columns": len(df.columns),
                "Last Updated": datetime.now().strftime("%Y-%m-%d %H:%M")
            })
        
        summary_df = pd.DataFrame(summary_data)
        st.dataframe(summary_df, use_container_width=True)
    
    # Submission trends (placeholder)
    st.subheader("Submission Trends")
    
    # Generate sample trend data
    dates = pd.date_range(start="2025-01-01", end="2025-01-15", freq="D")
    submissions = [50, 65, 80, 45, 70, 85, 90, 75, 60, 95, 100, 85, 70, 80, 75]
    
    trend_df = pd.DataFrame({
        "Date": dates,
        "Submissions": submissions
    })
    
    fig = px.line(trend_df, x="Date", y="Submissions", 
                  title="Daily Submissions", 
                  markers=True)
    st.plotly_chart(fig, use_container_width=True)

def show_data_quality(data_files, validation_results, config):
    """Show data quality dashboard"""
    st.header("📊 Data Quality Monitoring")
    
    if not data_files:
        st.warning("No data available in cleaned_stable directory.")
        return
    
    # Quality metrics
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Validation Results")
        
        if validation_results:
            for dataset, results in validation_results.items():
                with st.expander(f"📋 {dataset}"):
                    # Placeholder quality metrics
                    st.metric("Pass Rate", "94%")
                    st.metric("Failed Checks", "12")
                    st.metric("Warnings", "5")
        else:
            st.info("No validation results available. Run validation to see quality metrics.")
    
    with col2:
        st.subheader("Common Issues")
        
        # Placeholder common issues
        issues_data = [
            {"Issue": "Missing GPS coordinates", "Count": 25, "Severity": "Warning"},
            {"Issue": "Age out of range", "Count": 8, "Severity": "Error"},
            {"Issue": "Invalid gender codes", "Count": 12, "Severity": "Warning"},
            {"Issue": "Duplicate household IDs", "Count": 3, "Severity": "Critical"}
        ]
        
        issues_df = pd.DataFrame(issues_data)
        
        # Color code by severity
        def color_severity(val):
            if val == "Critical":
                return "color: red"
            elif val == "Error":
                return "color: orange"
            else:
                return "color: blue"
        
        styled_df = issues_df.style.applymap(color_severity, subset=['Severity'])
        st.dataframe(styled_df, use_container_width=True)
    
    # Quality trends
    st.subheader("Quality Trends")
    
    # Generate sample quality trend data
    dates = pd.date_range(start="2025-01-01", end="2025-01-15", freq="D")
    pass_rates = [85, 87, 90, 88, 92, 94, 95, 93, 91, 96, 97, 95, 94, 96, 94]
    
    quality_df = pd.DataFrame({
        "Date": dates,
        "Pass Rate": pass_rates
    })
    
    fig = px.line(quality_df, x="Date", y="Pass Rate",
                  title="Data Quality Pass Rate Over Time",
                  markers=True)
    fig.add_hline(y=90, line_dash="dash", line_color="red", 
                  annotation_text="Minimum Threshold (90%)")
    st.plotly_chart(fig, use_container_width=True)

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

def show_system_status(config):
    """Show system status dashboard"""
    st.header("⚙️ System Status")
    
    # Pipeline status
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Pipeline Status")
        
        # Check if directories exist and have recent activity
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
            st.json(config.get('project', {}))
        else:
            st.warning("No configuration loaded")
    
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

if __name__ == "__main__":
    main()
