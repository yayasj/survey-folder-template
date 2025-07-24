# Survey Data Dashboard

A comprehensive Streamlit dashboard for monitoring and controlling the survey data pipeline.

## Features

### 📈 Overview
- Real-time survey metrics and statistics
- Dataset summaries with actual record counts
- Submission trends using real submission dates
- Validation status summary

### 📊 Data Quality
- Real-time validation results from Great Expectations
- Detailed quality metrics per dataset
- Issue tracking and severity classification
- Quality trend visualization

### 🗺️ Geographic View
- Interactive map showing household locations (if GPS data available)
- GPS coverage and accuracy metrics
- Geographic distribution analysis

### 👥 Enumerator Performance
- Performance metrics by enumerator
- Submission counts and productivity tracking
- Performance comparison charts

### 🚀 Pipeline Control
- **Individual Stage Control**: Run ingest, validate, clean, or publish independently
- **Complete Pipeline**: Execute the full pipeline sequence
- **Preview Mode**: Dry-run publication to preview changes
- **Status Monitoring**: Real-time pipeline status
- **Publication Management**: List publications and manage rollbacks

### ⚙️ System Status
- Directory structure and file counts
- Configuration display
- Recent activity logs
- System health checks

## Usage

### Starting the Dashboard

```bash
# From the project root directory
streamlit run streamlit_app/app.py --server.port 8501
```

### Accessing the Dashboard

Once started, open your browser to:
- Local: http://localhost:8501
- Network: http://[your-ip]:8501

### Dashboard Sections

Navigate between sections using the sidebar menu:

1. **Overview**: High-level metrics and trends
2. **Data Quality**: Validation results and quality monitoring
3. **Geographic View**: Map visualization of survey locations
4. **Enumerator Performance**: Team productivity metrics
5. **Pipeline Control**: Execute and monitor pipeline operations
6. **System Status**: Technical system monitoring

## Pipeline Control Features

The Pipeline Control section allows you to:

### Individual Stage Control
- **Ingest**: Download latest data from ODK Central
- **Validate**: Run Great Expectations validation checks
- **Clean**: Apply data cleaning rules and transformations
- **Publish**: Atomically promote cleaned data to production

### Publication Management
- **Preview**: See what will be published before committing
- **Publish**: Execute atomic publication with automatic backups
- **List Publications**: View publication history
- **Rollback**: Restore previous versions if needed

### Complete Pipeline
- Execute the full sequence: ingest → validate → clean → publish
- Monitor progress of each stage
- Automatic error handling and reporting

## Data Integration

The dashboard automatically integrates with:

- **cleaned_stable/**: Published survey data
- **validation_results/**: Great Expectations validation outputs
- **logs/**: Pipeline execution logs
- **config.yml**: Project configuration
- **CLI Commands**: Direct pipeline control

## Configuration

Dashboard settings are controlled via `config.yml`:

```yaml
dashboard:
  port: 8501
  title: "Project Name - Data Dashboard"
  refresh_interval: 300  # seconds
  features:
    submission_trends: true
    quality_metrics: true
    geographical_view: true
    enumerator_performance: true
```

## Real-time Updates

The dashboard provides real-time monitoring through:

- Automatic data refresh from cleaned_stable directory
- Live validation results from latest validation runs
- Real-time pipeline status updates
- Interactive controls for immediate pipeline execution

## Security Notes

- The dashboard provides direct access to pipeline controls
- Use caution with rollback operations in production
- Pipeline execution may take several minutes for large datasets
- Always preview publications before executing them

## Troubleshooting

### Common Issues

1. **"No data available"**: Ensure the pipeline has been run and data exists in cleaned_stable/
2. **CLI commands failing**: Check that you're running from the correct directory with proper Python environment
3. **Missing validation results**: Run the validate command before viewing quality metrics
4. **Geographic view empty**: Ensure your data includes 'gps_latitude' and 'gps_longitude' columns

### Log Files

Check recent logs in the System Status section for detailed error information and pipeline execution history.
