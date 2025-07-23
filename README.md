# Survey Folder Template

A Cookiecutter template for creating standardized survey data management projects using the 2M Corp pipeline.

## Features

🚀 **Complete Project Scaffolding**
- ODK Central integration ready
- Prefect 2.x workflow orchestration
- Streamlit dashboard for monitoring
- Great Expectations data validation
- Docker deployment configuration

📊 **Production-Ready Dashboard**
- Multi-section interface (Overview, Data Quality, Geographic, Performance)
- Auto-refresh capability
- Responsive design for client access

⚡ **Automated Pipeline**
- Daily data ingestion from ODK Central
- Validation with Great Expectations
- Configurable data cleaning rules
- Atomic publishing to stable directory

🛠️ **Developer Experience**
- CLI interface with Click
- Makefile for common tasks
- Docker development environment
- Comprehensive documentation

## Quick Start

1. **Install Cookiecutter:**
   ```bash
   pip install cookiecutter
   ```

2. **Generate a new survey project:**
   ```bash
   cookiecutter https://github.com/2mcorp/survey-folder-template
   ```

3. **Follow the prompts to configure your project:**
   - Project name and description
   - ODK Central URL and credentials
   - Client information
   - Team member details

4. **Set up the generated project:**
   ```bash
   cd your_project_name
   ./setup.sh
   ```

## Template Configuration

The template accepts these configuration parameters:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `project_name` | Human-readable project name | "Household Survey 2025" |
| `project_slug` | Machine-readable identifier | "household_survey_2025" |
| `project_description` | Brief project description | "National household survey data pipeline" |
| `odk_central_url` | ODK Central server URL | "https://your-odk-central.com/" |
| `client_name` | Client organization name | "Ministry of Health" |
| `survey_start_date` | Survey start date | "2025-01-01" |
| `survey_end_date` | Survey end date | "2025-12-31" |
| `data_manager_name` | Data manager name | "John Doe" |
| `data_manager_email` | Data manager email | "john.doe@example.com" |
| `qa_officer_name` | QA officer name | "Jane Smith" |
| `qa_officer_email` | QA officer email | "jane.smith@example.com" |
| `streamlit_port` | Dashboard port number | "8501" |

## Generated Project Structure

```
your_project_name/
├── 📋 Configuration
│   ├── config.yml                  # Main survey configuration
│   ├── .env.example               # Environment variables template
│   └── cleaning_rules.csv         # Data cleaning rules template
├── 📊 Data Directories
│   ├── raw/                       # ODK Central exports archive
│   ├── staging/                   # Processing workspace
│   ├── cleaned_stable/            # Production-ready data
│   ├── validation_results/        # Great Expectations output
│   └── logs/                      # Pipeline execution logs
├── 🔧 Pipeline Code
│   ├── survey_pipeline/           # Core Python package
│   │   ├── cli.py                # Command-line interface
│   │   ├── config.py             # Configuration management
│   │   └── utils.py              # Utility functions
│   ├── flows/                     # Prefect workflow definitions
│   └── expectations/             # Great Expectations suites
├── 📱 Dashboard
│   └── streamlit_app/app.py      # Interactive dashboard
├── 🐳 Deployment
│   ├── Dockerfile                # Container definition
│   ├── docker-compose.yml        # Production deployment
│   └── docker-compose.dev.yml    # Development environment
└── 🔨 Utilities
    ├── Makefile                   # Common tasks
    ├── setup.sh                  # Project setup script
    └── requirements.txt          # Python dependencies
```

## Workflow Overview

The generated project implements this data flow:

```
ODK Central → Ingest → Validate → Clean → Publish → Dashboard
     ↑            ↓         ↓        ↓       ↓         ↓
  Raw Data   Staging/   Failed   Cleaned  Stable   Client
             Raw/      Rows     Data     Data     Access
```

### Key Components

1. **Ingestion**: Download data from ODK Central using pyODK
2. **Validation**: Apply Great Expectations suites with admin column enrichment
3. **Cleaning**: Process Excel-based cleaning rules with audit trail
4. **Publishing**: Atomic directory swap to ensure data consistency
5. **Dashboard**: Real-time monitoring via Streamlit

## Usage Guide

### Daily Operations

```bash
# Manual pipeline execution
make ingest          # Download latest ODK data
make validate        # Run data validation
make clean-data      # Apply cleaning rules
make publish         # Promote to stable directory

# Automated execution
make run-pipeline    # Execute complete pipeline

# Monitoring
make run-dashboard   # Start Streamlit dashboard
make status          # Check pipeline status
```

### Configuration Management

1. **Update ODK credentials** in `.env`:
   ```bash
   ODK_USERNAME=your_username
   ODK_PASSWORD=your_password
   ODK_PROJECT_ID=your_project_id
   ```

2. **Configure survey details** in `config.yml`:
   - Project information
   - Dataset definitions
   - Validation settings
   - Admin columns for failed row extracts

3. **Define cleaning rules** in `cleaning_rules.xlsx`:
   - Variable-specific rules
   - Rule parameters
   - Priority and activation flags

### Development

```bash
# Development environment
make setup           # Initialize virtual environment
make docker-dev      # Run with Docker development setup
make test            # Run test suite
make lint            # Code quality checks
```

## Requirements

- Python 3.10+
- ODK Central server access
- 4GB+ RAM for large datasets
- Docker (optional, for containerized deployment)

## Dependencies

The template generates projects with these key dependencies:

- **Data Processing**: pandas, numpy, pyodk, openpyxl
- **Workflow**: prefect 2.x
- **Validation**: great-expectations
- **Dashboard**: streamlit, plotly, folium
- **Configuration**: python-dotenv, pyyaml, click

## Support

For technical support or feature requests:

- **Documentation**: [Link to full user guide]
- **Issues**: [GitHub Issues](https://github.com/2mcorp/survey-folder-template/issues)
- **Email**: info@2m-corp.com

## License

This template is open source and available under the MIT License.

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Changelog

### v1.0.0 (2025-07-23)
- Initial release with complete Cookiecutter template
- Prefect 2.x workflow orchestration
- Streamlit dashboard with production deployment
- Docker containerization support
- CLI interface with comprehensive commands
- Great Expectations validation framework
- Excel-based cleaning rules engine

---

**Created by 2M Corp Data Team** | **Powered by Cookiecutter**
