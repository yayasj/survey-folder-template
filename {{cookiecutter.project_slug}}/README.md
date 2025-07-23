# {{cookiecutter.project_name}}

{{cookiecutter.project_description}}

## Project Information
- **Client:** {{cookiecutter.client_name}}
- **Survey Period:** {{cookiecutter.survey_start_date}} to {{cookiecutter.survey_end_date}}
- **Data Manager:** {{cookiecutter.data_manager_name}} ({{cookiecutter.data_manager_email}})
- **QA Officer:** {{cookiecutter.qa_officer_name}} ({{cookiecutter.qa_officer_email}})

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your ODK Central credentials
   ```

3. **Run initial data ingestion:**
   ```bash
   python -m survey_pipeline.cli ingest
   ```

4. **Start Streamlit dashboard:**
   ```bash
   streamlit run streamlit_app/app.py --server.port {{cookiecutter.streamlit_port}}
   ```

## Directory Structure

```
{{cookiecutter.project_slug}}/
├── config.yml                  # Survey-specific configuration
├── cleaning_rules.xlsx         # Data correction rules
├── expectations/               # Great Expectations validation suites
├── raw/                        # Raw ODK exports (daily backups)
├── staging/                    # Working directories
│   ├── raw/                    # Current run raw data
│   ├── failed/                 # Failed validation extracts
│   └── cleaned/                # Cleaned data awaiting promotion
├── cleaned_stable/             # Production-ready cleaned data
├── validation_results/         # Validation run results
├── data_docs/                  # Great Expectations HTML reports
├── logs/                       # Pipeline execution logs
├── streamlit_app/              # Dashboard application
└── flows/                      # Prefect workflow definitions
```

## Key Files

- `config.yml` - Main configuration file for ODK endpoints, datasets, and admin columns
- `cleaning_rules.xlsx` - Excel workbook for defining data cleaning rules
- `.env` - Environment variables for ODK credentials (DO NOT COMMIT)
- `flows/main_flow.py` - Main Prefect workflow orchestrating the entire pipeline

## Workflow Commands

```bash
# Manual pipeline execution
python -m survey_pipeline.cli ingest      # Download latest data from ODK
python -m survey_pipeline.cli validate    # Run Great Expectations validation
python -m survey_pipeline.cli clean       # Apply cleaning rules
python -m survey_pipeline.cli publish     # Promote to cleaned_stable/

# Automated execution (setup cron)
python -m survey_pipeline.cli run-pipeline  # Execute full pipeline
```

## Support

For technical issues or questions, contact:
- Technical Lead: [technical.lead@2mcorp.com]
- Documentation: [Link to full user guide]
