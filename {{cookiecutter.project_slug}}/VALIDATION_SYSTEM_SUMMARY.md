# Survey Pipeline Validation System Summary

## Overview
The validation system uses **Great Expectations** to validate datasets against predefined expectation suites, with comprehensive logging and failed row extraction.

## Configuration Sources

### 1. Main Configuration (`config.yml`)
The validation system reads configuration from:

```yaml
# Validation behavior configuration
validation:
  minimum_pass_rate: 85.0              # Minimum % of expectations that must pass
  fail_fast_on_critical: true          # Stop pipeline on critical validation failures
  save_failed_rows: true               # Extract failed rows to separate files
  abort_on_structural_failure: true
  continue_on_business_logic_failure: true
  max_iterations: 5
  create_backup: true
  rules_file: "cleaning_rules.xlsx"

# Dataset-to-suite mappings
datasets:
  household_dataset:
    file_pattern: "household*.csv"     # Files to match
    validation_suite: "household_suite" # Suite to use
    cleaning_rules: "household_cleaning_rules.xlsx"
    required: true
    primary_key: ["household_id", "submission_uuid"]
    
  individual_dataset:
    file_pattern: "individual*.csv"
    validation_suite: "individual_suite"
    cleaning_rules: "individual_cleaning_rules.xlsx"
    required: true
    primary_key: ["respondent_id"]

# Administrative columns to include in failed row extracts
admin_columns:
  - "district"
  - "community"
  - "enumerator_id"
  - "supervisor_id"
  - "form_version"
  - "submission_uuid"
  - "submission_date"
  - "gps_latitude"
  - "gps_longitude"

# Log configuration
logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

log_files:
  validation: "logs/validate_{date}.log"
```

### 2. Expectation Suites (`expectations/` directory)
Validation rules are defined in YAML files:

**Location:** `expectations/[suite_name].yml`

**Example structure:**
```yaml
expectations:
  - expectation_type: expect_table_columns_to_match_ordered_list
    kwargs:
      column_list: ["household_id", "num_members", ...]
    meta:
      severity: critical
      description: "Required columns must be present"
      
  - expectation_type: expect_column_values_to_not_be_null
    kwargs:
      column: household_id
      mostly: 1.0
    meta:
      severity: critical
      description: "Household ID must not be null"
```

## Data Search Locations

### 1. Input Data Search
- **Primary location:** `staging/raw/`
- **File patterns:** Defined in `config.yml` under `datasets.[dataset].file_pattern`
- **Search method:** Uses `fnmatch` pattern matching to find files
- **Supported formats:** CSV, Excel (.xlsx, .xls)

### 2. Suite Discovery
- **Location:** `expectations/`
- **Naming convention:** `[suite_name].yml`
- **Mapping:** Dataset file patterns → validation suites via config

## Validation Process Flow

1. **Dataset Discovery**
   - Scans `staging/raw/` for CSV files
   - Matches files against patterns in `datasets` config
   - Identifies corresponding validation suite for each file

2. **Suite Loading**
   - Loads expectation suite from `expectations/[suite_name].yml`
   - Parses expectations with severity levels (critical, warning, error)

3. **Validation Execution**
   - Runs each expectation against the dataset
   - Tracks pass/fail status and severity
   - Creates failure masks for failed row extraction

4. **Failed Row Extraction**
   - Identifies rows that failed any expectation
   - Adds administrative columns from `admin_columns` config
   - Adds validation timestamp

## Output and Logging

### 1. Validation Results
**Location:** `validation_results/[timestamp]/`
- `validation_summary.json` - Overall results
- Per-dataset results embedded in summary

**Structure:**
```json
{
  "run_timestamp": "2025-07-24_14-30-15",
  "total_datasets": 2,
  "validated_datasets": 2,
  "passed_datasets": 1,
  "failed_datasets": 1,
  "overall_pass_rate": 75.5,
  "overall_success": false,
  "dataset_results": {
    "household": {
      "pass_rate": 90.0,
      "failed_expectations": 2,
      "critical_failures": 1,
      "failed_rows_count": 15
    }
  }
}
```

### 2. Failed Row Extracts
**Location:** `staging/failed/[timestamp]/`
- `failed_rows_[dataset_name].csv` - Rows that failed validation
- Includes all original columns plus admin columns
- Prefixed with validation timestamp

### 3. Console Output
Real-time validation progress with:
- Dataset-by-dataset progress
- Pass/fail counts
- Critical failure warnings
- Overall summary with pass rates

### 4. Log Files
**Location:** `logs/validate_{date}.log` (configurable)
- Detailed validation execution logs
- Individual expectation results
- Error details and stack traces
- Performance metrics

## Usage Commands

### Validate All Datasets
```bash
python -m survey_pipeline.cli validate
```
- Finds all CSV files in `staging/raw/`
- Runs validation suites based on file patterns
- Produces overall validation summary

### Validate Specific Dataset
```bash
python -m survey_pipeline.cli validate --dataset household --suite household_suite
```
- Validates specific file against specified suite
- Suite auto-discovered if not specified

## Key Features

1. **Severity Levels:** Critical, error, warning expectations
2. **Failed Row Extraction:** Automatic extraction of problematic rows
3. **Administrative Context:** Includes geographic and submission metadata
4. **Pass Rate Thresholds:** Configurable minimum pass rates
5. **Pattern Matching:** Flexible file-to-suite mapping
6. **Great Expectations Integration:** Full GX compatibility with fallback support
7. **Comprehensive Logging:** Multiple output formats and detail levels

## Integration Points

- **CLI:** Primary interface for running validation
- **Streamlit Dashboard:** Real-time validation results viewing
- **Prefect Flows:** Automated pipeline integration
- **Data Cleaning:** Failed rows feed into cleaning process
- **Publishing:** Validation gates for data publishing

This system provides comprehensive data quality assurance with detailed tracking and extraction capabilities for survey data processing.
