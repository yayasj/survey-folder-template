# ODK Central Data Export Fixes

## Problem Description

ODK Central data exports often have formatting issues that make the downloaded CSV files difficult to work with:

1. **Group Headers**: ODK forms with grouped questions can create unnamed columns in the export
2. **Missing Column Headers**: Complex form structures may result in columns without proper headers
3. **Empty Columns**: Group organization can create completely empty columns in the export
4. **Nested Group Paths**: Column names like `group1/subgroup/question` are verbose and problematic

## Solution Implemented

The `odk_client.py` module has been enhanced with comprehensive data processing to handle these issues:

### Key Features

1. **Automatic Column Header Fixing**
   - Detects unnamed columns (`Unnamed: X`, empty strings, NaN values)
   - Generates meaningful names like `field_6_unnamed_1`
   - Removes group prefixes from column names (`group1/question1` → `question1`)

2. **Empty Column Removal**
   - Identifies and removes completely empty columns
   - Handles both null values and empty strings
   - Reduces file size and complexity

3. **Data Value Cleaning**
   - Removes leading/trailing whitespace
   - Converts string representations of null values (`'nan'`, `'None'`, `''`) to proper pandas NA values
   - Standardizes data formats

4. **Fallback Export Method**
   - If the primary `get_table()` method fails, automatically tries alternative export approach
   - Uses submission listing and conversion as backup
   - Ensures data can be exported even from problematic forms

### Configuration Options

Add these settings to your `config.yml` under the `odk` section:

```yaml
odk:
  # ... existing config ...
  
  # Data processing options for handling ODK export issues
  clean_column_headers: true      # Fix unnamed columns and group header issues
  remove_empty_columns: true      # Remove completely empty columns from exports
  use_fallback_export: true       # Use alternative export method if primary fails
```

### Before and After Example

**Before (Problematic Export):**
```
respondent_id | name | group1/question1 | Unnamed: 5 | Unnamed: 6 |  | group2/nested/deep_question
R001         | John | Yes             |           |           |  | Answer
```

**After (Processed Export):**
```
respondent_id | name | question1 | field_8_unnamed_3 | deep_question
R001         | John | Yes      | orphaned_data    | Answer
```

### Benefits

- ✅ Clean, usable column names
- ✅ No unnamed or empty columns
- ✅ Reduced file size
- ✅ Better compatibility with data analysis tools
- ✅ Automatic handling of complex form structures
- ✅ Fallback methods for problematic forms

### Usage

The fixes are automatically applied when using the pipeline's ingest command:

```bash
# Download data with automatic formatting fixes
python -m survey_pipeline.cli ingest

# Test with dry run to see what would be processed
python -m survey_pipeline.cli ingest --dry-run

# Download specific forms only
python -m survey_pipeline.cli ingest --forms "household_survey,individual_assessment"
```

### Testing

Run the included test to verify the fixes work correctly:

```bash
python test_odk_fixes.py
```

This will demonstrate the data processing improvements with sample problematic data.
