# Enhanced Manual Cleaning Rules Guide

## Overview
The manual rule type now supports **constraint-based targeting** using the `parameters` column. You can specify multiple field constraints to target exact observations for correction.

## How Manual Rules Work

### **Constraint-Based Targeting**
The `parameters` column allows you to specify field constraints in the format:
```
field1=value1;field2=value2;field3=value3
```

**All constraints must be met** for a record to be targeted (AND logic).

## Real Examples from Your Cleaning Rules

### **1. Household ID Corrections**
```
variable: household_id
rule_type: manual
parameters: instanceID=uuid:3a3b093f-1023-4636-a340-5d50ba119457
new_value: HH_9999
note: Correct HH ID error
```
**This targets the specific record with that exact instanceID and changes its household_id to HH_9999.**

### **2. Respondent ID Corrections**
```
variable: respondent_id  
rule_type: manual
parameters: instanceID=uuid:4f5b6548-296d-4566-a102-ad5e42f4ea39
new_value: HH_9999
note: Correct HH ID
```
**This targets the specific record with that instanceID and corrects the respondent_id.**

## Advanced Constraint Examples

### **Multiple Field Constraints**
```
variable: gender
rule_type: manual
parameters: household_id=HH_1234;age=25
new_value: Female
note: Fix gender for specific person in specific household
```
**This targets records where household_id=HH_1234 AND age=25.**

### **Cross-Dataset Constraints**
```
variable: income_last_month
rule_type: manual  
parameters: respondent_id=HH_5678;employment_status=unemployed
new_value: 0
note: Set income to 0 for unemployed respondent
```

### **Using Survey Metadata**
```
variable: phone_number
rule_type: manual
parameters: SubmitterName=John Doe;DeviceID=12345
new_value: +1234567890
note: Fix phone number for specific surveyor submission
```

## Supported Constraint Fields

Any column in your dataset can be used as a constraint field:

### **ODK Standard Fields:**
- `instanceID` - Unique record identifier
- `SubmitterID` - Surveyor identifier  
- `SubmitterName` - Surveyor name
- `DeviceID` - Device identifier
- `SubmissionDate` - When submitted

### **Survey-Specific Fields:**
- `household_id` - Household identifier
- `respondent_id` - Individual identifier
- `age`, `gender`, `income_last_month` - Any survey field
- Any custom field in your survey

## Data Type Handling

The enhanced system automatically handles data type conversions:

### **Numeric Fields:**
```
variable: age
parameters: respondent_id=HH_1234
new_value: 25
```
The system converts "25" to integer/float as needed.

### **String Fields:**
```
variable: occupation
parameters: instanceID=uuid:abc123
new_value: Teacher
```
String values are used directly.

## Step-by-Step Usage

### **1. Identify the Target Record**
Find the unique identifier for the record you want to change:
```python
# Check instanceID for a specific record
df[df['household_id'] == 'HH_PROBLEM']['instanceID'].iloc[0]
# Result: 'uuid:3a3b093f-1023-4636-a340-5d50ba119457'
```

### **2. Add Manual Rule to Excel**
Add a row to your cleaning rules file:
```
variable     | rule_type | parameters                                      | new_value | note           | priority | active
household_id | manual    | instanceID=uuid:3a3b093f-1023-4636-a340-5d50ba119457 | HH_FIXED  | Fix wrong ID   | 1        | TRUE
```

### **3. Test with Dry Run**
```bash
python -m survey_pipeline.cli clean --dry-run
```

### **4. Apply Changes**
```bash
python -m survey_pipeline.cli clean
```

### **5. Verify in Audit Trail**
Check the cleaning results to confirm the exact record was changed.

## Advanced Patterns

### **Conditional Corrections**
```
# Fix all unemployed people's income
variable: income_last_month
parameters: employment_status=unemployed
new_value: 0

# Fix children's education level  
variable: highest_level
parameters: age=10;currently_enrolled=yes
new_value: Primary

# Fix survey errors by specific surveyor
variable: gender
parameters: SubmitterName=BadSurveyor;gender=invalid_choice
new_value: Female
```

### **Batch Corrections**
```
# Fix multiple records with same issue
variable: occupation
parameters: main_activity=farming;occupation=
new_value: Farmer

# Standardize data entry errors
variable: marital_status  
parameters: marital_status=maried
new_value: married
```

## Best Practices

1. **Use Unique Identifiers**: `instanceID` is the most reliable constraint
2. **Test First**: Always use `--dry-run` to verify targeting
3. **Document Well**: Use descriptive notes explaining the correction
4. **Check Audit Trail**: Verify exact records were changed
5. **Multiple Constraints**: Use multiple fields for precise targeting

## Troubleshooting

### **No Changes Made**
- Check that constraint field names match exactly (case-sensitive)
- Verify constraint values exist in the data
- Ensure `active=TRUE` in the rules file

### **Too Many Changes**
- Add more specific constraints to narrow targeting
- Use `instanceID` for single-record targeting

### **Data Type Errors**
- The system handles conversions automatically
- Check logs for conversion warnings

## Example Workflow

```bash
# 1. Identify problem record
python3 -c "
import pandas as pd
df = pd.read_csv('staging/raw/individual_questionnaire.csv')
problem = df[df['age'] < 0]
print('Problem record instanceID:', problem['instanceID'].iloc[0])
"

# 2. Add manual rule to Excel file with that instanceID

# 3. Test the fix
python -m survey_pipeline.cli clean --dry-run

# 4. Apply the fix  
python -m survey_pipeline.cli clean
```

The enhanced manual rule system gives you precise control over individual record corrections while maintaining full audit trails!
