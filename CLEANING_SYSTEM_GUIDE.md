# Cleaning Script Guide: How to Target Specific Observations

## Overview
The cleaning script uses an **Excel-based rules engine** that can target and clean specific observations using the `parameters` column. You were absolutely right - the `parameters` column is the proper way to specify targeting conditions, while `note` is for human-readable comments.

## How the Cleaning System Works

### **Rule Structure (Excel columns):**
- **`variable`**: Column name to clean
- **`rule_type`**: Type of operation (clamp, recode, manual, etc.)
- **`parameters`**: **KEY COLUMN** - Targeting specifications
- **`new_value`**: Replacement value  
- **`note`**: Human-readable description/comment
- **`priority`**: Execution order (lower = earlier)
- **`active`**: Enable/disable rule (TRUE/FALSE)

### **8 Rule Types Available:**
1. **`clamp`**: Restrict to ranges → `parameters: min=0;max=120`
2. **`recode`**: Map values → `parameters: "M":"Male";"F":"Female"`
3. **`replace_negative`**: Replace negatives → `parameters: replacement=0`
4. **`trim_whitespace`**: Remove whitespace → (no parameters)
5. **`pad_zeros`**: Add leading zeros → `parameters: length=6`
6. **`parse_date`**: Standardize dates → `parameters: format=%Y-%m-%d`
7. **`flag_outliers`**: Mark outliers → `parameters: method=iqr;threshold=3.0`
8. **`manual`**: **Target specific observations** → (see targeting methods below)

## ✅ **YES, You Can Clean Specific Observations!**

The **`manual`** rule type allows you to target specific observations using the `parameters` column:

### **Method 1: Target Specific Respondent/ID**
```
variable: gender
rule_type: manual
parameters: respondent_id=HH001_M001
new_value: Female
note: Fix gender for specific respondent
```

### **Method 2: Target by Row Index**
```
variable: occupation  
rule_type: manual
parameters: row_index=15
new_value: Student
note: Fix occupation in row 15
```

### **Method 3: Target by Condition**
```
# Fix impossible ages
variable: age
rule_type: manual  
parameters: condition=age>120
new_value: 25
note: Fix obviously wrong ages

# Fix children's education
variable: education
rule_type: manual
parameters: condition=age<18  
new_value: Primary
note: Set appropriate education for children

# Fix extreme incomes
variable: income
rule_type: manual
parameters: condition=income>1000000
new_value: 50000
note: Fix unrealistic income values
```

### **Method 4: Target by Value Match**
```
variable: marital_status
rule_type: manual
parameters: value=UNKNOWN
new_value: Single  
note: Replace UNKNOWN status with Single
```

### **Method 5: Target Null Values (Default)**
```
variable: phone_number
rule_type: manual
parameters: (leave empty)
new_value: N/A
note: Fill missing phone numbers
```

## Parameter Syntax Reference

### **Condition Formats:**
- **Greater than**: `condition=age>65`
- **Less than**: `condition=income<0`  
- **Equals**: `condition=status==INVALID`
- **Cross-variable**: `condition=age<18` (affects any variable)

### **ID Targeting:**
- **Specific ID**: `respondent_id=HH001_M001`
- **Row number**: `row_index=42`
- **Value match**: `value=MISSING`

## How to Use

### **Step 1: Edit cleaning_rules.xlsx**
Add a new row with these values:
```
variable     | rule_type | parameters              | new_value | note           | priority | active
gender       | manual    | respondent_id=HH001_M001| Female    | Fix gender     | 4        | TRUE
```

### **Step 2: Test with Dry Run**
```bash
python -m survey_pipeline.cli clean --dry-run
```

### **Step 3: Apply Changes**
```bash  
python -m survey_pipeline.cli clean
```

### **Step 4: Check Audit Trail**
The system creates detailed audit trails showing:
- Which rules were applied
- How many records changed
- Before/after values for sample records
- Timestamps and change details

## Real Examples Currently in System

```
1. Target Specific Person:
   parameters: respondent_id=HH001_M001 → Fix gender for HH001_M001

2. Fix Extreme Values:  
   parameters: condition=income>1000000 → Cap unrealistic incomes

3. Age-Based Rules:
   parameters: condition=age<18 → Set children's education to Primary

4. Specific Row Fix:
   parameters: row_index=15 → Fix occupation in row 15
```

## Best Practices

1. **Use `parameters` for targeting**, `note` for documentation
2. **Test with --dry-run first** to preview changes
3. **Use higher priority numbers** (4+) for manual rules  
4. **Be specific with conditions** to avoid unintended changes
5. **Check audit trails** after cleaning to verify results

The cleaning system now properly supports targeting specific observations using the `parameters` column as originally designed!
