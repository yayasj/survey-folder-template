# Enhanced Cleaning Rules Guide

## Updated Rule Types (flag_outliers removed)

The cleaning system now supports **11 rule types** for comprehensive data cleaning:

### **Existing Rule Types:**
1. **`clamp`** - Restrict numeric values to ranges
2. **`recode`** - Map/transform values  
3. **`replace_negative`** - Replace negative numbers
4. **`trim_whitespace`** - Remove leading/trailing spaces
5. **`pad_zeros`** - Add leading zeros to strings
6. **`parse_date`** - Standardize date formats
7. **`manual`** - Constraint-based manual corrections

### **New String Formatting Rules:**
8. **`proper`** - Title Case formatting (First Letter Capitalized)
9. **`lower`** - Convert to lowercase
10. **`upper`** - Convert to UPPERCASE

### **New Regex-Based Rules:**
11. **`regex_replace`** - Pattern-based string replacements

---

## New Rule Types Documentation

### **1. PROPER Case Formatting**
Converts text to Title Case (first letter of each word capitalized).

**Parameters:** None  
**Example:**
```
variable: occupation
rule_type: proper
parameters: (empty)
new_value: (empty)
note: Apply proper case formatting to occupation names
```
**Result:** `"teacher"` → `"Teacher"`, `"FARMER"` → `"Farmer"`

---

### **2. LOWER Case Formatting**
Converts all text to lowercase.

**Parameters:** None  
**Example:**
```
variable: comments
rule_type: lower
parameters: (empty)
new_value: (empty)
note: Convert comments to lowercase for consistency
```
**Result:** `"GOOD SURVEY"` → `"good survey"`

---

### **3. UPPER Case Formatting**
Converts all text to UPPERCASE.

**Parameters:** None  
**Example:**
```
variable: country_code
rule_type: upper
parameters: (empty)
new_value: (empty) 
note: Standardize country codes to uppercase
```
**Result:** `"usa"` → `"USA"`

---

### **4. REGEX_REPLACE - Pattern-Based Replacements**
Powerful pattern matching and replacement system with 5 different strategies.

#### **4a. Exact Match Replacement**
Replace values that match exactly.

**Parameters:** `exact=pattern`  
**Example:**
```
variable: marital_status
rule_type: regex_replace
parameters: exact=maried
new_value: married
note: Fix common spelling error
```
**Result:** Only `"maried"` → `"married"` (exact match only)

#### **4b. Starts With Replacement**
Replace entire values that start with a pattern.

**Parameters:** `startswith=pattern`  
**Example:**
```
variable: education_level
rule_type: regex_replace
parameters: startswith=prim
new_value: Primary
note: Standardize education levels starting with prim
```
**Result:** `"primary school"` → `"Primary"`, `"primary education"` → `"Primary"`

#### **4c. Ends With Replacement**
Replace entire values that end with a pattern.

**Parameters:** `endswith=pattern`  
**Example:**
```
variable: file_name
rule_type: regex_replace
parameters: endswith=.tmp
new_value: .csv
note: Convert temporary files to CSV
```
**Result:** `"data.tmp"` → `.csv`, `"report.tmp"` → `.csv`

#### **4d. Contains Replacement**
Replace entire values that contain a pattern.

**Parameters:** `contains=pattern`  
**Example:**
```
variable: comment
rule_type: regex_replace
parameters: contains=test
new_value: survey
note: Replace test references with survey
```
**Result:** `"test data"` → `"survey"`, `"testing phase"` → `"survey"`

#### **4e. Regex Pattern Replacement**
Advanced regex replacement within strings (most powerful).

**Parameters:** `regex=pattern`  
**Examples:**

**Remove non-digits from phone numbers:**
```
variable: phone_number
rule_type: regex_replace
parameters: regex=[^0-9]
new_value: (empty string)
note: Remove non-digit characters from phone numbers
```
**Result:** `"123-456-7890"` → `"1234567890"`

**Replace multiple spaces with single space:**
```
variable: address
rule_type: regex_replace
parameters: regex=\s+
new_value: " "
note: Replace multiple spaces with single space
```
**Result:** `"123  Main   St"` → `"123 Main St"`

**Remove special characters:**
```
variable: name
rule_type: regex_replace
parameters: regex=[^a-zA-Z\s]
new_value: (empty string)
note: Remove special characters from names
```
**Result:** `"John@123"` → `"John"`

---

## Real-World Examples

### **Data Standardization Workflow:**
```
# Step 1: Clean whitespace
variable: occupation
rule_type: trim_whitespace
priority: 1

# Step 2: Apply proper case
variable: occupation  
rule_type: proper
priority: 2

# Step 3: Fix common errors
variable: occupation
rule_type: regex_replace
parameters: exact=Teecher
new_value: Teacher
priority: 3
```

### **Phone Number Cleaning:**
```
# Remove all non-digits
variable: phone_number
rule_type: regex_replace
parameters: regex=[^0-9]
new_value: (empty)
note: Clean phone numbers to digits only
```

### **Address Standardization:**
```
# Fix multiple spaces
variable: address
rule_type: regex_replace
parameters: regex=\s+
new_value: " "

# Standardize street abbreviations
variable: address
rule_type: regex_replace
parameters: contains=Street
new_value: St
```

### **Gender Data Cleaning:**
```
# Step 1: Standard recoding
variable: gender
rule_type: recode
parameters: "M":"Male";"F":"Female"

# Step 2: Fix case issues
variable: gender
rule_type: proper
```

---

## Best Practices

### **Rule Ordering:**
1. **Priority 1**: Basic cleaning (trim_whitespace, clamp)
2. **Priority 2**: Formatting (proper, lower, upper)
3. **Priority 3**: Pattern fixes (regex_replace)
4. **Priority 4**: Manual corrections

### **Regex Pattern Tips:**
- **`[^0-9]`** - Match anything except digits
- **`\s+`** - Match one or more whitespace characters
- **`[a-zA-Z]`** - Match letters only
- **`^pattern`** - Match pattern at start of string
- **`pattern$`** - Match pattern at end of string

### **Testing Workflow:**
1. Add rules to Excel file
2. Test with `--dry-run` first
3. Check audit trail for results
4. Apply with `clean` command

The enhanced cleaning system now provides comprehensive string processing capabilities for survey data standardization!
