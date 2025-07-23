"""
Excel-based Data Cleaning Rules Engine
Handles data cleaning based on Excel-defined rules with audit trail
"""

import logging
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Union
import pandas as pd
import numpy as np
from copy import deepcopy

from .config import load_config
from .utils import save_run_metadata, create_run_timestamp, ensure_directory

logger = logging.getLogger(__name__)

class DataCleaningEngine:
    """Excel-based data cleaning engine with audit trail"""
    
    def __init__(self, config: Dict[str, Any], project_root: Path):
        """
        Initialize cleaning engine
        
        Args:
            config: Configuration dictionary
            project_root: Project root directory
        """
        self.config = config
        self.project_root = project_root
        self.cleaning_config = config.get('cleaning', {})
        
        # Set up audit trail
        self.audit_trail = []
        self.records_modified = 0
        self.rules_applied = 0
        
    def load_cleaning_rules(self, rules_file: str) -> pd.DataFrame:
        """
        Load cleaning rules from Excel file
        
        Args:
            rules_file: Path to Excel rules file
            
        Returns:
            DataFrame with cleaning rules
        """
        try:
            rules_path = Path(rules_file)
            if not rules_path.is_absolute():
                rules_path = self.project_root / rules_path
            
            if not rules_path.exists():
                raise FileNotFoundError(f"Rules file not found: {rules_path}")
            
            # Load rules from Excel
            if rules_path.suffix.lower() == '.xlsx':
                rules_df = pd.read_excel(rules_path)
            elif rules_path.suffix.lower() == '.csv':
                rules_df = pd.read_csv(rules_path)
            else:
                raise ValueError(f"Unsupported rules file format: {rules_path.suffix}")
            
            # Validate required columns
            required_columns = ['variable', 'rule_type', 'active']
            missing_columns = [col for col in required_columns if col not in rules_df.columns]
            if missing_columns:
                raise ValueError(f"Rules file missing required columns: {missing_columns}")
            
            # Filter to active rules only
            active_rules = rules_df[rules_df['active'].astype(str).str.upper() == 'TRUE'].copy()
            
            # Sort by priority (lower numbers = higher priority)
            if 'priority' in active_rules.columns:
                active_rules = active_rules.sort_values('priority', na_position='last')
            
            logger.info(f"Loaded {len(active_rules)} active cleaning rules from {rules_path}")
            return active_rules
            
        except Exception as e:
            logger.error(f"Failed to load cleaning rules: {str(e)}")
            raise
    
    def clean_dataset(
        self, 
        dataset_path: Path, 
        rules_file: str,
        max_iterations: int = 5,
        output_path: Optional[Path] = None
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Clean a single dataset using Excel rules
        
        Args:
            dataset_path: Path to dataset file
            rules_file: Path to Excel rules file
            max_iterations: Maximum cleaning iterations
            output_path: Optional output path for cleaned data
            
        Returns:
            Tuple of (cleaned_dataframe, cleaning_results)
        """
        try:
            logger.info(f"Starting cleaning for {dataset_path.name}")
            
            # Load dataset
            if dataset_path.suffix.lower() == '.csv':
                df = pd.read_csv(dataset_path)
            elif dataset_path.suffix.lower() in ['.xlsx', '.xls']:
                df = pd.read_excel(dataset_path)
            else:
                raise ValueError(f"Unsupported file format: {dataset_path.suffix}")
            
            original_df = df.copy()
            logger.info(f"Loaded dataset: {len(df)} rows, {len(df.columns)} columns")
            
            # Load cleaning rules
            rules_df = self.load_cleaning_rules(rules_file)
            
            # Reset audit trail for this dataset
            self.audit_trail = []
            self.records_modified = 0
            self.rules_applied = 0
            
            # Apply cleaning rules iteratively
            for iteration in range(max_iterations):
                logger.info(f"Cleaning iteration {iteration + 1}/{max_iterations}")
                
                df_before = df.copy()
                iteration_changes = 0
                
                # Apply each rule
                for _, rule in rules_df.iterrows():
                    changes = self._apply_rule(df, rule)
                    iteration_changes += changes
                
                logger.info(f"Iteration {iteration + 1}: {iteration_changes} changes made")
                
                # If no changes were made, we can stop early
                if iteration_changes == 0:
                    logger.info(f"No changes in iteration {iteration + 1}, stopping early")
                    break
            
            # Generate cleaning results
            cleaning_results = {
                'dataset_name': dataset_path.stem,
                'original_rows': len(original_df),
                'cleaned_rows': len(df),
                'original_columns': len(original_df.columns),
                'cleaned_columns': len(df.columns),
                'records_modified': self.records_modified,
                'rules_applied': self.rules_applied,
                'iterations_completed': iteration + 1,
                'audit_trail': self.audit_trail.copy()
            }
            
            # Save cleaned dataset if output path provided
            if output_path:
                ensure_directory(output_path.parent)
                df.to_csv(output_path, index=False)
                cleaning_results['output_path'] = str(output_path)
                logger.info(f"Saved cleaned data to {output_path}")
            
            logger.info(f"✅ Cleaning complete: {self.records_modified} records modified, "
                       f"{self.rules_applied} rules applied")
            
            return df, cleaning_results
            
        except Exception as e:
            logger.error(f"Cleaning failed for {dataset_path.name}: {str(e)}")
            raise
    
    def _apply_rule(self, df: pd.DataFrame, rule: pd.Series) -> int:
        """
        Apply a single cleaning rule to the dataframe
        
        Args:
            df: DataFrame to clean
            rule: Rule specification
            
        Returns:
            Number of changes made
        """
        try:
            variable = rule['variable']
            rule_type = rule['rule_type']
            parameters = rule.get('parameters', '')
            new_value = rule.get('new_value', '')
            note = rule.get('note', '')
            
            if variable not in df.columns:
                logger.warning(f"Column '{variable}' not found, skipping rule: {rule_type}")
                return 0
            
            changes_made = 0
            
            # Track original values for audit
            original_values = df[variable].copy()
            
            if rule_type == 'clamp':
                changes_made = self._apply_clamp_rule(df, variable, parameters)
                
            elif rule_type == 'recode':
                changes_made = self._apply_recode_rule(df, variable, parameters)
                
            elif rule_type == 'replace_negative':
                changes_made = self._apply_replace_negative_rule(df, variable, parameters)
                
            elif rule_type == 'trim_whitespace':
                changes_made = self._apply_trim_whitespace_rule(df, variable)
                
            elif rule_type == 'pad_zeros':
                changes_made = self._apply_pad_zeros_rule(df, variable, parameters)
                
            elif rule_type == 'parse_date':
                changes_made = self._apply_parse_date_rule(df, variable, parameters)
                
            elif rule_type == 'flag_outliers':
                changes_made = self._apply_flag_outliers_rule(df, variable, parameters)
                
            elif rule_type == 'manual':
                changes_made = self._apply_manual_rule(df, variable, new_value, note)
                
            else:
                logger.warning(f"Unknown rule type: {rule_type}")
                return 0
            
            # Add to audit trail if changes were made
            if changes_made > 0:
                self._add_to_audit_trail(
                    rule_type=rule_type,
                    variable=variable,
                    changes_made=changes_made,
                    parameters=parameters,
                    note=note,
                    original_values=original_values,
                    new_values=df[variable].copy()
                )
                
                self.rules_applied += 1
                self.records_modified += changes_made
            
            return changes_made
            
        except Exception as e:
            logger.error(f"Failed to apply rule {rule_type} to {variable}: {str(e)}")
            return 0
    
    def _apply_clamp_rule(self, df: pd.DataFrame, variable: str, parameters: str) -> int:
        """Apply clamping rule to restrict values to a range"""
        try:
            # Parse parameters like "min=0;max=120"
            params = self._parse_parameters(parameters)
            min_val = float(params.get('min', float('-inf')))
            max_val = float(params.get('max', float('inf')))
            
            # Only clamp numeric columns
            if not pd.api.types.is_numeric_dtype(df[variable]):
                df[variable] = pd.to_numeric(df[variable], errors='coerce')
            
            original_values = df[variable].copy()
            df[variable] = df[variable].clip(lower=min_val, upper=max_val)
            
            changes = (original_values != df[variable]).sum()
            logger.debug(f"Clamped {changes} values in {variable} to range [{min_val}, {max_val}]")
            return changes
            
        except Exception as e:
            logger.error(f"Failed to apply clamp rule: {str(e)}")
            return 0
    
    def _apply_recode_rule(self, df: pd.DataFrame, variable: str, parameters: str) -> int:
        """Apply recoding rule to map values"""
        try:
            # Parse parameters like '"M":"Male";"F":"Female"'
            # Handle the complex format with quotes
            recode_map = {}
            
            # Remove outer quotes and split on semicolon
            params = parameters.strip('\'"')
            if params:
                pairs = params.split(';')
                for pair in pairs:
                    if ':' in pair:
                        key, value = pair.split(':', 1)
                        # Remove quotes from key and value
                        key = key.strip('\'"')
                        value = value.strip('\'"')
                        recode_map[key] = value
            
            if not recode_map:
                return 0
            
            original_values = df[variable].copy()
            df[variable] = df[variable].map(recode_map).fillna(df[variable])
            
            changes = (original_values != df[variable]).sum()
            logger.debug(f"Recoded {changes} values in {variable} using map: {recode_map}")
            return changes
            
        except Exception as e:
            logger.error(f"Failed to apply recode rule: {str(e)}")
            return 0
    
    def _apply_replace_negative_rule(self, df: pd.DataFrame, variable: str, parameters: str) -> int:
        """Replace negative values with specified replacement"""
        try:
            params = self._parse_parameters(parameters)
            replacement = float(params.get('replacement', 0))
            
            # Only apply to numeric columns
            if not pd.api.types.is_numeric_dtype(df[variable]):
                df[variable] = pd.to_numeric(df[variable], errors='coerce')
            
            mask = df[variable] < 0
            changes = mask.sum()
            df.loc[mask, variable] = replacement
            
            logger.debug(f"Replaced {changes} negative values in {variable} with {replacement}")
            return changes
            
        except Exception as e:
            logger.error(f"Failed to apply replace_negative rule: {str(e)}")
            return 0
    
    def _apply_trim_whitespace_rule(self, df: pd.DataFrame, variable: str) -> int:
        """Trim whitespace from string values"""
        try:
            if not pd.api.types.is_string_dtype(df[variable]):
                df[variable] = df[variable].astype(str)
            
            original_values = df[variable].copy()
            df[variable] = df[variable].str.strip()
            
            changes = (original_values != df[variable]).sum()
            logger.debug(f"Trimmed whitespace from {changes} values in {variable}")
            return changes
            
        except Exception as e:
            logger.error(f"Failed to apply trim_whitespace rule: {str(e)}")
            return 0
    
    def _apply_pad_zeros_rule(self, df: pd.DataFrame, variable: str, parameters: str) -> int:
        """Pad numeric strings with leading zeros"""
        try:
            params = self._parse_parameters(parameters)
            length = int(params.get('length', 6))
            
            original_values = df[variable].copy()
            df[variable] = df[variable].astype(str).str.zfill(length)
            
            changes = (original_values != df[variable]).sum()
            logger.debug(f"Padded {changes} values in {variable} to length {length}")
            return changes
            
        except Exception as e:
            logger.error(f"Failed to apply pad_zeros rule: {str(e)}")
            return 0
    
    def _apply_parse_date_rule(self, df: pd.DataFrame, variable: str, parameters: str) -> int:
        """Parse and standardize date formats"""
        try:
            params = self._parse_parameters(parameters)
            date_format = params.get('format', '%Y-%m-%d')
            
            original_values = df[variable].copy()
            df[variable] = pd.to_datetime(df[variable], format=date_format, errors='coerce')
            
            # Count successful conversions as changes
            changes = (~original_values.isna() & ~df[variable].isna()).sum()
            logger.debug(f"Parsed {changes} dates in {variable} using format {date_format}")
            return changes
            
        except Exception as e:
            logger.error(f"Failed to apply parse_date rule: {str(e)}")
            return 0
    
    def _apply_flag_outliers_rule(self, df: pd.DataFrame, variable: str, parameters: str) -> int:
        """Flag outliers for review (adds a flag column)"""
        try:
            params = self._parse_parameters(parameters)
            method = params.get('method', 'iqr')
            threshold = float(params.get('threshold', 3.0))
            
            # Only apply to numeric columns
            if not pd.api.types.is_numeric_dtype(df[variable]):
                df[variable] = pd.to_numeric(df[variable], errors='coerce')
            
            flag_column = f"{variable}_outlier_flag"
            
            if method == 'iqr':
                Q1 = df[variable].quantile(0.25)
                Q3 = df[variable].quantile(0.75)
                IQR = Q3 - Q1
                outlier_mask = (df[variable] < (Q1 - threshold * IQR)) | (df[variable] > (Q3 + threshold * IQR))
            else:
                # Z-score method
                z_scores = np.abs((df[variable] - df[variable].mean()) / df[variable].std())
                outlier_mask = z_scores > threshold
            
            df[flag_column] = outlier_mask
            changes = outlier_mask.sum()
            
            logger.debug(f"Flagged {changes} outliers in {variable} using {method} method")
            return changes
            
        except Exception as e:
            logger.error(f"Failed to apply flag_outliers rule: {str(e)}")
            return 0
    
    def _apply_manual_rule(self, df: pd.DataFrame, variable: str, new_value: str, note: str) -> int:
        """Apply manual correction based on note pattern"""
        try:
            # Extract conditions from note (e.g., "Fix specific incorrect gender entry for HH001_M001")
            # This is a simplified implementation - could be enhanced for more complex conditions
            
            if not new_value or pd.isna(new_value):
                return 0
            
            # For demonstration, apply to any null values
            mask = df[variable].isna()
            changes = mask.sum()
            
            if changes > 0:
                df.loc[mask, variable] = new_value
                logger.debug(f"Applied manual rule to {changes} values in {variable}: {note}")
            
            return changes
            
        except Exception as e:
            logger.error(f"Failed to apply manual rule: {str(e)}")
            return 0
    
    def _parse_parameters(self, parameters: str) -> Dict[str, str]:
        """Parse parameter string like 'min=0;max=120' into dictionary"""
        params = {}
        if not parameters or pd.isna(parameters):
            return params
        
        try:
            pairs = parameters.split(';')
            for pair in pairs:
                if '=' in pair:
                    key, value = pair.split('=', 1)
                    params[key.strip()] = value.strip()
        except Exception as e:
            logger.warning(f"Failed to parse parameters '{parameters}': {str(e)}")
        
        return params
    
    def _add_to_audit_trail(
        self, 
        rule_type: str, 
        variable: str, 
        changes_made: int,
        parameters: str, 
        note: str,
        original_values: pd.Series,
        new_values: pd.Series
    ):
        """Add entry to audit trail"""
        
        # Find changed rows
        changed_mask = original_values != new_values
        changed_indices = changed_mask[changed_mask].index.tolist()
        
        audit_entry = {
            'timestamp': datetime.now().isoformat(),
            'rule_type': rule_type,
            'variable': variable,
            'changes_made': changes_made,
            'parameters': parameters,
            'note': note,
            'changed_rows': changed_indices[:100]  # Limit to first 100 for performance
        }
        
        # Add sample of changes for review
        if len(changed_indices) > 0:
            sample_size = min(5, len(changed_indices))
            sample_indices = changed_indices[:sample_size]
            
            audit_entry['sample_changes'] = [
                {
                    'row_index': idx,
                    'original_value': str(original_values.iloc[idx]),
                    'new_value': str(new_values.iloc[idx])
                }
                for idx in sample_indices
            ]
        
        self.audit_trail.append(audit_entry)
    
    def clean_all_datasets(
        self, 
        run_timestamp: str,
        rules_file: str = "cleaning_rules.xlsx",
        max_iterations: int = 5
    ) -> Dict[str, Any]:
        """
        Clean all datasets in staging area
        
        Args:
            run_timestamp: Timestamp for this cleaning run
            rules_file: Default path to Excel rules file (used if dataset-specific not found)
            max_iterations: Maximum cleaning iterations per dataset
            
        Returns:
            Overall cleaning results
        """
        try:
            staging_path = self.project_root / "staging" / "raw"
            cleaned_path = self.project_root / "staging" / "cleaned" / run_timestamp
            ensure_directory(cleaned_path)
            
            # Get dataset configurations
            datasets_config = self.config.get('datasets', {})
            
            # Find all CSV files in staging
            csv_files = list(staging_path.glob("*.csv"))
            logger.info(f"Found {len(csv_files)} datasets to clean")
            
            overall_results = {
                'run_timestamp': run_timestamp,
                'rules_file': rules_file,
                'total_datasets': len(csv_files),
                'cleaned_datasets': 0,
                'failed_datasets': 0,
                'total_records_modified': 0,
                'total_rules_applied': 0,
                'dataset_results': {},
                'overall_success': True
            }
            
            for csv_file in csv_files:
                dataset_name = csv_file.stem
                output_path = cleaned_path / f"{dataset_name}.csv"
                
                # Find dataset-specific rules file
                dataset_rules_file = rules_file  # Default
                
                # Check if there's a specific rules file for this dataset
                for config_name, config_data in datasets_config.items():
                    file_pattern = config_data.get('file_pattern', '')
                    dataset_cleaning_rules = config_data.get('cleaning_rules', '')
                    
                    if file_pattern and dataset_cleaning_rules:
                        import fnmatch
                        if fnmatch.fnmatch(csv_file.name, file_pattern):
                            dataset_rules_file = dataset_cleaning_rules
                            logger.info(f"Using dataset-specific rules for {dataset_name}: {dataset_rules_file}")
                            break
                
                try:
                    # Clean dataset
                    cleaned_df, cleaning_result = self.clean_dataset(
                        dataset_path=csv_file,
                        rules_file=dataset_rules_file,
                        max_iterations=max_iterations,
                        output_path=output_path
                    )
                    
                    overall_results['dataset_results'][dataset_name] = cleaning_result
                    overall_results['cleaned_datasets'] += 1
                    overall_results['total_records_modified'] += cleaning_result['records_modified']
                    overall_results['total_rules_applied'] += cleaning_result['rules_applied']
                    
                except Exception as e:
                    logger.error(f"Failed to clean {dataset_name}: {str(e)}")
                    overall_results['dataset_results'][dataset_name] = {
                        'status': 'error',
                        'error': str(e)
                    }
                    overall_results['failed_datasets'] += 1
                    overall_results['overall_success'] = False
            
            # Save overall results
            results_dir = self.project_root / "cleaning_results" / run_timestamp
            ensure_directory(results_dir)
            
            results_path = results_dir / "cleaning_summary.json"
            with open(results_path, 'w') as f:
                json.dump(overall_results, f, indent=2, default=str)
            
            logger.info(f"✅ Cleaning complete: {overall_results['cleaned_datasets']}/{overall_results['total_datasets']} datasets cleaned, "
                       f"{overall_results['total_records_modified']} total records modified")
            
            return overall_results
            
        except Exception as e:
            logger.error(f"Cleaning failed: {str(e)}")
            raise

def create_cleaning_engine(config_path: Optional[str] = None) -> DataCleaningEngine:
    """
    Factory function to create data cleaning engine
    
    Args:
        config_path: Optional path to configuration file
        
    Returns:
        Configured data cleaning engine
    """
    config = load_config(config_path)
    project_root = Path.cwd()
    return DataCleaningEngine(config, project_root)
