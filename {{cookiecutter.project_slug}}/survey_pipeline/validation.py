"""
Great Expectations Validation Engine
Handles data validation with expectation suites and failed row extraction
"""

import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import yaml

# Import Great Expectations components (v1.x compatible)
try:
    import great_expectations as gx
    from great_expectations.data_context import CloudDataContext, FileDataContext
    from great_expectations.exceptions import DataContextError
    GX_VERSION = "1.x"
except ImportError:
    try:
        from great_expectations import DataContext
        from great_expectations.exceptions import DataContextError
        GX_VERSION = "0.x"
    except ImportError as e:
        raise ImportError(f"Could not import Great Expectations: {e}")

from .config import load_config
from .utils import save_run_metadata, create_run_timestamp, ensure_directory

logger = logging.getLogger(__name__)

class ValidationEngine:
    """Great Expectations validation engine with failed row extraction"""
    
    def __init__(self, config: Dict[str, Any], project_root: Path):
        """
        Initialize validation engine
        
        Args:
            config: Configuration dictionary
            project_root: Project root directory
        """
        self.config = config
        self.project_root = project_root
        self.validation_config = config.get('validation', {})
        
        # Initialize Great Expectations data context
        self._initialize_data_context()
    
    def _initialize_data_context(self):
        """Initialize Great Expectations data context"""
        try:
            if GX_VERSION == "1.x":
                # For Great Expectations v1.x
                try:
                    # Try to get existing context
                    self.data_context = gx.get_context(project_root_dir=self.project_root)
                    logger.info("Using existing Great Expectations context (v1.x)")
                except Exception:
                    # Initialize new context if none exists
                    logger.info("Initializing new Great Expectations context (v1.x)")
                    self.data_context = gx.get_context(
                        project_root_dir=self.project_root,
                        context_root_dir=self.project_root
                    )
            else:
                # For Great Expectations v0.x (legacy)
                try:
                    self.data_context = DataContext(context_root_dir=self.project_root)
                    logger.info("Using existing Great Expectations context (v0.x)")
                except DataContextError:
                    logger.info("Initializing new Great Expectations context (v0.x)")
                    self.data_context = DataContext.create(self.project_root)
        except Exception as e:
            logger.warning(f"Could not initialize Great Expectations context: {e}")
            logger.info("Validation will run in standalone mode without GX context")
            self.data_context = None
    
    def load_expectation_suite(self, suite_name: str) -> Dict[str, Any]:
        """
        Load expectation suite from YAML file
        
        Args:
            suite_name: Name of the expectation suite
            
        Returns:
            Expectation suite dictionary
        """
        suite_path = self.project_root / "expectations" / f"{suite_name}.yml"
        
        if not suite_path.exists():
            raise FileNotFoundError(f"Expectation suite not found: {suite_path}")
        
        with open(suite_path, 'r') as f:
            suite_config = yaml.safe_load(f)
        
        logger.info(f"Loaded expectation suite: {suite_name}")
        return suite_config
    
    def validate_dataset(
        self, 
        dataset_path: Path, 
        suite_name: str,
        run_timestamp: str
    ) -> Tuple[Dict[str, Any], Optional[pd.DataFrame]]:
        """
        Validate a dataset against an expectation suite
        
        Args:
            dataset_path: Path to the dataset file
            suite_name: Name of the expectation suite to use
            run_timestamp: Timestamp for this validation run
            
        Returns:
            Tuple of (validation_results, failed_rows_df)
        """
        try:
            logger.info(f"Validating {dataset_path.name} with suite {suite_name}")
            
            # Load dataset
            if dataset_path.suffix.lower() == '.csv':
                df = pd.read_csv(dataset_path)
            elif dataset_path.suffix.lower() in ['.xlsx', '.xls']:
                df = pd.read_excel(dataset_path)
            else:
                raise ValueError(f"Unsupported file format: {dataset_path.suffix}")
            
            logger.info(f"Loaded dataset: {len(df)} rows, {len(df.columns)} columns")
            
            # Load expectation suite configuration
            suite_config = self.load_expectation_suite(suite_name)
            
            # Run validation
            validation_results = self._run_expectations(df, suite_config, dataset_path.stem)
            
            # Extract failed rows
            failed_rows_df = self._extract_failed_rows(df, validation_results)
            
            # Add admin columns to failed rows if configured
            if failed_rows_df is not None and not failed_rows_df.empty:
                failed_rows_df = self._add_admin_columns(failed_rows_df)
            
            # Save failed rows extract
            if failed_rows_df is not None and not failed_rows_df.empty:
                failed_rows_path = self._save_failed_rows(
                    failed_rows_df, 
                    dataset_path.stem, 
                    run_timestamp
                )
                validation_results['failed_rows_path'] = str(failed_rows_path)
                validation_results['failed_rows_count'] = len(failed_rows_df)
            else:
                validation_results['failed_rows_count'] = 0
            
            return validation_results, failed_rows_df
            
        except Exception as e:
            logger.error(f"Validation failed for {dataset_path.name}: {str(e)}")
            raise
    
    def _run_expectations(
        self, 
        df: pd.DataFrame, 
        suite_config: Dict[str, Any], 
        dataset_name: str
    ) -> Dict[str, Any]:
        """Run Great Expectations validation"""
        
        expectations = suite_config.get('expectations', [])
        results = {
            'dataset_name': dataset_name,
            'suite_name': suite_config.get('expectation_suite_name', 'unknown'),
            'total_expectations': len(expectations),
            'passed_expectations': 0,
            'failed_expectations': 0,
            'critical_failures': 0,
            'warning_failures': 0,
            'expectation_results': [],
            'overall_success': True
        }
        
        for expectation in expectations:
            expectation_type = expectation.get('expectation_type')
            kwargs = expectation.get('kwargs', {})
            meta = expectation.get('meta', {})
            severity = meta.get('severity', 'warning')
            
            try:
                # Run individual expectation
                result = self._run_single_expectation(df, expectation_type, kwargs)
                
                expectation_result = {
                    'expectation_type': expectation_type,
                    'kwargs': kwargs,
                    'severity': severity,
                    'success': result['success'],
                    'result': result,
                    'meta': meta
                }
                
                results['expectation_results'].append(expectation_result)
                
                if result['success']:
                    results['passed_expectations'] += 1
                else:
                    results['failed_expectations'] += 1
                    
                    if severity == 'critical':
                        results['critical_failures'] += 1
                        results['overall_success'] = False
                    elif severity in ['error', 'warning']:
                        results['warning_failures'] += 1
                
            except Exception as e:
                logger.error(f"Failed to run expectation {expectation_type}: {str(e)}")
                results['failed_expectations'] += 1
                results['overall_success'] = False
        
        # Calculate pass rate
        total = results['total_expectations']
        passed = results['passed_expectations']
        results['pass_rate'] = (passed / total * 100) if total > 0 else 0
        
        logger.info(f"Validation complete: {passed}/{total} expectations passed "
                   f"({results['pass_rate']:.1f}%)")
        
        return results
    
    def _run_single_expectation(
        self, 
        df: pd.DataFrame, 
        expectation_type: str, 
        kwargs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Run a single Great Expectations expectation"""
        
        if expectation_type == "expect_table_columns_to_match_ordered_list":
            expected_columns = kwargs.get('column_list', [])
            actual_columns = list(df.columns)
            success = actual_columns == expected_columns
            return {
                'success': success,
                'result': {
                    'observed_value': actual_columns,
                    'expected_value': expected_columns
                }
            }
        
        elif expectation_type == "expect_column_values_to_not_be_null":
            column = kwargs.get('column')
            mostly = kwargs.get('mostly', 1.0)
            
            if column not in df.columns:
                return {'success': False, 'result': {'error': f'Column {column} not found'}}
            
            non_null_count = df[column].notna().sum()
            total_count = len(df)
            null_rate = (total_count - non_null_count) / total_count if total_count > 0 else 0
            success = (1 - null_rate) >= mostly
            
            return {
                'success': success,
                'result': {
                    'observed_value': 1 - null_rate,
                    'expected_value': mostly,
                    'non_null_count': int(non_null_count),
                    'null_count': int(total_count - non_null_count)
                }
            }
        
        elif expectation_type == "expect_column_values_to_be_between":
            column = kwargs.get('column')
            min_value = kwargs.get('min_value')
            max_value = kwargs.get('max_value')
            
            if column not in df.columns:
                return {'success': False, 'result': {'error': f'Column {column} not found'}}
            
            # Only check non-null values
            valid_values = df[column].dropna()
            if len(valid_values) == 0:
                return {'success': True, 'result': {'observed_value': 'no_data'}}
            
            in_range = valid_values.between(min_value, max_value, inclusive='both')
            pass_rate = in_range.sum() / len(valid_values)
            success = pass_rate >= kwargs.get('mostly', 1.0)
            
            return {
                'success': success,
                'result': {
                    'observed_value': pass_rate,
                    'expected_range': [min_value, max_value],
                    'values_in_range': int(in_range.sum()),
                    'total_values': len(valid_values)
                }
            }
        
        elif expectation_type == "expect_column_values_to_be_in_set":
            column = kwargs.get('column')
            value_set = set(kwargs.get('value_set', []))
            
            if column not in df.columns:
                return {'success': False, 'result': {'error': f'Column {column} not found'}}
            
            valid_values = df[column].dropna()
            if len(valid_values) == 0:
                return {'success': True, 'result': {'observed_value': 'no_data'}}
            
            in_set = valid_values.isin(value_set)
            pass_rate = in_set.sum() / len(valid_values)
            success = pass_rate >= kwargs.get('mostly', 1.0)
            
            return {
                'success': success,
                'result': {
                    'observed_value': pass_rate,
                    'expected_set': list(value_set),
                    'values_in_set': int(in_set.sum()),
                    'total_values': len(valid_values),
                    'unexpected_values': list(valid_values[~in_set].unique())
                }
            }
        
        elif expectation_type == "expect_column_values_to_be_unique":
            column = kwargs.get('column')
            mostly = kwargs.get('mostly', 1.0)
            
            if column not in df.columns:
                return {'success': False, 'result': {'error': f'Column {column} not found'}}
            
            # Check for duplicates in the specified column
            total_count = len(df)
            unique_count = df[column].nunique()
            duplicate_count = total_count - unique_count
            unique_rate = unique_count / total_count if total_count > 0 else 1.0
            success = unique_rate >= mostly
            
            return {
                'success': success,
                'result': {
                    'observed_value': unique_rate,
                    'expected_value': mostly,
                    'unique_count': int(unique_count),
                    'total_count': int(total_count),
                    'duplicate_count': int(duplicate_count)
                }
            }
        
        elif expectation_type == "expect_compound_columns_to_be_unique":
            column_list = kwargs.get('column_list', [])
            
            # Check if all columns exist
            missing_columns = [col for col in column_list if col not in df.columns]
            if missing_columns:
                return {'success': False, 'result': {'error': f'Columns not found: {missing_columns}'}}
            
            # Check for duplicates
            duplicates = df.duplicated(subset=column_list)
            duplicate_count = duplicates.sum()
            success = duplicate_count == 0
            
            return {
                'success': success,
                'result': {
                    'duplicate_count': int(duplicate_count),
                    'total_rows': len(df),
                    'duplicate_rate': duplicate_count / len(df) if len(df) > 0 else 0
                }
            }
        
        else:
            logger.warning(f"Unsupported expectation type: {expectation_type}")
            return {'success': True, 'result': {'skipped': True}}
    
    def _extract_failed_rows(
        self, 
        df: pd.DataFrame, 
        validation_results: Dict[str, Any]
    ) -> Optional[pd.DataFrame]:
        """Extract rows that failed validation with detailed failure messages"""
        
        failed_row_mask = pd.Series([False] * len(df))
        row_failure_messages = []  # Track failure messages for each row
        
        # Initialize failure tracking for each row
        for i in range(len(df)):
            row_failure_messages.append([])
        
        # Process each failed expectation
        for expectation_result in validation_results['expectation_results']:
            if not expectation_result['success']:
                expectation_type = expectation_result['expectation_type']
                kwargs = expectation_result['kwargs']
                meta = expectation_result.get('meta', {})
                
                # Create mask for this expectation's failures
                mask = self._create_failure_mask(df, expectation_type, kwargs)
                if mask is not None:
                    failed_row_mask |= mask
                    
                    # Generate descriptive failure message
                    failure_message = self._generate_failure_message(
                        expectation_type, kwargs, meta
                    )
                    
                    # Add failure message to affected rows
                    for i, failed in enumerate(mask):
                        if failed:
                            row_failure_messages[i].append(failure_message)
        
        if failed_row_mask.any():
            failed_rows = df[failed_row_mask].copy()
            
            # Add validation failure information
            validation_messages = []
            validation_severity = []
            failure_count = []
            
            for i, row_idx in enumerate(failed_rows.index):
                original_idx = df.index.get_loc(row_idx)
                messages = row_failure_messages[original_idx]
                
                validation_messages.append(" | ".join(messages))
                
                # Determine overall severity for this row
                severity_levels = []
                for expectation_result in validation_results['expectation_results']:
                    if not expectation_result['success']:
                        expectation_type = expectation_result['expectation_type']
                        kwargs = expectation_result['kwargs']
                        meta = expectation_result.get('meta', {})
                        
                        mask = self._create_failure_mask(df, expectation_type, kwargs)
                        if mask is not None and mask.iloc[original_idx]:
                            severity_levels.append(meta.get('severity', 'warning'))
                
                # Determine highest severity level
                if 'critical' in severity_levels:
                    validation_severity.append('critical')
                elif 'error' in severity_levels:
                    validation_severity.append('error')
                else:
                    validation_severity.append('warning')
                
                failure_count.append(len(messages))
            
            # Add validation columns to the failed rows dataframe
            failed_rows['validation_failures'] = validation_messages
            failed_rows['validation_severity'] = validation_severity
            failed_rows['failure_count'] = failure_count
            
            logger.info(f"Extracted {len(failed_rows)} failed rows")
            return failed_rows
        
        return None
    
    def _create_failure_mask(
        self, 
        df: pd.DataFrame, 
        expectation_type: str, 
        kwargs: Dict[str, Any]
    ) -> Optional[pd.Series]:
        """Create a boolean mask for rows that failed this expectation"""
        
        if expectation_type == "expect_column_values_to_not_be_null":
            column = kwargs.get('column')
            if column in df.columns:
                return df[column].isna()
        
        elif expectation_type == "expect_column_values_to_be_between":
            column = kwargs.get('column')
            min_value = kwargs.get('min_value')
            max_value = kwargs.get('max_value')
            
            if column in df.columns:
                return ~df[column].between(min_value, max_value, inclusive='both')
        
        elif expectation_type == "expect_column_values_to_be_in_set":
            column = kwargs.get('column')
            value_set = set(kwargs.get('value_set', []))
            
            if column in df.columns:
                return ~df[column].isin(value_set)
        
        elif expectation_type == "expect_column_values_to_be_unique":
            column = kwargs.get('column')
            
            if column in df.columns:
                # Mark duplicated values (keep=False marks ALL duplicates)
                return df.duplicated(subset=[column], keep=False)
        
        elif expectation_type == "expect_compound_columns_to_be_unique":
            column_list = kwargs.get('column_list', [])
            missing_columns = [col for col in column_list if col not in df.columns]
            
            if not missing_columns:
                return df.duplicated(subset=column_list, keep=False)
        
        return None
    
    def _generate_failure_message(
        self, 
        expectation_type: str, 
        kwargs: Dict[str, Any], 
        meta: Dict[str, Any]
    ) -> str:
        """Generate human-readable failure message for an expectation"""
        
        # Check if custom description is provided in meta
        if meta.get('description'):
            return meta['description']
        
        # Generate default messages based on expectation type
        if expectation_type == "expect_column_values_to_not_be_null":
            column = kwargs.get('column', 'unknown')
            return f"Missing required value in '{column}'"
        
        elif expectation_type == "expect_column_values_to_be_between":
            column = kwargs.get('column', 'unknown')
            min_val = kwargs.get('min_value', 'unknown')
            max_val = kwargs.get('max_value', 'unknown')
            return f"Value in '{column}' must be between {min_val} and {max_val}"
        
        elif expectation_type == "expect_column_values_to_be_in_set":
            column = kwargs.get('column', 'unknown')
            value_set = kwargs.get('value_set', [])
            if len(value_set) <= 5:
                values_str = ", ".join(map(str, value_set))
                return f"Value in '{column}' must be one of: {values_str}"
            else:
                return f"Value in '{column}' is not from allowed list ({len(value_set)} options)"
        
        elif expectation_type == "expect_column_values_to_be_unique":
            column = kwargs.get('column', 'unknown')
            return f"Duplicate value found in '{column}' (must be unique)"
        
        elif expectation_type == "expect_compound_columns_to_be_unique":
            column_list = kwargs.get('column_list', [])
            columns_str = ", ".join(column_list)
            return f"Duplicate combination found in columns: {columns_str}"
        
        elif expectation_type == "expect_table_columns_to_match_ordered_list":
            return "Column structure does not match expected format"
        
        else:
            return f"Failed validation rule: {expectation_type}"
    
    def _add_admin_columns(self, failed_rows_df: pd.DataFrame) -> pd.DataFrame:
        """Add administrative columns to failed rows extract"""
        
        admin_columns = self.config.get('admin_columns', [])
        
        # Add timestamp
        failed_rows_df['validation_timestamp'] = datetime.now().isoformat()
        
        # Add any missing admin columns with placeholder values
        for col in admin_columns:
            if col not in failed_rows_df.columns:
                failed_rows_df[col] = 'NOT_AVAILABLE'
        
        # Define validation-specific columns that should come first
        validation_cols = ['validation_failures', 'validation_severity', 'failure_count']
        existing_validation_cols = [col for col in validation_cols if col in failed_rows_df.columns]
        
        # Reorder columns: validation columns first, then admin columns, then data columns
        all_admin_cols = ['validation_timestamp'] + admin_columns
        existing_admin_cols = [col for col in all_admin_cols if col in failed_rows_df.columns]
        
        # Get remaining data columns (excluding validation and admin columns)
        excluded_cols = existing_validation_cols + existing_admin_cols
        other_cols = [col for col in failed_rows_df.columns if col not in excluded_cols]
        
        # Final column order: validation info, admin info, then original data
        final_column_order = existing_validation_cols + existing_admin_cols + other_cols
        
        return failed_rows_df[final_column_order]
    
    def _save_failed_rows(
        self, 
        failed_rows_df: pd.DataFrame, 
        dataset_name: str, 
        run_timestamp: str
    ) -> Path:
        """Save failed rows extract to file"""
        
        # Create failed directory for this run
        failed_dir = self.project_root / "staging" / "failed" / run_timestamp
        ensure_directory(failed_dir)
        
        # Save failed rows
        failed_rows_path = failed_dir / f"failed_rows_{dataset_name}.csv"
        failed_rows_df.to_csv(failed_rows_path, index=False)
        
        logger.info(f"Saved {len(failed_rows_df)} failed rows to {failed_rows_path}")
        return failed_rows_path
    
    def validate_all_datasets(self, run_timestamp: str) -> Dict[str, Any]:
        """
        Validate all datasets in staging area
        
        Args:
            run_timestamp: Timestamp for this validation run
            
        Returns:
            Overall validation results
        """
        try:
            staging_path = self.project_root / "staging" / "raw"
            datasets_config = self.config.get('datasets', {})
            
            # Find all CSV files in staging
            csv_files = list(staging_path.glob("*.csv"))
            logger.info(f"Found {len(csv_files)} datasets to validate")
            
            overall_results = {
                'run_timestamp': run_timestamp,
                'total_datasets': len(csv_files),
                'validated_datasets': 0,
                'passed_datasets': 0,
                'failed_datasets': 0,
                'critical_failures': 0,
                'dataset_results': {},
                'overall_pass_rate': 0.0,
                'overall_success': True
            }
            
            total_pass_rate = 0.0
            
            for csv_file in csv_files:
                dataset_name = csv_file.stem
                
                # Find matching suite configuration
                suite_name = None
                for config_name, config_data in datasets_config.items():
                    file_pattern = config_data.get('file_pattern', '')
                    validation_suite = config_data.get('validation_suite', '')
                    
                    if file_pattern and validation_suite:
                        import fnmatch
                        if fnmatch.fnmatch(csv_file.name, file_pattern):
                            suite_name = validation_suite
                            break
                
                if not suite_name:
                    logger.warning(f"No validation suite configured for {dataset_name}")
                    continue
                
                try:
                    # Validate dataset
                    validation_result, failed_rows = self.validate_dataset(
                        csv_file, suite_name, run_timestamp
                    )
                    
                    overall_results['dataset_results'][dataset_name] = validation_result
                    overall_results['validated_datasets'] += 1
                    
                    # Track pass/fail
                    if validation_result['overall_success']:
                        overall_results['passed_datasets'] += 1
                    else:
                        overall_results['failed_datasets'] += 1
                        
                        if validation_result['critical_failures'] > 0:
                            overall_results['critical_failures'] += 1
                            overall_results['overall_success'] = False
                    
                    total_pass_rate += validation_result['pass_rate']
                    
                except Exception as e:
                    logger.error(f"Failed to validate {dataset_name}: {str(e)}")
                    overall_results['failed_datasets'] += 1
                    overall_results['overall_success'] = False
            
            # Calculate overall pass rate
            if overall_results['validated_datasets'] > 0:
                overall_results['overall_pass_rate'] = total_pass_rate / overall_results['validated_datasets']
            
            # Check against minimum threshold
            min_pass_rate = self.validation_config.get('minimum_pass_rate', 85)
            if overall_results['overall_pass_rate'] < min_pass_rate:
                overall_results['overall_success'] = False
            
            # Save validation results
            results_dir = self.project_root / "validation_results" / run_timestamp
            ensure_directory(results_dir)
            
            results_path = results_dir / "validation_summary.json"
            with open(results_path, 'w') as f:
                json.dump(overall_results, f, indent=2, default=str)
            
            logger.info(f"✅ Validation complete: {overall_results['passed_datasets']}/{overall_results['validated_datasets']} datasets passed "
                       f"({overall_results['overall_pass_rate']:.1f}% overall)")
            
            return overall_results
            
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            raise

def create_validation_engine(config_path: Optional[str] = None) -> ValidationEngine:
    """
    Factory function to create validation engine
    
    Args:
        config_path: Optional path to configuration file
        
    Returns:
        Configured validation engine
    """
    config = load_config(config_path)
    project_root = Path.cwd()
    return ValidationEngine(config, project_root)
