"""
ODK Central Integration Module
Handles authentication, form discovery, and data download from ODK Central

This module addresses common ODK export issues using the proper ODK Central CSV API:
- Group headers that create column name prefixes (meta/, __system/, etc.)
- Data misalignment caused by nested group structures
- Unnamed columns resulting from group organization
- Column header formatting inconsistencies

Key Solution:
- Uses ODK Central's CSV export API with groupPaths=false parameter
- This flattens group headers at the source (ODK server-side)
- Eliminates the need for complex client-side column processing
- Provides proper fallback methods for compatibility

Features:
- Primary: ODK Central CSV API with group flattening
- Fallback: OData method with manual processing  
- Final fallback: List submissions conversion
- Configurable data processing options
- Comprehensive error handling and logging
"""

import logging
import json
import tempfile
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
from pyodk import Client
from pyodk.errors import PyODKError

from .config import load_config
from .utils import save_run_metadata, create_run_timestamp, ensure_directory

logger = logging.getLogger(__name__)

class ODKCentralClient:
    """Client for interacting with ODK Central"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize ODK Central client
        
        Args:
            config: Configuration dictionary with ODK settings
        """
        self.config = config
        self.odk_config = config.get('odk', {})
        
        # Data processing options
        self.clean_column_headers = self.odk_config.get('clean_column_headers', True)
        self.remove_empty_columns = self.odk_config.get('remove_empty_columns', True)
        self.use_fallback_export = self.odk_config.get('use_fallback_export', True)
        self.flatten_group_headers = self.odk_config.get('flatten_group_headers', True)  # Use CSV API with groupPaths=false
        
        # Validate required configuration
        required_fields = ['base_url', 'username', 'password', 'project_id']
        for field in required_fields:
            if not self.odk_config.get(field):
                raise ValueError(f"Missing required ODK configuration: {field}")
        
        # Initialize pyODK client using temporary config file
        try:
            self.client = self._create_pyodk_client()
            logger.info(f"Initialized ODK Central client for {self.odk_config['base_url']}")
        except Exception as e:
            logger.error(f"Failed to initialize ODK Central client: {str(e)}")
            raise
    
    def _create_pyodk_client(self) -> Client:
        """
        Create pyODK client using temporary config file approach
        
        Returns:
            Configured pyODK Client instance
        """
        # Create temporary TOML config file
        config_content = f'''[central]
base_url = "{self.odk_config['base_url']}"
username = "{self.odk_config['username']}"
password = "{self.odk_config['password']}"
default_project_id = {self.odk_config['project_id']}
'''
        
        # Write to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write(config_content)
            config_path = f.name
        
        try:
            # Initialize client with config file
            client = Client(
                config_path=config_path,
                project_id=int(self.odk_config['project_id'])
            )
            return client
        finally:
            # Clean up temporary file
            try:
                os.unlink(config_path)
            except OSError:
                pass
    
    def test_connection(self) -> bool:
        """
        Test connection to ODK Central
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try to get project information
            project_id = int(self.odk_config['project_id'])
            project = self.client.projects.get(project_id)
            
            # Access project name using dot notation for pyODK objects
            project_name = getattr(project, 'name', f'Project {project_id}')
            logger.info(f"✅ Connection successful to project: {project_name}")
            return True
            
        except PyODKError as e:
            logger.error(f"❌ ODK Central connection failed: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error testing connection: {str(e)}")
            return False
    
    def discover_forms(self) -> List[Dict[str, Any]]:
        """
        Discover all forms in the project
        
        Returns:
            List of form metadata dictionaries
        """
        try:
            project_id = int(self.odk_config['project_id'])
            forms = self.client.forms.list(project_id=project_id)
            
            # Convert pyODK form objects to dictionaries
            form_list = []
            for form in forms:
                form_dict = {
                    'xmlFormId': getattr(form, 'xmlFormId', 'unknown'),
                    'name': getattr(form, 'name', 'Unnamed'),
                    'version': getattr(form, 'version', 'unknown'),
                    'state': getattr(form, 'state', 'unknown')
                }
                form_list.append(form_dict)
            
            logger.info(f"Discovered {len(form_list)} forms in project {project_id}")
            
            # Log form details
            for form in form_list:
                logger.info(f"  - {form['xmlFormId']}: {form['name']} "
                          f"(v{form['version']})")
            
            return form_list
            
        except PyODKError as e:
            logger.error(f"Failed to discover forms: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error discovering forms: {str(e)}")
            raise
    
    def get_form_submissions_count(self, form_id: str) -> int:
        """
        Get the number of submissions for a form
        
        Args:
            form_id: ODK form ID
            
        Returns:
            Number of submissions
        """
        try:
            project_id = int(self.odk_config['project_id'])
            submissions = self.client.submissions.list(
                project_id=project_id,
                form_id=form_id
            )
            return len(submissions)
            
        except PyODKError as e:
            logger.error(f"Failed to get submission count for {form_id}: {str(e)}")
            return 0
    
    def _process_odk_table_data(self, data: Dict[str, Any], form_id: str) -> Optional[pd.DataFrame]:
        """
        Process ODK table data to handle group headers and formatting issues
        
        Args:
            data: Raw table data from ODK Central
            form_id: Form ID for logging
            
        Returns:
            Processed DataFrame or None if no valid data
        """
        try:
            raw_data = data.get('value', [])
            if not raw_data:
                return None
            
            # Create initial DataFrame
            df = pd.DataFrame(raw_data)
            
            if df.empty:
                return None
            
            logger.info(f"Processing ODK table data for {form_id}: {len(df)} rows, {len(df.columns)} columns")
            
            # Handle column naming issues
            if self.clean_column_headers:
                df = self._fix_column_headers(df, form_id)
            
            # Remove completely empty columns that might result from group headers
            if self.remove_empty_columns:
                df = self._remove_empty_columns(df)
            
            # Clean up any remaining formatting issues
            df = self._clean_data_values(df)
            
            logger.info(f"Processed data for {form_id}: {len(df)} rows, {len(df.columns)} columns after cleanup")
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing ODK table data for {form_id}: {str(e)}")
            return None
    
    def _fix_column_headers(self, df: pd.DataFrame, form_id: str) -> pd.DataFrame:
        """
        Fix column header issues from ODK exports
        
        Args:
            df: Raw DataFrame
            form_id: Form ID for logging
            
        Returns:
            DataFrame with fixed column headers
        """
        try:
            # Check for unnamed columns and generate names
            new_columns = []
            unnamed_count = 0
            columns_to_drop = []
            
            for i, col in enumerate(df.columns):
                col_str = str(col).strip()
                
                # More comprehensive detection of problematic columns
                is_unnamed = (
                    pd.isna(col) or 
                    col_str == '' or 
                    col_str == 'nan' or
                    col_str.startswith('Unnamed') or
                    col_str.startswith('Column') or
                    # Check for numeric-only column names (often auto-generated)
                    (col_str.isdigit() and len(col_str) <= 3) or
                    # Check for generic patterns like 'field_1', 'var_1', etc.
                    bool(re.match(r'^(field|var|col)_?\d*$', col_str, re.IGNORECASE))
                )
                
                if is_unnamed:
                    # Check if this column has any meaningful data
                    non_null_count = df[col].notna().sum()
                    non_empty_count = 0
                    
                    if df[col].dtype == 'object':
                        non_empty_count = df[col].fillna('').str.strip().ne('').sum()
                    else:
                        non_empty_count = non_null_count
                    
                    # If column has very little data (less than 5% of rows), mark for removal
                    data_threshold = max(1, len(df) * 0.05)  # At least 5% of rows or 1 row
                    
                    if non_empty_count < data_threshold:
                        columns_to_drop.append(col)
                        logger.info(f"Marking sparse unnamed column at position {i+1} for removal: '{col_str}' "
                                  f"({non_empty_count}/{len(df)} non-empty values)")
                    else:
                        # Generate a meaningful name for unnamed columns with data
                        unnamed_count += 1
                        new_name = f"field_{i+1}_unnamed_{unnamed_count}"
                        new_columns.append(new_name)
                        logger.warning(f"Fixed unnamed column at position {i+1} → '{new_name}' in {form_id} "
                                     f"({non_empty_count} non-empty values)")
                else:
                    # Clean existing column names
                    clean_name = col_str
                    # Remove group prefixes that might cause issues
                    if '/' in clean_name:
                        clean_name = clean_name.split('/')[-1]  # Use last part after group separator
                    
                    # Clean up any remaining problematic characters
                    clean_name = re.sub(r'[^\w\-_]', '_', clean_name)
                    clean_name = clean_name.strip('_')
                    
                    if not clean_name:  # If cleaning resulted in empty string
                        clean_name = f"field_{i+1}"
                    
                    new_columns.append(clean_name)
            
            # Apply new column names
            df.columns = new_columns
            
            # Drop columns that were marked for removal
            if columns_to_drop:
                df = df.drop(columns=columns_to_drop)
                logger.info(f"Dropped {len(columns_to_drop)} sparse unnamed columns from {form_id}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error fixing column headers for {form_id}: {str(e)}")
            return df
    
    def _remove_empty_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove columns that are completely empty (often from group headers)
        
        Args:
            df: DataFrame to clean
            
        Returns:
            DataFrame with empty columns removed
        """
        try:
            initial_cols = len(df.columns)
            
            # Identify completely empty columns (all NaN)
            empty_cols = df.columns[df.isnull().all()].tolist()
            
            # Identify columns that are all empty strings
            string_empty_cols = []
            for col in df.columns:
                if df[col].dtype == 'object':
                    if df[col].fillna('').str.strip().eq('').all():
                        string_empty_cols.append(col)
            
            # Identify columns with very sparse data (less than 2% non-empty values)
            sparse_threshold = max(1, len(df) * 0.02)  # At least 2% of rows or 1 row
            sparse_cols = []
            
            for col in df.columns:
                if col not in empty_cols and col not in string_empty_cols:
                    if df[col].dtype == 'object':
                        non_empty_count = df[col].fillna('').str.strip().ne('').sum()
                    else:
                        non_empty_count = df[col].notna().sum()
                    
                    if non_empty_count < sparse_threshold:
                        sparse_cols.append(col)
            
            # Combine all columns to remove
            all_cols_to_remove = list(set(empty_cols + string_empty_cols + sparse_cols))
            
            if all_cols_to_remove:
                logger.info(f"Removing {len(all_cols_to_remove)} problematic columns:")
                if empty_cols:
                    logger.info(f"  - {len(empty_cols)} completely empty columns: {empty_cols}")
                if string_empty_cols:
                    logger.info(f"  - {len(string_empty_cols)} empty string columns: {string_empty_cols}")
                if sparse_cols:
                    logger.info(f"  - {len(sparse_cols)} sparse columns: {sparse_cols}")
                
                df = df.drop(columns=all_cols_to_remove)
                logger.info(f"Column count reduced from {initial_cols} to {len(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error removing empty columns: {str(e)}")
            return df
    
    def _clean_data_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean up data values in the DataFrame
        
        Args:
            df: DataFrame to clean
            
        Returns:
            DataFrame with cleaned values
        """
        try:
            # Convert object columns and clean up common ODK artifacts
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Remove leading/trailing whitespace
                    df[col] = df[col].astype(str).str.strip()
                    
                    # Replace 'nan' strings with actual NaN
                    df[col] = df[col].replace(['nan', 'None', ''], pd.NA)
            
            return df
            
        except Exception as e:
            logger.error(f"Error cleaning data values: {str(e)}")
            return df
    
    def _process_submissions_to_dataframe(self, submissions: List[Any], form_id: str) -> Optional[pd.DataFrame]:
        """
        Convert ODK submissions list to DataFrame (fallback method)
        
        Args:
            submissions: List of submission objects from ODK
            form_id: Form ID for logging
            
        Returns:
            Processed DataFrame or None
        """
        try:
            if not submissions:
                return None
            
            # Convert submissions to list of dictionaries
            submission_dicts = []
            for submission in submissions:
                if hasattr(submission, '__dict__'):
                    submission_dicts.append(vars(submission))
                elif isinstance(submission, dict):
                    submission_dicts.append(submission)
                else:
                    # Try to convert pyODK object to dict
                    try:
                        submission_dict = {}
                        for attr in dir(submission):
                            if not attr.startswith('_'):
                                try:
                                    value = getattr(submission, attr)
                                    if not callable(value):
                                        submission_dict[attr] = value
                                except:
                                    continue
                        submission_dicts.append(submission_dict)
                    except:
                        logger.warning(f"Could not convert submission object for {form_id}")
                        continue
            
            if not submission_dicts:
                return None
            
            # Create DataFrame
            df = pd.DataFrame(submission_dicts)
            
            if df.empty:
                return None
            
            logger.info(f"Converted {len(submissions)} submissions to DataFrame for {form_id}: {len(df.columns)} columns")
            
            # Apply same cleaning as table method
            if self.clean_column_headers:
                df = self._fix_column_headers(df, form_id)
            if self.remove_empty_columns:
                df = self._remove_empty_columns(df)
            df = self._clean_data_values(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Error converting submissions to DataFrame for {form_id}: {str(e)}")
            return None
    
    def download_form_data(
        self, 
        form_id: str, 
        output_path: Path,
        format: str = "csv"
    ) -> Tuple[Path, Dict[str, Any]]:
        """
        Download data for a specific form using ODK Central's CSV export with group flattening
        
        Args:
            form_id: ODK form ID
            output_path: Directory to save the downloaded data
            format: Download format (csv, json, xlsx)
            
        Returns:
            Tuple of (file_path, download_metadata)
        """
        try:
            project_id = int(self.odk_config['project_id'])
            
            logger.info(f"Downloading {format.upper()} data for form: {form_id}")
            
            # Ensure output directory exists
            ensure_directory(output_path)
            
            # Download submissions data
            if format.lower() == "csv":
                # Use ODK Central's CSV export API with group flattening
                try:
                    # Build the CSV export URL
                    csv_export_url = f"projects/{project_id}/forms/{form_id}/submissions.csv"
                    
                    logger.info(f"Using ODK Central CSV export API for {form_id} with group flattening enabled")
                    
                    # Make direct API call to get CSV with flattened group headers
                    response = self.client.session.response_or_error(
                        method="GET",
                        url=self.client.session.urlformat(csv_export_url),
                        params={
                            'groupPaths': 'false',  # This removes group prefixes (meta/ becomes instanceID)
                            'deletedFields': 'false',  # Don't include deleted fields
                            'splitSelectMultiples': 'false'  # Keep select multiples as single columns
                        },
                        logger=logger
                    )
                    
                    # Save CSV data directly to file
                    filename = f"{form_id}.csv"
                    file_path = output_path / filename
                    
                    # Write the CSV content to file
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(response.text)
                    
                    logger.info(f"✅ Successfully downloaded flattened CSV for {form_id}")
                    
                except Exception as csv_api_error:
                    logger.warning(f"ODK Central CSV API failed for {form_id}: {str(csv_api_error)}")
                    
                    if self.use_fallback_export:
                        logger.info(f"Falling back to OData method with manual processing for {form_id}")
                        
                        # Fallback to get_table method with manual processing
                        try:
                            data = self.client.submissions.get_table(
                                project_id=project_id,
                                form_id=form_id
                            )
                            
                            # Convert to DataFrame and save as CSV
                            if 'value' in data and data['value']:
                                # Process ODK table data to handle group headers and column formatting
                                processed_data = self._process_odk_table_data(data, form_id)
                                
                                if processed_data is not None and not processed_data.empty:
                                    # Save to CSV file
                                    filename = f"{form_id}.csv"
                                    file_path = output_path / filename
                                    
                                    processed_data.to_csv(file_path, index=False, encoding='utf-8')
                                    logger.info(f"✅ Fallback OData method successful for {form_id}")
                                else:
                                    logger.warning(f"No valid data found after processing for form: {form_id}")
                                    return None, None
                            else:
                                # No data available
                                logger.warning(f"No submissions found for form: {form_id}")
                                return None, None
                                
                        except Exception as odata_error:
                            logger.error(f"OData fallback also failed for {form_id}: {str(odata_error)}")
                            
                            # Final fallback: list submissions
                            try:
                                submissions = self.client.submissions.list(
                                    project_id=project_id,
                                    form_id=form_id
                                )
                                
                                if submissions:
                                    # Convert submissions to DataFrame
                                    processed_data = self._process_submissions_to_dataframe(submissions, form_id)
                                    
                                    if processed_data is not None and not processed_data.empty:
                                        filename = f"{form_id}.csv"
                                        file_path = output_path / filename
                                        processed_data.to_csv(file_path, index=False, encoding='utf-8')
                                        logger.info(f"✅ Final fallback method successful for {form_id}")
                                    else:
                                        logger.warning(f"No valid data from final fallback for form: {form_id}")
                                        return None, None
                                else:
                                    logger.warning(f"No submissions found for form: {form_id}")
                                    return None, None
                                    
                            except Exception as final_error:
                                logger.error(f"All export methods failed for {form_id}: {str(final_error)}")
                                raise
                    else:
                        logger.error(f"CSV export failed for {form_id} and fallback is disabled")
                        raise csv_api_error
                
            elif format.lower() == "json":
                data = self.client.submissions.list(
                    project_id=project_id,
                    form_id=form_id
                )
                
                # Save to JSON file
                filename = f"{form_id}.json"
                file_path = output_path / filename
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, default=str)
            
            else:
                raise ValueError(f"Unsupported download format: {format}")
            
            # Get submission count for metadata
            submission_count = self.get_form_submissions_count(form_id)
            
            # Create download metadata
            metadata = {
                "form_id": form_id,
                "format": format,
                "filename": filename,
                "file_path": str(file_path),
                "submission_count": submission_count,
                "download_timestamp": datetime.now().isoformat(),
                "file_size_bytes": file_path.stat().st_size if file_path.exists() else 0,
                "method_used": "csv_api_with_group_flattening" if format.lower() == "csv" else "standard"
            }
            
            logger.info(f"✅ Downloaded {submission_count} submissions for {form_id} "
                       f"({metadata['file_size_bytes']} bytes) using {metadata['method_used']}")
            
            return file_path, metadata
            
        except PyODKError as e:
            logger.error(f"Failed to download data for {form_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error downloading {form_id}: {str(e)}")
            raise
    
    def download_all_forms(
        self, 
        output_path: Path,
        format: str = "csv",
        forms_filter: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Download data for all forms in the project
        
        Args:
            output_path: Directory to save downloaded data
            format: Download format (csv, json, xlsx)
            forms_filter: Optional list of specific form IDs to download
            
        Returns:
            Dictionary with download results and metadata
        """
        try:
            run_timestamp = create_run_timestamp()
            
            # Create run-specific directory
            run_output_path = output_path / run_timestamp
            ensure_directory(run_output_path)
            
            # Discover forms
            all_forms = self.discover_forms()
            
            # Filter forms if specified
            if forms_filter:
                forms_to_download = [f for f in all_forms if f['xmlFormId'] in forms_filter]
                logger.info(f"Filtering to {len(forms_to_download)} specified forms")
            else:
                forms_to_download = all_forms
            
            # Download each form
            download_results = {}
            total_submissions = 0
            
            for form in forms_to_download:
                form_id = form['xmlFormId']
                
                try:
                    file_path, metadata = self.download_form_data(
                        form_id=form_id,
                        output_path=run_output_path,
                        format=format
                    )
                    
                    download_results[form_id] = {
                        "status": "success",
                        "file_path": str(file_path),
                        "metadata": metadata
                    }
                    
                    total_submissions += metadata['submission_count']
                    
                except Exception as e:
                    logger.error(f"Failed to download {form_id}: {str(e)}")
                    download_results[form_id] = {
                        "status": "error",
                        "error": str(e)
                    }
            
            # Create overall run metadata
            run_metadata = {
                "run_timestamp": run_timestamp,
                "odk_project_id": self.odk_config['project_id'],
                "odk_base_url": self.odk_config['base_url'],
                "download_format": format,
                "forms_requested": len(forms_to_download),
                "forms_successful": len([r for r in download_results.values() if r['status'] == 'success']),
                "forms_failed": len([r for r in download_results.values() if r['status'] == 'error']),
                "total_submissions": total_submissions,
                "download_results": download_results
            }
            
            # Save run metadata
            metadata_path = save_run_metadata(
                run_timestamp=run_timestamp,
                metadata=run_metadata,
                output_dir=run_output_path
            )
            
            logger.info(f"✅ Download complete: {run_metadata['forms_successful']}/{run_metadata['forms_requested']} forms, "
                       f"{total_submissions} total submissions")
            
            return run_metadata
            
        except Exception as e:
            logger.error(f"Failed to download all forms: {str(e)}")
            raise

def create_odk_client(config_path: Optional[str] = None) -> ODKCentralClient:
    """
    Factory function to create ODK Central client
    
    Args:
        config_path: Optional path to configuration file
        
    Returns:
        Configured ODK Central client
    """
    config = load_config(config_path)
    return ODKCentralClient(config)

def test_odk_connection(config_path: Optional[str] = None) -> bool:
    """
    Test ODK Central connection
    
    Args:
        config_path: Optional path to configuration file
        
    Returns:
        True if connection successful
    """
    try:
        client = create_odk_client(config_path)
        return client.test_connection()
    except Exception as e:
        logger.error(f"Failed to test ODK connection: {str(e)}")
        return False
