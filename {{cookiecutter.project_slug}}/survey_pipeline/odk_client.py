"""
ODK Central Integration Module
Handles authentication, form discovery, and data download from ODK Central
"""

import logging
import json
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
        
        # Validate required configuration
        required_fields = ['base_url', 'username', 'password', 'project_id']
        for field in required_fields:
            if not self.odk_config.get(field):
                raise ValueError(f"Missing required ODK configuration: {field}")
        
        # Initialize pyODK client
        try:
            self.client = Client(
                base_url=self.odk_config['base_url'],
                username=self.odk_config['username'],
                password=self.odk_config['password']
            )
            logger.info(f"Initialized ODK Central client for {self.odk_config['base_url']}")
        except Exception as e:
            logger.error(f"Failed to initialize ODK Central client: {str(e)}")
            raise
    
    def test_connection(self) -> bool:
        """
        Test connection to ODK Central
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try to get project information
            project_id = self.odk_config['project_id']
            project = self.client.projects.get(project_id)
            
            logger.info(f"✅ Connection successful to project: {project['name']}")
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
            project_id = self.odk_config['project_id']
            forms = self.client.forms.list(project_id=project_id)
            
            logger.info(f"Discovered {len(forms)} forms in project {project_id}")
            
            # Log form details
            for form in forms:
                logger.info(f"  - {form['xmlFormId']}: {form.get('name', 'Unnamed')} "
                          f"(v{form.get('version', 'unknown')})")
            
            return forms
            
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
            project_id = self.odk_config['project_id']
            submissions = self.client.submissions.list(
                project_id=project_id,
                form_id=form_id
            )
            return len(submissions)
            
        except PyODKError as e:
            logger.error(f"Failed to get submission count for {form_id}: {str(e)}")
            return 0
    
    def download_form_data(
        self, 
        form_id: str, 
        output_path: Path,
        format: str = "csv"
    ) -> Tuple[Path, Dict[str, Any]]:
        """
        Download data for a specific form
        
        Args:
            form_id: ODK form ID
            output_path: Directory to save the downloaded data
            format: Download format (csv, json, xlsx)
            
        Returns:
            Tuple of (file_path, download_metadata)
        """
        try:
            project_id = self.odk_config['project_id']
            
            logger.info(f"Downloading {format.upper()} data for form: {form_id}")
            
            # Ensure output directory exists
            ensure_directory(output_path)
            
            # Download submissions data
            if format.lower() == "csv":
                data = self.client.submissions.get_table(
                    project_id=project_id,
                    form_id=form_id,
                    format="csv"
                )
                
                # Save to CSV file
                filename = f"{form_id}.csv"
                file_path = output_path / filename
                
                # Write CSV data
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(data)
                
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
                "file_size_bytes": file_path.stat().st_size if file_path.exists() else 0
            }
            
            logger.info(f"✅ Downloaded {submission_count} submissions for {form_id} "
                       f"({metadata['file_size_bytes']} bytes)")
            
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
