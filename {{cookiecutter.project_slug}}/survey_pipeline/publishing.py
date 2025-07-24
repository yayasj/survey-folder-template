"""
Data Publishing Engine for Survey Pipeline
Handles atomic publishing of cleaned data to stable directory
"""

import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

from .utils import create_run_timestamp, backup_directory, atomic_directory_swap
from .config import load_config

class PublishingEngine:
    """
    Engine for atomically publishing cleaned data to stable directory
    """
    
    def __init__(self, config: Dict[str, Any], project_root: Path):
        """Initialize publishing engine"""
        self.config = config
        self.project_root = project_root
        self.logger = logging.getLogger(__name__)
        
        # Configuration
        self.publish_config = config.get('publish', {})
        self.stable_directory = self.publish_config.get('stable_directory', 'cleaned_stable')
        self.backup_previous = self.publish_config.get('backup_previous', True)
        
        # Paths
        self.staging_cleaned = project_root / "staging" / "cleaned"
        self.stable_path = project_root / self.stable_directory
        
    def validate_staging_data(self) -> Dict[str, Any]:
        """
        Validate that staging data is ready for publication
        
        Returns:
            Dict with validation results
        """
        validation_results = {
            'valid': True,
            'issues': [],
            'datasets_found': [],
            'total_records': 0
        }
        
        # Check if staging directory exists and has data
        if not self.staging_cleaned.exists():
            validation_results['valid'] = False
            validation_results['issues'].append("No staging/cleaned directory found")
            return validation_results
        
        # Look for CSV files in staging/cleaned/ and its subdirectories
        csv_files = []
        
        # First, look for CSV files directly in staging/cleaned/
        direct_csv_files = list(self.staging_cleaned.glob("*.csv"))
        csv_files.extend(direct_csv_files)
        
        # If no direct CSV files found, look in timestamped subdirectories
        if not direct_csv_files:
            for subdir in self.staging_cleaned.iterdir():
                if subdir.is_dir():
                    subdir_csv_files = list(subdir.glob("*.csv"))
                    csv_files.extend(subdir_csv_files)
        
        if not csv_files:
            validation_results['valid'] = False
            validation_results['issues'].append("No CSV files found in staging/cleaned or its subdirectories")
            return validation_results
        
        # Validate each dataset
        for csv_file in csv_files:
            try:
                import pandas as pd
                df = pd.read_csv(csv_file)
                
                dataset_info = {
                    'file': csv_file.name,
                    'path': str(csv_file),
                    'records': len(df),
                    'columns': len(df.columns),
                    'last_modified': datetime.fromtimestamp(csv_file.stat().st_mtime)
                }
                
                validation_results['datasets_found'].append(dataset_info)
                validation_results['total_records'] += len(df)
                
                # Check for empty datasets
                if len(df) == 0:
                    validation_results['issues'].append(f"Dataset {csv_file.name} is empty")
                
                self.logger.info(f"Found dataset: {csv_file.name} ({len(df)} records)")
                
            except Exception as e:
                validation_results['valid'] = False
                validation_results['issues'].append(f"Error reading {csv_file.name}: {str(e)}")
        
        # Check for minimum data requirements
        if validation_results['total_records'] == 0:
            validation_results['valid'] = False
            validation_results['issues'].append("No data records found across all datasets")
        
        return validation_results
    
    def create_publication_metadata(self, run_timestamp: str, validation_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create metadata for the publication
        
        Args:
            run_timestamp: Timestamp of the publication run
            validation_results: Results from staging validation
            
        Returns:
            Publication metadata
        """
        metadata = {
            'publication_timestamp': run_timestamp,
            'publication_date': datetime.now().isoformat(),
            'datasets_published': validation_results['datasets_found'],
            'total_records_published': validation_results['total_records'],
            'source_directory': str(self.staging_cleaned),
            'target_directory': str(self.stable_path),
            'backup_created': self.backup_previous,
            'publisher': 'survey-pipeline-automation',
            'config_version': self.config.get('version', '1.0.0')
        }
        
        return metadata
    
    def create_backup(self, run_timestamp: str) -> Optional[Path]:
        """
        Create backup of current stable data
        
        Args:
            run_timestamp: Timestamp for backup naming
            
        Returns:
            Path to backup directory if created, None otherwise
        """
        if not self.backup_previous:
            self.logger.info("Backup disabled in configuration")
            return None
        
        if not self.stable_path.exists() or not any(self.stable_path.iterdir()):
            self.logger.info("No existing stable data to backup")
            return None
        
        backup_name = f"stable_backup_{run_timestamp}"
        backup_path = backup_directory(self.stable_path, backup_name)
        
        if backup_path:
            self.logger.info(f"Created backup: {backup_path}")
            return backup_path
        else:
            self.logger.warning("Failed to create backup")
            return None
    
    def publish_data(self, run_timestamp: str, force: bool = False) -> Dict[str, Any]:
        """
        Atomically publish cleaned data to stable directory
        
        Args:
            run_timestamp: Timestamp of the publication run
            force: Skip validation checks if True
            
        Returns:
            Publication results
        """
        self.logger.info(f"Starting data publication for run: {run_timestamp}")
        
        # Step 1: Validate staging data
        if not force:
            validation_results = self.validate_staging_data()
            if not validation_results['valid']:
                return {
                    'success': False,
                    'error': 'Staging data validation failed',
                    'issues': validation_results['issues'],
                    'run_timestamp': run_timestamp
                }
        else:
            self.logger.warning("Force mode enabled - skipping validation")
            validation_results = {'datasets_found': [], 'total_records': 0}
        
        # Step 2: Create backup
        backup_path = self.create_backup(run_timestamp)
        
        # Step 3: Create publication metadata
        metadata = self.create_publication_metadata(run_timestamp, validation_results)
        
        try:
            # Step 4: Atomic directory swap
            # First, we need to create a consolidated staging directory if data is in subdirectories
            consolidated_staging = self._prepare_staging_for_publication(validation_results)
            
            success = atomic_directory_swap(
                source=consolidated_staging,
                target=self.stable_path,
                backup=self.backup_previous
            )
            
            if success:
                # Step 5: Save publication metadata
                metadata_file = self.stable_path / f"_publication_metadata_{run_timestamp}.json"
                import json
                with open(metadata_file, 'w') as f:
                    json.dump(metadata, f, indent=2, default=str)
                
                self.logger.info("✅ Data published successfully to stable directory")
                
                # Step 6: Clean up staging data
                self._cleanup_staging(run_timestamp, consolidated_staging)
                
                return {
                    'success': True,
                    'metadata': metadata,
                    'backup_path': str(backup_path) if backup_path else None,
                    'datasets_published': len(validation_results['datasets_found']),
                    'total_records': validation_results['total_records'],
                    'run_timestamp': run_timestamp
                }
            else:
                return {
                    'success': False,
                    'error': 'Atomic directory swap failed',
                    'run_timestamp': run_timestamp
                }
                
        except Exception as e:
            self.logger.error(f"Publication failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'run_timestamp': run_timestamp
            }
    
    def _prepare_staging_for_publication(self, validation_results: Dict[str, Any]) -> Path:
        """
        Prepare staging data for publication by consolidating files if needed
        
        Args:
            validation_results: Results from staging validation
            
        Returns:
            Path to consolidated staging directory ready for publication
        """
        # Check if all datasets are directly in staging/cleaned
        datasets = validation_results['datasets_found']
        all_direct = all(Path(dataset['path']).parent == self.staging_cleaned for dataset in datasets)
        
        if all_direct:
            # Files are already in staging/cleaned, can use directly
            return self.staging_cleaned
        
        # Need to consolidate files from subdirectories
        consolidated_dir = self.project_root / "staging" / "cleaned_consolidated"
        consolidated_dir.mkdir(exist_ok=True)
        
        # Clear any existing files
        for existing_file in consolidated_dir.glob("*"):
            if existing_file.is_file():
                existing_file.unlink()
        
        # Copy all CSV files to consolidated directory
        import shutil
        for dataset in datasets:
            source_path = Path(dataset['path'])
            target_path = consolidated_dir / dataset['file']
            shutil.copy2(source_path, target_path)
            self.logger.info(f"Consolidated {dataset['file']} for publication")
        
        return consolidated_dir
    
    def _cleanup_staging(self, run_timestamp: str, source_dir: Optional[Path] = None):
        """Clean up staging directory after successful publication"""
        try:
            # Archive staging data instead of deleting
            archive_dir = self.project_root / "staging" / "published_archive" / run_timestamp
            archive_dir.parent.mkdir(parents=True, exist_ok=True)
            
            # Use the source directory that was actually published
            staging_to_archive = source_dir if source_dir else self.staging_cleaned
            
            if staging_to_archive.exists():
                import shutil
                
                if source_dir and source_dir.name == "cleaned_consolidated":
                    # If we used a consolidated directory, archive the original timestamped directories
                    for subdir in self.staging_cleaned.iterdir():
                        if subdir.is_dir():
                            target_archive = archive_dir / subdir.name
                            shutil.move(str(subdir), str(target_archive))
                            self.logger.info(f"Archived staging data to: {target_archive}")
                    
                    # Clean up the consolidated directory
                    shutil.rmtree(source_dir)
                else:
                    # Archive the staging directory directly
                    shutil.move(str(staging_to_archive), str(archive_dir))
                    self.logger.info(f"Archived staging data to: {archive_dir}")
            
        except Exception as e:
            self.logger.warning(f"Failed to archive staging data: {str(e)}")
    
    def rollback_publication(self, backup_timestamp: str) -> Dict[str, Any]:
        """
        Rollback to a previous publication backup
        
        Args:
            backup_timestamp: Timestamp of the backup to restore
            
        Returns:
            Rollback results
        """
        self.logger.info(f"Starting rollback to backup: {backup_timestamp}")
        
        # Find backup directory
        backup_pattern = f"stable_backup_{backup_timestamp}"
        backup_candidates = list(self.project_root.glob(f"**/{backup_pattern}"))
        
        if not backup_candidates:
            return {
                'success': False,
                'error': f'No backup found for timestamp: {backup_timestamp}'
            }
        
        backup_path = backup_candidates[0]
        
        try:
            # Create current backup before rollback
            current_timestamp = create_run_timestamp()
            current_backup = self.create_backup(f"pre_rollback_{current_timestamp}")
            
            # Atomic swap to restore backup
            success = atomic_directory_swap(
                source=backup_path,
                target=self.stable_path,
                backup=True
            )
            
            if success:
                self.logger.info(f"✅ Successfully rolled back to: {backup_timestamp}")
                return {
                    'success': True,
                    'restored_from': str(backup_path),
                    'current_backup': str(current_backup) if current_backup else None,
                    'rollback_timestamp': current_timestamp
                }
            else:
                return {
                    'success': False,
                    'error': 'Atomic rollback operation failed'
                }
                
        except Exception as e:
            self.logger.error(f"Rollback failed: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def list_publications(self) -> List[Dict[str, Any]]:
        """
        List recent publications and their metadata
        
        Returns:
            List of publication records
        """
        publications = []
        
        if not self.stable_path.exists():
            return publications
        
        # Find metadata files
        metadata_files = list(self.stable_path.glob("_publication_metadata_*.json"))
        metadata_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        for metadata_file in metadata_files:
            try:
                import json
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                publications.append(metadata)
            except Exception as e:
                self.logger.warning(f"Error reading metadata file {metadata_file}: {str(e)}")
        
        return publications
    
    def get_publication_status(self) -> Dict[str, Any]:
        """
        Get current publication status and statistics
        
        Returns:
            Publication status information
        """
        status = {
            'stable_directory_exists': self.stable_path.exists(),
            'stable_directory_path': str(self.stable_path),
            'staging_ready': self.staging_cleaned.exists(),
            'staging_path': str(self.staging_cleaned),
            'last_publication': None,
            'current_datasets': [],
            'total_records': 0
        }
        
        # Get last publication info
        publications = self.list_publications()
        if publications:
            status['last_publication'] = publications[0]
        
        # Get current stable data info
        if self.stable_path.exists():
            csv_files = list(self.stable_path.glob("*.csv"))
            for csv_file in csv_files:
                try:
                    import pandas as pd
                    df = pd.read_csv(csv_file)
                    dataset_info = {
                        'file': csv_file.name,
                        'records': len(df),
                        'columns': len(df.columns),
                        'last_modified': datetime.fromtimestamp(csv_file.stat().st_mtime).isoformat()
                    }
                    status['current_datasets'].append(dataset_info)
                    status['total_records'] += len(df)
                except Exception as e:
                    self.logger.warning(f"Error reading dataset {csv_file}: {str(e)}")
        
        return status

def create_publishing_engine(config_path: Optional[str] = None, project_root: Optional[Path] = None) -> PublishingEngine:
    """
    Factory function to create a publishing engine
    
    Args:
        config_path: Path to configuration file
        project_root: Project root directory
        
    Returns:
        Configured PublishingEngine instance
    """
    config = load_config(config_path)
    
    if project_root is None:
        project_root = Path.cwd()
    
    return PublishingEngine(config, project_root)
