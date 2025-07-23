"""
Utility functions for survey pipeline
"""

import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import sys

def get_project_root() -> Path:
    """Get the project root directory"""
    return Path(__file__).parent.parent

def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    Set up logging configuration
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional log file path
        format_string: Optional custom format string
        
    Returns:
        Configured logger
    """
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=format_string,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Add file handler if specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(logging.Formatter(format_string))
        logging.getLogger().addHandler(file_handler)
    
    return logging.getLogger(__name__)

def save_run_metadata(
    run_timestamp: str,
    metadata: Dict[str, Any],
    output_dir: Path
) -> Path:
    """
    Save run metadata to JSON file
    
    Args:
        run_timestamp: Timestamp for this run
        metadata: Metadata dictionary
        output_dir: Directory to save metadata
        
    Returns:
        Path to saved metadata file
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Add timestamp and pipeline info
    metadata.update({
        "run_timestamp": run_timestamp,
        "pipeline_version": "1.0.0",
        "python_version": sys.version,
        "created_at": datetime.now().isoformat()
    })
    
    metadata_path = output_dir / f"run_metadata_{run_timestamp}.json"
    
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
    
    return metadata_path

def create_run_timestamp() -> str:
    """Create a standardized timestamp for pipeline runs"""
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def ensure_directory(path: Path) -> Path:
    """
    Ensure directory exists, create if necessary
    
    Args:
        path: Directory path
        
    Returns:
        Path object
    """
    path.mkdir(parents=True, exist_ok=True)
    return path

def backup_directory(source: Path, backup_name: str) -> Optional[Path]:
    """
    Create a backup of a directory
    
    Args:
        source: Source directory to backup
        backup_name: Name for backup directory
        
    Returns:
        Path to backup directory or None if source doesn't exist
    """
    if not source.exists():
        return None
    
    import shutil
    
    backup_path = source.parent / backup_name
    
    if backup_path.exists():
        shutil.rmtree(backup_path)
    
    shutil.copytree(source, backup_path)
    return backup_path

def atomic_directory_swap(source: Path, target: Path, backup: bool = True) -> bool:
    """
    Atomically swap directories
    
    Args:
        source: Source directory
        target: Target directory  
        backup: Whether to create backup of target
        
    Returns:
        True if successful
    """
    if not source.exists():
        raise FileNotFoundError(f"Source directory does not exist: {source}")
    
    import shutil
    
    # Create backup if requested and target exists
    if backup and target.exists():
        backup_name = f"{target.name}_backup_{create_run_timestamp()}"
        backup_directory(target, backup_name)
    
    # Create temporary directory for atomic swap
    temp_path = target.parent / f"{target.name}_temp_{create_run_timestamp()}"
    
    try:
        # Copy source to temp location
        shutil.copytree(source, temp_path)
        
        # Remove target if it exists
        if target.exists():
            shutil.rmtree(target)
        
        # Rename temp to target (atomic operation)
        temp_path.rename(target)
        
        return True
        
    except Exception as e:
        # Clean up temp directory if it exists
        if temp_path.exists():
            shutil.rmtree(temp_path)
        raise e

def send_notification(
    subject: str,
    message: str,
    config: Dict[str, Any],
    notification_type: str = "email"
) -> bool:
    """
    Send notification via email or Slack
    
    Args:
        subject: Notification subject
        message: Notification message
        config: Configuration with notification settings
        notification_type: Type of notification (email, slack)
        
    Returns:
        True if notification sent successfully
    """
    notifications_config = config.get('notifications', {})
    
    if notification_type == "email":
        return _send_email_notification(subject, message, notifications_config)
    elif notification_type == "slack":
        return _send_slack_notification(subject, message, notifications_config)
    else:
        logging.warning(f"Unsupported notification type: {notification_type}")
        return False

def _send_email_notification(
    subject: str,
    message: str,
    config: Dict[str, Any]
) -> bool:
    """Send email notification"""
    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        smtp_server = config.get('smtp_server')
        smtp_port = config.get('smtp_port', 587)
        username = config.get('smtp_username')
        password = config.get('smtp_password')
        recipients = config.get('recipients', [])
        
        if not all([smtp_server, username, password, recipients]):
            logging.warning("Email configuration incomplete, skipping notification")
            return False
        
        # Create message
        msg = MIMEMultipart()
        msg['From'] = username
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = subject
        msg.attach(MIMEText(message, 'plain'))
        
        # Send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(username, password)
            server.send_message(msg)
        
        logging.info(f"Email notification sent to {len(recipients)} recipients")
        return True
        
    except Exception as e:
        logging.error(f"Failed to send email notification: {str(e)}")
        return False

def _send_slack_notification(
    subject: str,
    message: str,
    config: Dict[str, Any]
) -> bool:
    """Send Slack notification"""
    try:
        import requests
        
        webhook_url = config.get('slack_webhook')
        
        if not webhook_url:
            logging.warning("Slack webhook URL not configured, skipping notification")
            return False
        
        payload = {
            "text": f"*{subject}*\n{message}"
        }
        
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        
        logging.info("Slack notification sent successfully")
        return True
        
    except Exception as e:
        logging.error(f"Failed to send Slack notification: {str(e)}")
        return False
