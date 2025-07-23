"""
Configuration management for survey pipeline
"""

import yaml
import os
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

def get_project_root() -> Path:
    """Get the project root directory"""
    return Path(__file__).parent.parent

def load_config(config_path: str = None) -> Dict[str, Any]:
    """
    Load configuration from config.yml and environment variables
    
    Args:
        config_path: Optional path to config file
        
    Returns:
        Configuration dictionary
    """
    # Load environment variables
    load_dotenv()
    
    # Determine config file path
    if config_path is None:
        config_path = get_project_root() / "config.yml"
    else:
        config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    # Load YAML configuration
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Override with environment variables where applicable
    _override_with_env_vars(config)
    
    return config

def _override_with_env_vars(config: Dict[str, Any]) -> None:
    """Override config values with environment variables"""
    
    # ODK Central settings
    if 'odk' in config:
        config['odk']['username'] = os.getenv('ODK_USERNAME', config['odk'].get('username'))
        config['odk']['password'] = os.getenv('ODK_PASSWORD', config['odk'].get('password'))
        config['odk']['project_id'] = os.getenv('ODK_PROJECT_ID', config['odk'].get('project_id'))
        
        # Override base URL if provided in env
        if os.getenv('ODK_BASE_URL'):
            config['odk']['base_url'] = os.getenv('ODK_BASE_URL')
    
    # Email notification settings
    if 'notifications' not in config:
        config['notifications'] = {}
    
    config['notifications'].update({
        'smtp_server': os.getenv('SMTP_SERVER'),
        'smtp_port': int(os.getenv('SMTP_PORT', 587)),
        'smtp_username': os.getenv('SMTP_USERNAME'),
        'smtp_password': os.getenv('SMTP_PASSWORD'),
        'recipients': os.getenv('NOTIFICATION_RECIPIENTS', '').split(','),
        'slack_webhook': os.getenv('SLACK_WEBHOOK_URL')
    })
    
    # Streamlit settings
    if 'dashboard' in config:
        config['dashboard']['secret_key'] = os.getenv('STREAMLIT_SECRET_KEY')
        if os.getenv('STREAMLIT_SERVER_PORT'):
            config['dashboard']['port'] = int(os.getenv('STREAMLIT_SERVER_PORT'))
    
    # Prefect settings
    if 'prefect' not in config:
        config['prefect'] = {}
    
    config['prefect'].update({
        'api_url': os.getenv('PREFECT_API_URL', 'http://127.0.0.1:4200/api'),
        'server_host': os.getenv('PREFECT_SERVER_HOST', '127.0.0.1'),
        'server_port': int(os.getenv('PREFECT_SERVER_PORT', 4200))
    })
    
    # Performance settings
    if 'performance' in config:
        if os.getenv('MAX_WORKERS'):
            config['performance']['n_workers'] = int(os.getenv('MAX_WORKERS'))
        if os.getenv('MEMORY_LIMIT_MB'):
            config['performance']['memory_limit'] = int(os.getenv('MEMORY_LIMIT_MB'))
        if os.getenv('CHUNK_SIZE'):
            config['performance']['chunk_size'] = int(os.getenv('CHUNK_SIZE'))
    
    # Environment flag
    config['environment'] = os.getenv('ENVIRONMENT', 'development')

def validate_config(config: Dict[str, Any]) -> bool:
    """
    Validate configuration for required fields
    
    Args:
        config: Configuration dictionary
        
    Returns:
        True if valid, raises ValueError if invalid
    """
    required_fields = [
        'project.name',
        'odk.base_url',
        'odk.username', 
        'odk.password',
        'odk.project_id'
    ]
    
    for field in required_fields:
        keys = field.split('.')
        value = config
        
        try:
            for key in keys:
                value = value[key]
            
            if not value:
                raise ValueError(f"Required configuration field '{field}' is empty")
                
        except KeyError:
            raise ValueError(f"Required configuration field '{field}' is missing")
    
    return True
