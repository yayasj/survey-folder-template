"""
Survey Pipeline Package
Core utilities and CLI for survey data management
"""

__version__ = "1.0.0"
__author__ = "2M Corp Data Team"
__email__ = "data@2mcorp.com"

from .config import load_config
from .utils import setup_logging, get_project_root

__all__ = [
    "load_config",
    "setup_logging", 
    "get_project_root"
]
