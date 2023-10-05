import yaml
import logging
from typing import Any
from typing import Dict


def load_config(path: str) -> Dict[str, Any]:
    """Loads and returns a configuration file as a dictionary"""
    with open(path) as f:
        config = yaml.safe_load(f)
    return config