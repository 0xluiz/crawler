# config.py

import json

def load_config(file_path='config.json'):
    """Load configuration from a JSON file."""
    with open(file_path, 'r') as config_file:
        config = json.load(config_file)
    return config
