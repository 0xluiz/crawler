# throttle.py

import time
import random
import logging
from config import load_config

config = load_config()
logger = logging.getLogger()

def throttle_request():
    """Throttle requests to avoid overloading the server."""
    delay = random.uniform(config['throttling']['min_delay'], config['throttling']['max_delay'])
    time.sleep(delay)

def handle_request_error(url, error):
    """Handle errors that occur during HTTP requests."""
    logger.error(f"Error fetching {url}: {error}")
    throttle_request()
