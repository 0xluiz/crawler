import asyncio
import logging
from database import create_connection
from crawler import start_crawler
from config import load_config

# Load configuration from config.json
config = load_config()

# Set up logging level based on the debug flag in config.json
log_level = logging.DEBUG if config.get('debug', False) else logging.INFO

# Configure logging
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crawler.log"),  # Log to a file named crawler.log
        logging.StreamHandler()  # Also log to the console
    ]
)

# Create a connection to the database
connection = create_connection(config['db_config'])  # Pass only the 'db_config' part

if connection:
    try:
        # Start the event loop
        asyncio.run(start_crawler(connection))
    except KeyboardInterrupt:
        logging.info("Crawler interrupted and shutting down gracefully.")
    finally:
        connection.close()
else:
    logging.error("Failed to establish a database connection.")
