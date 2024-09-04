import mysql.connector
import logging
from urllib.parse import urlparse

def create_connection(db_config):
    """Create a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(**db_config)  # Use the db_config directly
        logging.info("Successfully connected to the database.")
        return connection
    except mysql.connector.Error as err:
        logging.error(f"Database connection error: {err}")
        return None

def save_fqdn(cursor, fqdn):
    """Save an FQDN to the database if it doesn't already exist."""
    try:
        cursor.execute("SELECT id FROM fqdns WHERE fqdn = %s", (fqdn,))
        result = cursor.fetchone()

        if result is None:
            cursor.execute("INSERT INTO fqdns (fqdn) VALUES (%s)", (fqdn,))
            logging.info(f"FQDN '{fqdn}' inserted into the database.")
        else:
            logging.info(f"FQDN '{fqdn}' already exists in the database, skipping insertion.")
    except mysql.connector.Error as err:
        logging.error(f"Error saving FQDN '{fqdn}': {err}")

def save_words(cursor, fqdn_id, words):
    """Save the top 5 words associated with an FQDN."""
    try:
        cursor.execute(
            "INSERT INTO words (fqdn_id, word1, word2, word3, word4, word5) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (fqdn_id, words[0], words[1], words[2], words[3], words[4])
        )
        logging.info(f"Top 5 words for FQDN ID {fqdn_id} saved.")
    except mysql.connector.Error as err:
        logging.error(f"Error saving words for FQDN ID {fqdn_id}: {err}")

def get_uncrawled_fqdn(cursor):
    """Retrieve an uncrawled FQDN from the database."""
    try:
        cursor.execute("SELECT fqdn FROM fqdns WHERE crawled = FALSE LIMIT 1")
        return cursor.fetchone()
    except mysql.connector.Error as err:
        logging.error(f"Error retrieving uncrawled FQDN: {err}")
        return None

def mark_fqdn_as_crawled(cursor, fqdn):
    """Mark an FQDN as crawled."""
    try:
        cursor.execute("UPDATE fqdns SET crawled = TRUE, last_crawled = NOW() WHERE fqdn = %s", (fqdn,))
        logging.info(f"FQDN '{fqdn}' marked as crawled.")
    except mysql.connector.Error as err:
        logging.error(f"Error marking FQDN '{fqdn}' as crawled: {err}")

def save_search_result(cursor, url):
    """Save a search result URL to the database."""
    try:
        cursor.execute("INSERT IGNORE INTO searches (search_url, crawled) VALUES (%s, FALSE)", (url,))
        logging.info(f"Search result URL '{url}' saved.")
    except mysql.connector.Error as err:
        logging.error(f"Error saving search result URL '{url}': {err}")

def get_uncrawled_search(cursor):
    """Retrieve an uncrawled search result URL."""
    try:
        cursor.execute("SELECT search_url FROM searches WHERE crawled = FALSE LIMIT 1")
        return cursor.fetchone()
    except mysql.connector.Error as err:
        logging.error(f"Error retrieving uncrawled search result: {err}")
        return None

def mark_search_as_crawled(cursor, url):
    """Mark a search result URL as crawled."""
    try:
        cursor.execute("UPDATE searches SET crawled = TRUE WHERE search_url = %s", (url,))
        logging.info(f"Search result URL '{url}' marked as crawled.")
    except mysql.connector.Error as err:
        logging.error(f"Error marking search result URL '{url}' as crawled: {err}")

def save_to_crawl_url(cursor, url):
    """Save a URL to be crawled later."""
    try:
        fqdn = urlparse(url).hostname
        if fqdn:
            save_fqdn(cursor, fqdn)  # Save the FQDN if it doesn't already exist
        cursor.execute("INSERT IGNORE INTO to_crawl (url, crawled) VALUES (%s, FALSE)", (url,))
        logging.info(f"URL '{url}' added to to_crawl list.")
    except mysql.connector.Error as err:
        logging.error(f"Error saving URL '{url}' to to_crawl list: {err}")

def get_to_crawl_url(cursor):
    """Retrieve a URL from the to_crawl table that has not been crawled."""
    try:
        cursor.execute("SELECT url FROM to_crawl WHERE crawled = FALSE LIMIT 1")
        return cursor.fetchone()
    except mysql.connector.Error as err:
        logging.error(f"Error retrieving URL from to_crawl list: {err}")
        return None

def mark_to_crawl_as_crawled(cursor, url):
    """Mark a URL in the to_crawl table as crawled."""
    try:
        cursor.execute("UPDATE to_crawl SET crawled = TRUE WHERE url = %s", (url,))
        logging.info(f"URL '{url}' marked as crawled.")
    except mysql.connector.Error as err:
        logging.error(f"Error marking URL '{url}' as crawled: {err}")
