# main.py

import logging
import threading
import signal
import sys
from database import create_connection, get_uncrawled_search, save_search_result, mark_search_as_crawled, save_fqdn
from crawler import crawl, search_google_cse
from throttle import throttle_request
from urllib.parse import urlparse
from config import load_config
from threading import Event

# Setup logging
logging.basicConfig(
    filename='crawler.log',
    filemode='a',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)

logging.info("Crawler started.")

config = load_config()
threads = []
stop_event = Event()
num_threads = config.get('num_threads', 5)  # Default to 5 threads if not specified

def signal_handler(sig, frame):
    """Signal handler to gracefully shut down the application."""
    logging.info('Graceful shutdown initiated...')
    stop_event.set()  # Signal all threads to stop
    for thread in threads:
        thread.join()  # Wait for all threads to finish
    sys.exit(0)

def start_crawler_thread():
    """Start a thread for crawling."""
    connection = create_connection()
    
    while not stop_event.is_set():
        cursor = connection.cursor()

        # Fetch an uncrawled search result URL
        uncrawled_search = get_uncrawled_search(cursor)
        
        if uncrawled_search:
            url = uncrawled_search[0]
            fqdn = urlparse(url).hostname
            if fqdn:
                mark_search_as_crawled(cursor, url)  # Mark this search result as crawled
                save_fqdn(cursor, fqdn)  # Save only the FQDN
                crawl(connection)
            throttle_request()
        else:
            # If no uncrawled search results remain, perform a new Google CSE search
            with open('wordlist.txt', 'r') as file:
                wordlist = [line.strip() for line in file if line.strip()]

            for word in wordlist:
                urls = search_google_cse(word)
                for url in urls:
                    save_search_result(cursor, url)  # Save the search result URL
                if stop_event.is_set():
                    break  # Exit if stop event is set

        cursor.close()

    connection.close()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start threads for crawling based on num_threads setting
    for _ in range(num_threads):
        thread = threading.Thread(target=start_crawler_thread)
        thread.start()
        threads.append(thread)
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    logging.info("Crawler stopped.")
