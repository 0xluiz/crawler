import aiohttp
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import defaultdict
from database import (
    get_uncrawled_fqdn, save_fqdn, save_words, mark_fqdn_as_crawled,
    save_search_result, get_uncrawled_search, mark_search_as_crawled,
    get_to_crawl_url, save_to_crawl_url, mark_to_crawl_as_crawled
)
from throttle import handle_request_error
from config import load_config
import logging
import time
import requests

config = load_config()

# Dictionary to store average response times per domain
domain_response_times = defaultdict(list)

async def fetch_html(session, url):
    """Asynchronously fetch HTML content from a given URL with retries."""
    logging.debug(f"Fetching HTML for URL: {url}")
    parsed_url = urlparse(url)
    domain = parsed_url.netloc

    if not parsed_url.scheme:
        url = "https://" + url  # Default to HTTPS if no scheme is provided

    retries = config['retry_policy']['max_retries']
    initial_wait = config['retry_policy']['initial_wait']

    for attempt in range(retries + 1):
        try:
            start_time = time.time()  # Start timing the request
            async with session.get(url, headers=config['headers'], ssl=False, timeout=30) as response:
                elapsed_time = time.time() - start_time  # Calculate the elapsed time
                domain_response_times[domain].append(elapsed_time)  # Track response time

                # Calculate the average response time for the domain
                avg_response_time = sum(domain_response_times[domain]) / len(domain_response_times[domain])
                logging.debug(f"Average response time for {domain}: {avg_response_time:.2f} seconds")

                content_type = response.headers.get('Content-Type', '').lower()

                # Define the acceptable content types
                acceptable_types = ['text/html', 'application/xhtml+xml', 'application/xml', 'text/plain']

                if any(ctype in content_type for ctype in acceptable_types):
                    return await response.text()
                else:
                    logging.info(f"Skipping non-relevant content at URL: {url}, Content-Type: {content_type}")
                    return None
        except asyncio.TimeoutError:
            logging.error(f"Request timed out for URL: {url}, attempt {attempt + 1} of {retries + 1}")
            if attempt < retries:
                await asyncio.sleep(initial_wait)
            else:
                return None
        except aiohttp.ClientError as e:
            handle_request_error(url, e)
            logging.error(f"Failed to fetch HTML for URL: {url}, Error: {e}")
            return None

def extract_fqdns_and_words(html, base_url):
    """Extract FQDNs and the top 5 most frequent words from HTML content."""
    logging.debug(f"Extracting FQDNs and words from base URL: {base_url}")
    soup = BeautifulSoup(html, 'html.parser')
    fqdns = set()
    words = {}

    # Extract FQDNs from links
    for link in soup.find_all('a', href=True):
        href = link['href']
        
        if href.startswith('#'):
            continue
        
        absolute_url = urljoin(base_url, href)
        parsed_url = urlparse(absolute_url)
        fqdn = parsed_url.hostname
        
        if fqdn:
            fqdns.add(absolute_url)  # Store the full URL in fqdns
    
    # Extract words from the page content
    text = soup.get_text()
    for word in text.split():
        word = word.lower()
        if word.isalpha() and len(word) >= 4:
            words[word] = words.get(word, 0) + 1
    
    top_words = sorted(words.items(), key=lambda item: item[1], reverse=True)[:5]
    top_words = [word for word, _ in top_words]
    
    while len(top_words) < 5:
        top_words.append('')

    logging.info(f"Extracted {len(fqdns)} FQDNs and top 5 words: {top_words}")
    return fqdns, top_words

def adaptive_throttle(domain):
    """Dynamically throttle requests based on the average response time for the domain."""
    if domain in domain_response_times:
        avg_response_time = sum(domain_response_times[domain]) / len(domain_response_times[domain])
        
        # Base the throttle time on average response time with a minimum of 1 second
        throttle_time = max(avg_response_time * 2, 1)  # Multiply by 2 as a safety margin
        logging.debug(f"Throttling for {throttle_time:.2f} seconds for domain {domain}")
        time.sleep(throttle_time)
    else:
        # Default throttle time if no previous response time is available
        logging.debug(f"Default throttling for domain {domain}")
        time.sleep(1)  # Default to 1 second for new domains

def search_google_cse(query):
    """Perform a Google Custom Search and return the list of URLs."""
    api_key = config['google_cse']['api_key']  # Use API key from config.json
    cse_id = config['google_cse']['cx']  # Use CSE ID from config.json
    search_url = f"https://www.googleapis.com/customsearch/v1?key={api_key}&cx={cse_id}&q={query}"

    response = requests.get(search_url)
    if response.status_code == 200:
        search_results = response.json()
        urls = [item['link'] for item in search_results.get('items', [])]
        return urls
    else:
        logging.error(f"Google CSE request failed with status code: {response.status_code}")
        return []

async def crawl_and_store(connection, session, url, is_search_result=True):
    """Asynchronously crawl a webpage, store FQDNs and words, and handle to_crawl URLs."""
    cursor = connection.cursor()
    parsed_url = urlparse(url)
    domain = parsed_url.netloc

    adaptive_throttle(domain)  # Throttle before fetching HTML
    
    html = await fetch_html(session, url)
    
    if html:
        base_fqdns, top_words = extract_fqdns_and_words(html, url)
        fqdn = parsed_url.hostname
        
        if fqdn:
            save_fqdn(cursor, fqdn)
            cursor.execute("SELECT id FROM fqdns WHERE fqdn = %s", (fqdn,))
            result = cursor.fetchall()  # Fetch all to ensure no results are left

            if result:
                fqdn_id = result[0][0]  # Get the first (and presumably only) result
                save_words(cursor, fqdn_id, top_words)
                connection.commit()  # Commit after saving the words
            else:
                logging.warning(f"FQDN '{fqdn}' was not found in the database.")
        
        for full_url in base_fqdns:
            save_to_crawl_url(cursor, full_url)  # Save the full URL and extract FQDN

    # Whether HTML was fetched or not, mark the URL as crawled
    if is_search_result:
        mark_search_as_crawled(cursor, url)
    else:
        mark_to_crawl_as_crawled(cursor, url)
        
    connection.commit()
    cursor.close()

async def start_crawler(connection):
    """Asynchronously start the crawler."""
    async with aiohttp.ClientSession() as session:
        while True:
            cursor = connection.cursor()

            uncrawled_search = get_uncrawled_search(cursor)
            if uncrawled_search:
                url = uncrawled_search[0]
                await crawl_and_store(connection, session, url, is_search_result=True)
            else:
                to_crawl_url = get_to_crawl_url(cursor)
                if to_crawl_url:
                    url = to_crawl_url[0]
                    await crawl_and_store(connection, session, url, is_search_result=False)
                else:
                    with open('wordlist.txt', 'r') as file:
                        wordlist = [line.strip() for line in file if line.strip()]

                    tasks = []
                    for word in wordlist[:5]:
                        urls = search_google_cse(word)  # Use the function to search Google CSE
                        for url in urls:
                            save_search_result(cursor, url)
                            tasks.append(asyncio.ensure_future(crawl_and_store(connection, session, url)))

                    await asyncio.gather(*tasks)
                    
            cursor.close()
