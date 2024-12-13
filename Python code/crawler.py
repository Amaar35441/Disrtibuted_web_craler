import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import threading
import time
import logging

from database import DatabaseManager
from url_queue import URLQueue

class WebCrawler:
    def __init__(self, seed_urls, max_depth=2, num_threads=5):
        """
        Initialize web crawler with seed URLs
        """
        self.seed_urls = seed_urls
        self.max_depth = max_depth
        self.url_queue = URLQueue()
        self.db_manager = DatabaseManager()
        self.num_threads = num_threads
        self.visited_urls = set()
        self.visited_lock = threading.Lock()
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def is_valid_url(self, url):
        """
        Check if URL is valid and within same domain
        """
        try:
            parsed_url = urlparse(url)
            return parsed_url.scheme in ['http', 'https'] and parsed_url.netloc
        except Exception:
            return False

    def crawl(self, url, current_depth=0):
        """
        Crawl a single URL
        """
        # Check if URL has been visited
        with self.visited_lock:
            if current_depth > self.max_depth or url in self.visited_urls:
                return
            self.visited_urls.add(url)

        try:
            # Fetch webpage
            self.logger.info(f"Crawling: {url}")
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                # Parse content
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Store webpage content
                self.db_manager.insert_url(url, response.text, 'crawled')

                # Extract links
                for link in soup.find_all('a', href=True):
                    absolute_link = urljoin(url, link['href'])
                    
                    if self.is_valid_url(absolute_link):
                        # Store link relationship
                        self.db_manager.insert_link(url, absolute_link)
                        
                        # Add to queue for further crawling
                        self.url_queue.add_url(absolute_link)

        except requests.RequestException as e:
            self.logger.error(f"Error crawling {url}: {e}")
            self.db_manager.update_url_status(url, 'failed')

    def worker(self):
        """
        Worker thread for crawling
        """
        while True:
            url = self.url_queue.get_url()
            if not url:
                break

            try:
                self.crawl(url)
            except Exception as e:
                self.logger.error(f"Unexpected error crawling {url}: {e}")
            finally:
                self.url_queue.task_done()
                time.sleep(1)  # Politeness delay

    def start_crawling(self):
        """
        Start distributed web crawling
        """
        # Add seed URLs to queue
        for url in self.seed_urls:
            self.url_queue.add_url(url)

        # Create and start threads
        threads = []
        for _ in range(self.num_threads):
            thread = threading.Thread(target=self.worker)
            thread.daemon = True  # Allow program to exit even if threads are running
            thread.start()
            threads.append(thread)

        # Wait for queue to be empty
        self.url_queue.queue.join()

        # Close database connections
        self.db_manager.close_connections()
