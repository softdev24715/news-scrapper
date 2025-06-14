import scrapy
from datetime import datetime, timezone, timedelta
from scrapy.spiders import Spider
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import logging
import uuid
import json
import re
import xml.etree.ElementTree as ET

class GraininfoSpider(Spider):
    name = 'graininfo'
    allowed_domains = ['graininfo.ru']
    start_urls = ['https://graininfo.ru/rss/']
    
    # Class-level set to track processed URLs across all instances
    processed_urls = set()
    
    # Define namespaces
    namespaces = {
        'default': 'http://backend.userland.com/rss2',
        'turbo': 'http://turbo.yandex.ru',
        'yandex': 'http://news.yandex.ru'
    }
    
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        },
        'ROBOTSTXT_OBEY': True,
        'REDIRECT_ENABLED': True,
        'REDIRECT_MAX_TIMES': 5,
        'COOKIES_ENABLED': True,
        'DOWNLOAD_TIMEOUT': 30,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429, 302],
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': 900,
            'news_parser.middlewares.RotateUserAgentMiddleware': 543,
        },
        'COOKIES_DEBUG': True,  # Track cookie handling
        'CONCURRENT_REQUESTS': 1,  # Reduce request rate
        'DOWNLOAD_DELAY': 5  # Add delay between requests
    }
    
    def __init__(self, *args, **kwargs):
        super(GraininfoSpider, self).__init__(*args, **kwargs)
        # Get today's date for filtering
        today = datetime.now()
        self.target_date = today.strftime('%Y-%m-%d')
        
        logging.info(f"Initializing Graininfo spider for date: {self.target_date}")
        logging.info(f"Current processed URLs count: {len(self.processed_urls)}")

    def parse(self, response):
        """Parse the RSS feed and yield items."""
        logging.debug(f"Parsing RSS feed: {response.url}")
        
        try:
            # Parse XML with ElementTree
            root = ET.fromstring(response.text)
            
            # Register namespaces
            for prefix, uri in self.namespaces.items():
                ET.register_namespace(prefix, uri)
            
            # Find all item elements
            for item in root.findall('.//item'):
                # Get URL and check if already processed
                url = item.find('link')
                if url is None or url.text is None:
                    logging.warning("No URL found in item")
                    continue
                    
                url = url.text
                if url in self.processed_urls:
                    logging.debug(f"Skipping already processed URL: {url}")
                    continue
                    
                self.processed_urls.add(url)
                
                # Get title
                title = item.find('title')
                if title is None or title.text is None:
                    logging.warning(f"No title found for URL: {url}")
                    continue
                    
                # Get publication date
                date_elem = item.find('pubDate')
                if date_elem is None or date_elem.text is None:
                    logging.warning(f"No date found for URL: {url}")
                    continue
                    
                try:
                    # Parse RFC 822 date format
                    dt = datetime.strptime(date_elem.text, '%a, %d %b %Y %H:%M:%S %z')
                    published_at = int(dt.timestamp())
                    published_at_iso = dt.isoformat()
                    logging.info(f"Parsed date: {published_at_iso}")
                except Exception as e:
                    logging.error(f"Error parsing date {date_elem.text}: {e}")
                    continue
                    
                # Try to get content from yandex:full-text first
                content = None
                yandex_full_text = item.find('.//{http://news.yandex.ru}full-text')
                if yandex_full_text is not None and yandex_full_text.text:
                    content = yandex_full_text.text
                    logging.debug("Found content in yandex:full-text")
                
                # If no yandex content, try turbo:content
                if not content:
                    turbo_content = item.find('.//{http://turbo.yandex.ru}content')
                    if turbo_content is not None and turbo_content.text:
                        content = turbo_content.text
                        logging.debug("Found content in turbo:content")
                
                # If still no content, try description
                if not content:
                    description = item.find('description')
                    if description is not None and description.text:
                        content = description.text
                        logging.debug("Found content in description")
                
                if not content:
                    logging.warning(f"No content found for URL: {url}")
                    continue
                
                # Clean HTML from content
                soup = BeautifulSoup(content, 'html.parser')
                text = soup.get_text(separator='\n', strip=True)
                
                # Get author if available
                author = item.find('.//{http://purl.org/dc/elements/1.1/}creator')
                author_text = author.text.strip() if author is not None and author.text else None
                    
                # Get category if available
                category = item.find('category')
                category_text = category.text.strip() if category is not None and category.text else None
                    
                # Create article with required structure
                article = NewsArticle()
                article['id'] = str(uuid.uuid4())
                article['text'] = text
                article['metadata'] = {
                    'source': 'graininfo',
                    'published_at': published_at,
                    'published_at_iso': published_at_iso,
                    'url': url,
                    'header': title.text,
                    'parsed_at': int(datetime.now().timestamp())
                }
                
                # Add optional metadata if available
                if author_text:
                    article['metadata']['author'] = author_text
                if category_text:
                    article['metadata']['categories'] = [category_text]
                
                # Debug: Print found content
                logging.info(f"Processing article: {url}")
                logging.info(f"Title found: {title.text}")
                logging.info(f"Text length: {len(article['text'])}")
                
                yield article
                
        except ET.ParseError as e:
            logging.error(f"Error parsing XML: {e}")
            # Try alternative parsing with BeautifulSoup
            soup = BeautifulSoup(response.text, 'xml')
            for item in soup.find_all('item'):
                # Process items using BeautifulSoup
                url = item.find('link')
                if not url or not url.text:
                    continue
                    
                url = url.text
                if url in self.processed_urls:
                    continue
                    
                self.processed_urls.add(url)
                
                title = item.find('title')
                if not title or not title.text:
                    continue
                    
                date_elem = item.find('pubDate')
                if not date_elem or not date_elem.text:
                    continue
                    
                try:
                    dt = datetime.strptime(date_elem.text, '%a, %d %b %Y %H:%M:%S %z')
                    published_at = int(dt.timestamp())
                    published_at_iso = dt.isoformat()
                except Exception as e:
                    logging.error(f"Error parsing date {date_elem.text}: {e}")
                    continue
                    
                # Try to get content from different sources
                content = None
                yandex_full_text = item.find('yandex:full-text')
                if yandex_full_text and yandex_full_text.text:
                    content = yandex_full_text.text
                    logging.debug("Found content in yandex:full-text")
                
                if not content:
                    turbo_content = item.find('turbo:content')
                    if turbo_content and turbo_content.text:
                        content = turbo_content.text
                        logging.debug("Found content in turbo:content")
                
                if not content:
                    description = item.find('description')
                    if description and description.text:
                        content = description.text
                        logging.debug("Found content in description")
                
                if not content:
                    logging.warning(f"No content found for URL: {url}")
                    continue
                
                # Clean HTML from content
                text = BeautifulSoup(content, 'html.parser').get_text(separator='\n', strip=True)
                
                # Get author and category
                author = item.find('dc:creator')
                author_text = author.text.strip() if author and author.text else None
                
                category = item.find('category')
                category_text = category.text.strip() if category and category.text else None
                
                # Create article
                article = NewsArticle()
                article['id'] = str(uuid.uuid4())
                article['text'] = text
                article['metadata'] = {
                    'source': 'graininfo',
                    'published_at': published_at,
                    'published_at_iso': published_at_iso,
                    'url': url,
                    'header': title.text,
                    'parsed_at': int(datetime.now().timestamp())
                }
                
                if author_text:
                    article['metadata']['author'] = author_text
                if category_text:
                    article['metadata']['categories'] = [category_text]
                
                logging.info(f"Processing article: {url}")
                logging.info(f"Title found: {title.text}")
                logging.info(f"Text length: {len(article['text'])}")
                
                yield article

    def handle_error(self, failure):
        logging.error(f"Request failed: {failure.value}")
        if hasattr(failure.value, 'response'):
            response = failure.value.response
            logging.error(f"HTTP Error: {response.status} for URL: {response.url}")
            logging.debug(f"Response headers: {response.headers}")
            logging.debug(f"Response body: {response.text[:1000]}")

    def closed(self, reason):
        logging.info(f"Graininfo spider closed. Reason: {reason}")
        logging.info(f"Total URLs processed: {len(self.processed_urls)}") 