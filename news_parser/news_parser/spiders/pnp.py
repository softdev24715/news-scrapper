import scrapy
from datetime import datetime, timezone, timedelta
from scrapy.spiders import Spider
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import logging
import uuid
import json
import re

class PnpSpider(Spider):
    name = 'pnp'
    allowed_domains = ['pnp.ru']
    
    # Class-level set to track processed URLs across all instances
    processed_urls = set()
    
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
        super(PnpSpider, self).__init__(*args, **kwargs)
        # Get today's date for filtering
        today = datetime.now()
        self.target_date = today.strftime('%Y-%m-%d')
        # Use direct RSS feed URL
        self.start_urls = ['https://www.pnp.ru/rss/index.xml']
        
        logging.info(f"Initializing PNP spider for date: {self.target_date}")
        logging.info(f"Current processed URLs count: {len(self.processed_urls)}")

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                errback=self.handle_error,
                dont_filter=True,
                meta={
                    'dont_redirect': False,
                    'handle_httpstatus_list': [302, 403, 503],
                    'dont_merge_cookies': False,
                    'max_retry_times': 5
                }
            )

    def handle_error(self, failure):
        logging.error(f"Request failed: {failure.value}")
        if hasattr(failure.value, 'response'):
            response = failure.value.response
            logging.error(f"HTTP Error: {response.status} for URL: {response.url}")
            logging.debug(f"Response headers: {response.headers}")
            logging.debug(f"Response body: {response.text[:1000]}")

    def parse(self, response):
        logging.debug(f"Parsing RSS feed: {response.url}")
        logging.debug(f"Response status: {response.status}")
        
        # Parse RSS content directly since we're accessing the RSS feed
        yield from self.parse_rss_content(response)

    def parse_rss_content(self, response):
        # Extract article URLs from RSS feed
        # The PNP RSS feed appears to be in a text format rather than proper XML
        # Let's try to extract URLs directly from the text content
        
        # Debug: Log the first part of the response content
        logging.debug(f"RSS feed content preview: {response.text[:1000]}...")
        
        # Skip XML parsing and go directly to text parsing since the content is in text format
        logging.info("Skipping XML parsing, using text-based parsing directly")
        yield from self.parse_text_rss(response)

    def parse_text_rss(self, response):
        """Parse the text-based RSS feed format that PNP uses"""
        import re
        
        # Get the text content
        text_content = response.text
        
        logging.debug(f"Text RSS content preview: {text_content[:1000]}...")
        
        # Extract URLs using regex pattern for PNP article URLs
        url_pattern = r'https://www\.pnp\.ru/[^/\s]+/[^/\s]+\.html'
        urls = re.findall(url_pattern, text_content)
        
        logging.info(f"Found {len(urls)} URLs using regex pattern")
        
        # Extract dates using regex pattern for RSS dates
        date_pattern = r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d{2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4} \d{2}:\d{2}:\d{2} \+\d{4}'
        dates = re.findall(date_pattern, text_content)
        
        logging.info(f"Found {len(dates)} dates using regex pattern")
        
        # Create a mapping of URLs to dates by finding the closest date before each URL
        url_date_mapping = {}
        
        # Split the text into lines to process sequentially
        lines = text_content.split('\n')
        current_date = None
        
        for line in lines:
            # Check if line contains a date
            date_match = re.search(date_pattern, line)
            if date_match:
                try:
                    current_date = datetime.strptime(date_match.group(0), '%a, %d %b %Y %H:%M:%S %z')
                    logging.debug(f"Found date: {current_date}")
                except Exception as e:
                    logging.warning(f"Error parsing date {date_match.group(0)}: {e}")
                    continue
            
            # Check if line contains a URL
            url_match = re.search(url_pattern, line)
            if url_match and current_date:
                url = url_match.group(0)
                url_date_mapping[url] = current_date
                logging.debug(f"Mapped URL {url} to date {current_date}")
        
        # Process URLs with date filtering
        processed_count = 0
        for url in urls:
            if url in self.processed_urls:
                logging.debug(f"Skipping already processed URL: {url}")
                continue
            
            # Check if we have a date for this URL
            if url in url_date_mapping:
                dt = url_date_mapping[url]
                if dt.strftime('%Y-%m-%d') != self.target_date:
                    logging.debug(f"Article not from today ({dt.strftime('%Y-%m-%d')}): {url}")
                    continue
                else:
                    logging.info(f"Article is from today: {url}")
            else:
                # If no date found, assume it's from today for now
                logging.debug(f"No date found for URL, assuming today: {url}")
            
            # Additional validation to ensure it's a proper article URL
            if not url or not url.startswith('http') or 'javascript:' in url:
                logging.debug(f"Skipping invalid URL: {url}")
                continue
                
            self.processed_urls.add(url)
            processed_count += 1
            logging.info(f"Processing article URL: {url}")
            yield scrapy.Request(
                url=url,
                callback=self.parse_article,
                errback=self.handle_error,
                meta={
                    'dont_redirect': False,
                    'handle_httpstatus_list': [302, 403, 503],
                    'dont_merge_cookies': False,
                    'max_retry_times': 5
                }
            )
        
        logging.info(f"Processed {processed_count} URLs from text RSS feed")

    def parse_article(self, response):
        logging.debug(f"Parsing article: {response.url}")
        
        # Check if this is actually an article page (not a section page)
        if not response.url:
            logging.debug(f"Skipping empty URL")
            return
        
        # Additional validation - check if URL looks like an article
        url_parts = response.url.split('/')
        if len(url_parts) <= 4 or url_parts[-1] == '':
            logging.debug(f"Skipping section page URL: {response.url}")
            return
        
        # Get title - updated selector
        title = response.css('article h1::text').get()
        if not title:
            logging.warning(f"No title found for URL: {response.url}")
            return
            
        # Try to get date from multiple sources
        date_str = None
        
        # 1. Try JSON-LD first
        json_ld = response.css('script[type="application/ld+json"]::text').get()
        if json_ld:
            try:
                data = json.loads(json_ld)
                if isinstance(data, dict) and data.get('@type') == 'Article':
                    date_str = data.get('datePublished')
                    logging.info(f"Found date in JSON-LD: {date_str}")
            except json.JSONDecodeError as e:
                logging.warning(f"Failed to parse JSON-LD: {e}")
        
        # 2. Try time element with datetime attribute
        if not date_str:
            date_str = response.css('time::attr(datetime)').get()
            if date_str:
                logging.info(f"Found date in time element: {date_str}")
        
        # 3. Try time element text content
        if not date_str:
            time_text = response.css('time::text').get()
            if time_text:
                # Extract date from text like "09.06.2025 в 05:21"
                match = re.search(r'(\d{2}\.\d{2}\.\d{4})', time_text)
                if match:
                    date_str = match.group(1)
                    logging.info(f"Found date in time text: {date_str}")
        
        # 4. Try meta tags
        if not date_str:
            date_str = response.css('meta[property="article:published_time"]::attr(content)').get()
            if date_str:
                logging.info(f"Found date in meta tag: {date_str}")
        
        if not date_str:
            logging.warning(f"No date found for URL: {response.url}")
            return
            
        try:
            # Handle different date formats
            if '+' in date_str or 'Z' in date_str:
                # ISO format with timezone
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            elif re.match(r'\d{2}\.\d{2}\.\d{4}', date_str):
                # DD.MM.YYYY format
                dt = datetime.strptime(date_str, '%d.%m.%Y')
            else:
                # Try parsing as ISO format without timezone
                dt = datetime.fromisoformat(date_str)
            
            published_at = int(dt.timestamp())
            published_at_iso = dt.isoformat()
            logging.info(f"Parsed date: {published_at_iso}")
        except Exception as e:
            logging.error(f"Error parsing date {date_str}: {e}")
            return
            
        # Get content - updated selector
        content_parts = response.css('div.js-mediator-article p::text').getall()
        if not content_parts:
            logging.warning(f"No content found for URL: {response.url}")
            return
            
        # Create article with required structure matching Note.md format
        article = NewsArticle()
        article['id'] = str(uuid.uuid4())
        article['text'] = '\n'.join(content_parts)
        
        # Create metadata structure exactly as specified in Note.md
        article['metadata'] = {
            'source': 'pnp',
            'published_at': published_at,
            'published_at_iso': published_at_iso,
            'url': response.url,
            'header': title,
            'parsed_at': int(datetime.now().timestamp())
        }
        
        # Debug: Print found content
        logging.info(f"Processing article: {response.url}")
        logging.info(f"Title found: {title}")
        logging.info(f"Text length: {len(article['text'])}")
        
        yield article

    def closed(self, reason):
        logging.info(f"PNP spider closed. Reason: {reason}")
        logging.info(f"Total URLs processed: {len(self.processed_urls)}") 