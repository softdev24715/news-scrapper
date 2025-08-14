import scrapy
from datetime import datetime, timezone, timedelta
from scrapy.spiders import XMLFeedSpider
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import logging
import uuid

class ForbesSpider(XMLFeedSpider):
    name = 'forbes'
    allowed_domains = ['forbes.ru']
    start_urls = ['http://90.156.202.84:33000/forbes/newrss']
    
    # Class-level set to track processed URLs across all instances
    processed_urls = set()
    
    # Define the iterator and itertag for XML parsing
    iterator = 'iternodes'
    itertag = 'item'
    
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Cache-Control': 'max-age=0',
            'Sec-Ch-Ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Linux"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'Connection': 'keep-alive',
        },
        'ROBOTSTXT_OBEY': False,  # Disable robots.txt to avoid detection
        'REDIRECT_ENABLED': False,  # Disable redirects to avoid captcha
        'COOKIES_ENABLED': True,
        'DOWNLOAD_TIMEOUT': 30,
        'RETRY_TIMES': 0,  # Disable retries to avoid multiple captcha attempts
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': None,  # Disable proxy
            'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': None,  # Disable redirects
            'scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware': None,  # Disable robots.txt
            'news_parser.middlewares.RotateUserAgentMiddleware': None,  # Disable user agent rotation
        },
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_DELAY': 10,  # Increase delay
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'DOWNLOAD_DELAY_RANDOMIZE': 5,  # Add random delay up to 5 seconds
    }
    
    def __init__(self, *args, **kwargs):
        super(ForbesSpider, self).__init__(*args, **kwargs)
        # Get today's and yesterday's dates for filtering
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        self.target_dates = [
            today.strftime('%Y-%m-%d'),
            yesterday.strftime('%Y-%m-%d')
        ]
        logging.info(f"Initializing Forbes spider for dates: {self.target_dates}")
        logging.info(f"Current processed URLs count: {len(self.processed_urls)}")

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                errback=self.handle_error,
                dont_filter=True,
                meta={
                    'dont_redirect': True,  # Prevent redirects to captcha
                    'handle_httpstatus_list': [302, 403, 503],
                    'dont_merge_cookies': False,
                    'max_retry_times': 0
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
        logging.debug(f"Parsing URL: {response.url}")
        logging.debug(f"Response status: {response.status}")
        logging.debug(f"Response headers: {response.headers}")
        
        # Handle captcha redirect
        if response.status == 302:
            location = response.headers.get('Location', b'').decode('utf-8')
            if 'captcha' in location.lower():
                logging.error(f"Captcha detected! Redirect URL: {location}")
                logging.error("Forbes is blocking the scraper. Consider using a different approach.")
                return
            else:
                logging.warning(f"Unexpected redirect to: {location}")
                return
        
        # Check if we got a valid XML response
        if not response.text.strip().startswith('<?xml'):
            logging.error(f"Invalid XML response from {response.url}")
            logging.debug(f"Response content: {response.text[:1000]}")  # Log first 1000 chars for debugging
            return
            
        # Process the XML feed
        for node in response.xpath('//item'):
            yield from self.parse_node(response, node)

    def parse_node(self, response, node):
        # Check if URL already processed
        url = node.xpath('link/text()').get()
        if not url or url in self.processed_urls:
            logging.debug(f"Skipping already processed or invalid URL: {url}")
            return
        
        # Mark URL as processed
        self.processed_urls.add(url)
        
        # Generate unique ID
        article_id = str(uuid.uuid4())
            
        # Get publication date
        pub_date = node.xpath('pubDate/text()').get()
        if not pub_date:
            logging.warning(f"No publication date found for URL: {url}")
            return
            
        # Parse the date
        try:
            dt = datetime.strptime(pub_date, '%a, %d %b %Y %H:%M:%S %z')
            date_str = dt.strftime('%Y-%m-%d')
            
            # Only process today's and yesterday's articles
            if date_str not in self.target_dates:
                logging.debug(f"Skipping article from {date_str} (not today or yesterday): {url}")
                return
                
            published_at = int(dt.timestamp())
            published_at_iso = dt.isoformat()
            
            logging.info(f"Processing article from {date_str}: {url}")
        except Exception as e:
            logging.error(f"Error parsing date {pub_date}: {e}")
            return
        
        # Get title
        title = node.xpath('title/text()').get()
        if not title:
            logging.warning(f"No title found for URL: {url}")
            return
            
        # Get content - try content field first, then fall back to description
        content = node.xpath('content/text()').get()
        if not content:
            content = node.xpath('description/text()').get()
            if not content:
                logging.warning(f"No content found for URL: {url}")
                return
            
        # Parse content with BeautifulSoup to clean it up
        soup = BeautifulSoup(content, 'html.parser')
        text_parts = []
        for p in soup.find_all('p'):
            text = p.get_text(strip=True)
            if text:
                text_parts.append(text)
                
        if not text_parts:
            logging.warning(f"No text content found in article: {url}")
            return
            
        # Create article with required structure matching Note.md format
        article = NewsArticle()
        article['id'] = article_id
        article['text'] = '\n'.join(text_parts)
        
        # Create metadata structure exactly as specified in Note.md
        article['metadata'] = {
            'source': 'forbes',
            'published_at': published_at,
            'published_at_iso': published_at_iso,
            'url': url,
            'header': title,
            'parsed_at': int(datetime.now().timestamp())
        }
        
        # Debug: Print found content
        logging.info(f"Processing article: {url} with ID: {article_id}")
        logging.info(f"Title found: {title}")
        logging.info(f"Text length: {len(article['text'])}")
        logging.info(f"Article date: {date_str}")
        
        yield article

    def closed(self, reason):
        logging.info(f"Forbes spider closed. Reason: {reason}")
        logging.info(f"Total URLs processed: {len(self.processed_urls)}") 