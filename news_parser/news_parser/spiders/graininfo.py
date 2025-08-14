import scrapy
from datetime import datetime, timezone, timedelta
from scrapy.spiders import Spider
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import logging
import uuid
import json
import re
from urllib.parse import urljoin

class GraininfoSpider(Spider):
    name = 'graininfo'
    allowed_domains = ['graininfo.ru']
    start_urls = ['https://graininfo.ru/news/']
    
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
        'DOWNLOAD_DELAY': 3  # Add delay between requests
    }
    
    def __init__(self, *args, **kwargs):
        super(GraininfoSpider, self).__init__(*args, **kwargs)
        # Get today's and yesterday's dates for filtering
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        self.target_dates = [
            today.strftime('%d.%m.%Y'),
            yesterday.strftime('%d.%m.%Y')
        ]
        
        logging.info(f"Initializing Graininfo spider for dates: {self.target_dates}")
        logging.info(f"Current processed URLs count: {len(self.processed_urls)}")

    def parse(self, response):
        """Parse the main news page and extract article data directly."""
        logging.info(f"Parsing main news page: {response.url}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all news items on the page
        # Based on the website structure, news items are in a list format
        news_items = soup.find_all('li')
        
        for item in news_items:
            # Look for date pattern in the item
            date_text = item.get_text(strip=True)
            
            # Extract date using regex pattern (DD.MM.YYYY)
            date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', date_text)
            if not date_match:
                continue
                
            article_date = date_match.group(1)
            
            # Only process today's and yesterday's articles
            if article_date not in self.target_dates:
                logging.debug(f"Skipping article from {article_date} (not today or yesterday)")
                continue
            
            # Find the link to the article
            link = item.find('a')
            if not link or not link.get('href'):
                continue
                
            article_url = urljoin(response.url, link['href'])
            
            # Check if already processed
            if article_url in self.processed_urls:
                logging.debug(f"Skipping already processed URL: {article_url}")
                continue
                
            self.processed_urls.add(article_url)
            
            # Get article title
            title = link.get_text(strip=True)
            if not title:
                logging.warning(f"No title found for URL: {article_url}")
                continue
            
            # Try to get content from the listing page
            # Look for content in the same list item
            content = None
            
            # Try to find content in the item text (excluding the title and date)
            item_text = item.get_text(strip=True)
            # Remove the date and title from the text to get content
            item_text = re.sub(r'\d{2}\.\d{2}\.\d{4}', '', item_text)
            item_text = re.sub(re.escape(title), '', item_text)
            item_text = item_text.strip()
            
            if item_text and len(item_text) > 50:
                content = item_text
                logging.debug("Found content in listing page item")
            
            # If no content found on listing page, we'll need to visit the article page
            if not content:
                logging.info(f"No content found on listing page, will visit article: {article_url}")
                yield scrapy.Request(
                    url=article_url,
                    callback=self.parse_article,
                    meta={
                        'title': title,
                        'article_date': article_date,
                        'article_url': article_url
                    },
                    errback=self.handle_error
                )
            else:
                # We have all the data we need from the listing page
                logging.info(f"Found article from {article_date}: {title}")
                logging.info(f"Content length: {len(content)}")
                
                # Parse the date and set time to 00:00 (midnight) of that day
                try:
                    dt = datetime.strptime(article_date, '%d.%m.%Y')
                    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                    published_at = int(dt.timestamp())
                    published_at_iso = dt.isoformat()
                except Exception as e:
                    logging.error(f"Error parsing date {article_date}: {e}")
                    continue
                
                # Create article with required structure matching Note.md format
                article = NewsArticle()
                article['id'] = str(uuid.uuid4())
                article['text'] = content
                
                # Create metadata structure exactly as specified in Note.md
                article['metadata'] = {
                    'source': 'graininfo',
                    'published_at': published_at,
                    'published_at_iso': published_at_iso,
                    'url': article_url,
                    'header': title,
                    'parsed_at': int(datetime.now().timestamp())
                }
                
                yield article
        
        # Check for pagination and follow next pages if needed
        # Look for "След." (Next) link
        next_link = soup.find('a', string=re.compile(r'След\.', re.IGNORECASE))
        if next_link and next_link.get('href'):
            next_url = urljoin(response.url, next_link['href'])
            logging.info(f"Following next page: {next_url}")
            yield scrapy.Request(
                url=next_url,
                callback=self.parse,
                errback=self.handle_error
            )

    def parse_article(self, response):
        """Parse individual article page to extract content."""
        logging.info(f"Parsing article: {response.url}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract article content from meta description tag
        meta_description = soup.find('meta', attrs={'name': 'description'})
        if meta_description and meta_description.get('content'):
            text = meta_description['content'].strip()
            logging.debug("Found content in meta description")
        else:
            logging.warning(f"No meta description found for URL: {response.url}")
            return
        
        if not text or len(text.strip()) < 50:  # Minimum content length
            logging.warning(f"Content too short for URL: {response.url}")
            return
        
        # Parse the date and set time to 00:00 (midnight) of that day
        article_date = response.meta['article_date']
        try:
            # Convert DD.MM.YYYY to datetime with time set to 00:00
            dt = datetime.strptime(article_date, '%d.%m.%Y')
            # Set time to midnight (00:00:00)
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            published_at = int(dt.timestamp())
            published_at_iso = dt.isoformat()
        except Exception as e:
            logging.error(f"Error parsing date {article_date}: {e}")
            return
        
        # Create article with required structure matching Note.md format
        article = NewsArticle()
        article['id'] = str(uuid.uuid4())
        article['text'] = text
        
        # Create metadata structure exactly as specified in Note.md
        article['metadata'] = {
            'source': 'graininfo',
            'published_at': published_at,
            'published_at_iso': published_at_iso,
            'url': response.url,
            'header': response.meta['title'],
            'parsed_at': int(datetime.now().timestamp())
        }
        
        # Debug: Print found content
        logging.info(f"Processing article: {response.url}")
        logging.info(f"Title found: {response.meta['title']}")
        logging.info(f"Text length: {len(article['text'])}")
        logging.info(f"Article date: {article_date}")
        
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