import scrapy
from datetime import datetime, timezone, timedelta
from scrapy.spiders import SitemapSpider
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import logging
import uuid

class RGSpider(SitemapSpider):
    name = 'rg'
    allowed_domains = ['rg.ru']
    
    # Class-level set to track processed URLs across all instances
    processed_urls = set()
    
    def __init__(self, *args, **kwargs):
        super(RGSpider, self).__init__(*args, **kwargs)
        # Get today's and yesterday's dates for filtering
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        self.target_dates = [
            today.strftime('%Y-%m-%d'),
            yesterday.strftime('%Y-%m-%d')
        ]
        
        # Calculate yesterday's start and end timestamps
        yesterday_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_end = yesterday.replace(hour=23, minute=59, second=59)
        
        # Calculate today's start and end timestamps
        today_start = today.replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = today_start.replace(hour=23, minute=59, second=59)
        
        # Convert to Unix timestamps
        self.start_timestamp = int(yesterday_start.timestamp())
        self.end_timestamp = int(today_end.timestamp())
        
        # Construct sitemap URL for both days
        self.sitemap_urls = [f'https://rg.ru/sitemaps/index.xml?date_start={self.start_timestamp}&date_end={self.end_timestamp}']
        
        logging.info(f"Initializing RG spider for dates: {self.target_dates}")
        logging.info(f"Using sitemap URL: {self.sitemap_urls[0]}")
        logging.info(f"Current processed URLs count: {len(self.processed_urls)}")

    def sitemap_filter(self, entries):
        for entry in entries:
            # Extract date from lastmod
            date = entry.get('lastmod', '').split('T')[0]  # Get date part from ISO format
            if date in self.target_dates:
                logging.debug(f"Processing sitemap entry from {date}: {entry.get('loc')}")
                yield entry

    def parse(self, response):
        # Check if URL already processed
        if response.url in self.processed_urls:
            logging.debug(f"Skipping already processed URL: {response.url}")
            return
        
        # Mark URL as processed
        self.processed_urls.add(response.url)
        
        # Generate unique ID
        article_id = str(uuid.uuid4())
        
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract main title
        title_elem = soup.find('h1', class_='article__title')
        if title_elem:
            title_text = title_elem.get_text(strip=True)
        else:
            # Fallback to any h1 if specific class not found
            title_elem = soup.find('h1')
            title_text = title_elem.get_text(strip=True) if title_elem else None
        
        # Extract main content using the specific selector
        text_parts = []
        content_div = soup.find('div', class_='PageArticleContent_content__mdxza')
        
        if content_div:
            # Get all paragraphs
            paragraphs = content_div.find_all('p')
            for p in paragraphs:
                text = p.get_text(strip=True)
                if text:
                    text_parts.append(text)
            logging.info(f"Found {len(text_parts)} paragraphs in main content")
        else:
            logging.warning(f"Could not find main content div with selector .PageArticleContent_content__mdxza")
        
        # Get article timestamp if available
        published_at = None
        published_at_iso = None
        timestamp = response.css('time.article__date::attr(datetime)').get()
        if timestamp:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            published_at = int(dt.timestamp())
            published_at_iso = dt.isoformat()
        else:
            current_time = datetime.now()
            published_at = int(current_time.timestamp())
            published_at_iso = current_time.isoformat()
        
        # Create article with required structure matching Note.md format
        article = NewsArticle()
        article['id'] = article_id
        article['text'] = '\n'.join(text_parts)
        
        # Create metadata structure exactly as specified in Note.md
        article['metadata'] = {
            'source': 'rg',
            'published_at': published_at,
            'published_at_iso': published_at_iso,
            'url': response.url,
            'header': title_text,
            'parsed_at': int(datetime.now().timestamp())
        }
        
        # Debug: Print found content
        logging.info(f"Processing article: {response.url} with ID: {article_id}")
        logging.info(f"Title found: {title_text}")
        logging.info(f"Text length: {len(article['text'])}")
        
        yield article

    def closed(self, reason):
        logging.info(f"RG spider closed. Reason: {reason}")
        logging.info(f"Total URLs processed: {len(self.processed_urls)}") 