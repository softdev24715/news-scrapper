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
        
        # Generate date range from July 9th, 2025 to today
        start_date = datetime.now() - timedelta(days=2)
        end_date = datetime.now()
        
        # Generate list of all dates in the range
        self.target_dates = []
        current_date = start_date
        while current_date <= end_date:
            self.target_dates.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        # Calculate start and end timestamps for the entire range
        range_start = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        range_end = end_date.replace(hour=23, minute=59, second=59)
        
        # Convert to Unix timestamps
        self.start_timestamp = int(range_start.timestamp())
        self.end_timestamp = int(range_end.timestamp())
        
        # Construct sitemap URL for the entire date range
        self.sitemap_urls = [f'https://rg.ru/sitemaps/index.xml?date_start={self.start_timestamp}&date_end={self.end_timestamp}']
        
        logging.info(f"Initializing RG spider for date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        logging.info(f"Target dates: {self.target_dates}")
        logging.info(f"Using sitemap URL: {self.sitemap_urls[0]}")
        logging.info(f"Target date range: {range_start} to {range_end}")
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
        
        # Get article timestamp from meta tag
        published_at = None
        published_at_iso = None
        
        # Try to get timestamp from meta tag first
        timestamp_meta = response.css('meta[property="article:published_time"]::attr(content)').get()
        if timestamp_meta:
            try:
                # Handle timezone format: 2025-07-12T20:18:00
                if '+' in timestamp_meta:
                    # Remove timezone for parsing
                    date_part = timestamp_meta.split('+')[0]
                    dt = datetime.fromisoformat(date_part)
                else:
                    dt = datetime.fromisoformat(timestamp_meta)
                
                published_at = int(dt.timestamp())
                published_at_iso = dt.isoformat()
                logging.info(f"Parsed article date from meta tag: {dt}")
            except ValueError as e:
                logging.warning(f"Could not parse date '{timestamp_meta}' from meta tag: {e}")
                # Fallback to current time
                current_time = datetime.now()
                published_at = int(current_time.timestamp())
                published_at_iso = current_time.isoformat()
        else:
            # Fallback to time element if meta tag not found
            timestamp = response.css('time.article__date::attr(datetime)').get()
            if timestamp:
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    published_at = int(dt.timestamp())
                    published_at_iso = dt.isoformat()
                    logging.info(f"Parsed article date from time element: {dt}")
                except ValueError:
                    logging.warning(f"Could not parse date '{timestamp}' from time element")
                    current_time = datetime.now()
                    published_at = int(current_time.timestamp())
                    published_at_iso = current_time.isoformat()
            else:
                # Last resort: use current time
                current_time = datetime.now()
                published_at = int(current_time.timestamp())
                published_at_iso = current_time.isoformat()
                logging.warning("No date found in article (neither meta tag nor time element), using current time")
        
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