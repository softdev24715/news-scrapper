import scrapy
from datetime import datetime, timedelta
from scrapy.spiders import SitemapSpider
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import logging
import uuid

class InterfaxSpider(SitemapSpider):
    name = 'interfax'
    allowed_domains = ['interfax.ru']
    sitemap_urls = ['https://www.interfax.ru/SEO_SiteMapLastChanges.xml']
    
    def __init__(self, *args, **kwargs):
        super(InterfaxSpider, self).__init__(*args, **kwargs)
        # Get today's and yesterday's dates for filtering
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        self.target_dates = [
            today.strftime('%Y-%m-%d'),
            yesterday.strftime('%Y-%m-%d')
        ]
        logging.info(f"Filtering for articles from: {self.target_dates}")

    def sitemap_filter(self, entries):
        for entry in entries:
            # Get lastmod from the entry, default to today if not found
            lastmod = entry.get('lastmod', datetime.now().isoformat())
            # Check if the lastmod date matches today or yesterday
            entry_date = lastmod.split('T')[0]
            if entry_date in self.target_dates:
                logging.info(f"Found article from {entry_date}: {entry['loc']}")
                yield entry
            else:
                logging.debug(f"Skipping article from {entry_date} (not today or yesterday): {entry['loc']}")

    def parse(self, response):
        # Generate unique ID
        article_id = str(uuid.uuid4())
        
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract title from h1 within article
        title = soup.select_one('article h1')
        title_text = title.get_text(strip=True) if title else None
        
        # Extract main content from article paragraphs
        article_text = []
        article_content = soup.select_one('article')
        if article_content:
            # Get all paragraphs
            for p in article_content.find_all('p'):
                text = p.get_text(strip=True)
                if text:
                    article_text.append(text)
        
        # Get publication date
        published_at = None
        published_at_iso = None
        article_date = None
        date_elem = soup.select_one('article time')
        if date_elem:
            date_str = date_elem.get('datetime')
            if date_str:
                try:
                    dt = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S%z')
                    published_at = int(dt.timestamp())
                    published_at_iso = dt.isoformat()
                    article_date = dt.strftime('%Y-%m-%d')
                    logging.info(f"Parsed article date: {article_date}")
                except ValueError:
                    # If date parsing fails, use current time
                    current_time = datetime.now()
                    published_at = int(current_time.timestamp())
                    published_at_iso = current_time.isoformat()
                    article_date = current_time.strftime('%Y-%m-%d')
                    logging.warning(f"Could not parse date '{date_str}', using current time")
        else:
            # If no date found, use current time
            current_time = datetime.now()
            published_at = int(current_time.timestamp())
            published_at_iso = current_time.isoformat()
            article_date = current_time.strftime('%Y-%m-%d')
            logging.warning("No date found in article, using current time")
        
        # Check if the article date is from today or yesterday
        if article_date not in self.target_dates:
            logging.debug(f"Skipping article from {article_date} (not today or yesterday): {response.url}")
            return
        
        # Create article with required structure matching Note.md format
        article = NewsArticle()
        article['id'] = article_id
        article['text'] = '\n'.join(article_text)
        
        # Create metadata structure exactly as specified in Note.md
        article['metadata'] = {
            'source': 'interfax',
            'published_at': published_at,
            'published_at_iso': published_at_iso,
            'url': response.url,
            'header': title_text,
            'parsed_at': int(datetime.now().timestamp())
        }
        
        logging.info(f"Yielding article from {article_date}: {response.url}")
        logging.info(f"Title: {title_text}")
        logging.info(f"Text length: {len(article['text'])}")
        
        yield article 