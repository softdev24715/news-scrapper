import scrapy
from datetime import datetime, timedelta
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import uuid
import logging
import re
from urllib.parse import urljoin

class GazetaSpider(scrapy.Spider):
    name = 'gazeta'
    allowed_domains = ['gazeta.ru']
    start_urls = ['https://www.gazeta.ru/sitemap_news-y.xml']
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'LOG_LEVEL': 'DEBUG',
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
        },
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
    }

    def __init__(self, *args, **kwargs):
        super(GazetaSpider, self).__init__(*args, **kwargs)
        # Get today's and yesterday's dates for filtering
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        self.target_dates = [
            today.strftime('%Y-%m-%d'),
            yesterday.strftime('%Y-%m-%d')
        ]
        logging.info(f"Initializing Gazeta spider for dates: {self.target_dates}")

    def parse(self, response):
        logging.info(f"Parsing sitemap: {response.url}")
        
        # Parse the XML sitemap
        from xml.etree import ElementTree as ET
        
        try:
            # Parse XML content
            root = ET.fromstring(response.text)
            
            # Define namespace
            namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            
            # Find all URL entries
            urls = root.findall('.//ns:url', namespace)
            logging.info(f"Found {len(urls)} URLs in sitemap")
            
            for url_elem in urls:
                # Extract URL
                loc_elem = url_elem.find('ns:loc', namespace)
                if loc_elem is None or loc_elem.text is None:
                    continue
                    
                url = loc_elem.text.strip()
                
                # Extract last modification date
                lastmod_elem = url_elem.find('ns:lastmod', namespace)
                if lastmod_elem is None or lastmod_elem.text is None:
                    continue
                    
                lastmod_text = lastmod_elem.text.strip()
                
                # Parse the date
                try:
                    # Handle timezone format: 2025-07-12T19:49:27+03:00
                    if '+' in lastmod_text:
                        # Remove timezone for parsing
                        date_part = lastmod_text.split('+')[0]
                        parsed_date = datetime.fromisoformat(date_part)
                    else:
                        parsed_date = datetime.fromisoformat(lastmod_text)
                        
                    logging.info(f"Parsed date: {parsed_date} from {lastmod_text}")
                except ValueError as e:
                    logging.warning(f"Could not parse date {lastmod_text}: {e}")
                    continue
                
                # Check if the date is from today or yesterday
                date_str = parsed_date.strftime('%Y-%m-%d')
                if date_str in self.target_dates:
                    logging.info(f"Processing article from {date_str}: {url}")
                    yield scrapy.Request(
                        url=url,
                        callback=self.parse_article,
                        meta={
                            'url': url,
                            'parsed_date': parsed_date,
                            'lastmod': lastmod_text
                        }
                    )
                else:
                    logging.debug(f"Skipping article from {date_str} (not today or yesterday): {url}")
                    
        except ET.ParseError as e:
            logging.error(f"Failed to parse XML sitemap: {e}")
            return

    def parse_article(self, response):
        url = response.meta['url']
        parsed_date = response.meta.get('parsed_date')
        lastmod = response.meta.get('lastmod')
        
        logging.info(f"Parsing article: {url}")
        
        # Extract title from the page
        title = response.css('h1::text').get()
        if not title:
            title = response.css('title::text').get()
        if not title:
            title = response.css('h1.article__title::text').get()
        if not title:
            title = response.css('h1.b_article-title::text').get()
        
        if title:
            title = title.strip()
        else:
            title = "Untitled Article"
            
        logging.info(f"Found article title: {title}")
        
        # Get article content - try multiple selectors
        content_html = response.css('div.b_article-text').get()  # Primary selector
        if not content_html:
            content_html = response.css('div.article-text').get()  # Fallback
        if not content_html:
            content_html = response.css('div.article__text').get()  # Another fallback
        if not content_html:
            content_html = response.css('div.b_article-content').get()  # Another fallback
        
        soup = BeautifulSoup(content_html or '', 'html.parser')
        paragraphs = soup.find_all('p')
        article_text = '\n'.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
        
        # If no paragraphs found, try to get all text from the content div
        if not article_text and content_html:
            article_text = soup.get_text(strip=True)
        
        # Use parsed date if available, otherwise use current time
        if parsed_date:
            published_at = int(parsed_date.timestamp())
            published_at_iso = parsed_date.isoformat()
        else:
            published_at = int(datetime.now().timestamp())
            published_at_iso = datetime.now().isoformat()
        
        # Create article with required structure
        article = NewsArticle()
        article['id'] = str(uuid.uuid4())
        article['text'] = article_text
        
        # Create metadata structure
        article['metadata'] = {
            'source': 'gazeta',
            'published_at': published_at,
            'published_at_iso': published_at_iso,
            'url': url,
            'header': title,
            'parsed_at': int(datetime.now().timestamp()),
            'lastmod': lastmod
        }
        
        # Log the date information
        if parsed_date:
            date_str = parsed_date.strftime('%Y-%m-%d')
            logging.info(f"Yielding article from {date_str}: {url} with ID: {article['id']}")
        else:
            logging.info(f"Yielding article (date unknown): {url} with ID: {article['id']}")
        
        yield article 