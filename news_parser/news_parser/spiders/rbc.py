import scrapy
from datetime import datetime
from scrapy.spiders import SitemapSpider
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import logging
from urllib.parse import urlparse
import uuid

class RBCSpider(SitemapSpider):
    name = 'rbc'
    allowed_domains = ['rbc.ru']
    sitemap_urls = ['https://www.rbc.ru/sitemap_index.xml']
    sitemap_rules = [
        ('/news/', 'parse_article'),
        ('/economics/', 'parse_article'),
        ('/business/', 'parse_article'),
        ('/society/', 'parse_article'),
        ('/politics/', 'parse_article'),
        ('/technology_and_media/', 'parse_article')
    ]

    def __init__(self, *args, **kwargs):
        super(RBCSpider, self).__init__(*args, **kwargs)
        # Get today's date for filtering
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.today_sitemaps = set()

    def sitemap_filter(self, entries):
        for entry in entries:
            # For sitemap index entries, only keep today's sitemaps
            if 'sitemap' in entry.get('loc', ''):
                lastmod = entry.get('lastmod', '').split('T')[0]
                if lastmod == self.today:
                    self.today_sitemaps.add(entry.get('loc'))
                    yield entry
            # For article entries, only keep today's articles
            else:
                lastmod = entry.get('lastmod', '').split('T')[0]
                if lastmod == self.today:
                    yield entry

    def parse_article(self, response):
        # Generate unique ID
        article_id = str(uuid.uuid4())
        
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract main title
        title_elem = soup.find('h1', class_='article__header__title')
        if title_elem:
            title_text = title_elem.get_text(strip=True)
        else:
            # Fallback to any h1 if specific class not found
            title_elem = soup.find('h1')
            title_text = title_elem.get_text(strip=True) if title_elem else None
        
        # Extract main content
        text_parts = []
        content_div = soup.find('div', class_='article__text')
        
        if content_div:
            # Get all paragraphs
            paragraphs = content_div.find_all('p')
            for p in paragraphs:
                text = p.get_text(strip=True)
                if text:
                    text_parts.append(text)
        
        # Get article timestamp if available
        published_at = None
        published_at_iso = None
        timestamp = response.css('div.article__header__date::attr(datetime)').get()
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
            'source': 'rbc',
            'published_at': published_at,
            'published_at_iso': published_at_iso,
            'url': response.url,
            'header': title_text,
            'parsed_at': int(datetime.now().timestamp())
        }
        
        # Debug: Print found content
        logging.info(f"Title found: {title_text}")
        logging.info(f"Text length: {len(article['text'])}")
        
        yield article 