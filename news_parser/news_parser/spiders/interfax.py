import scrapy
from datetime import datetime
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
        # Get today's date for filtering
        self.today = datetime.now().strftime('%Y-%m-%d')
        logging.info(f"Filtering for articles from: {self.today}")

    def sitemap_filter(self, entries):
        for entry in entries:
            # Get lastmod from the entry, default to today if not found
            lastmod = entry.get('lastmod', datetime.now().isoformat())
            # Check if the lastmod date matches today
            if lastmod.split('T')[0] == self.today:
                logging.info(f"Found today's article: {entry['loc']}")
                yield entry

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
        date_elem = soup.select_one('article time')
        if date_elem:
            date_str = date_elem.get('datetime')
            if date_str:
                try:
                    dt = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S%z')
                    published_at = int(dt.timestamp())
                    published_at_iso = dt.isoformat()
                except ValueError:
                    # If date parsing fails, use current time
                    current_time = datetime.now()
                    published_at = int(current_time.timestamp())
                    published_at_iso = current_time.isoformat()
        else:
            # If no date found, use current time
            current_time = datetime.now()
            published_at = int(current_time.timestamp())
            published_at_iso = current_time.isoformat()
        
        # Get author if available
        author = soup.select_one('article .author')
        author_text = author.get_text(strip=True) if author else None
        
        # Get categories/tags if available
        categories = soup.select('article .tags a')
        categories_list = [cat.get_text(strip=True) for cat in categories] if categories else None
        
        # Get images if available
        images = soup.select('article img')
        images_list = [img.get('src') for img in images if img.get('src')] if images else None
        
        # Create article with required structure
        article = NewsArticle()
        article['id'] = article_id
        article['text'] = '\n'.join(article_text)
        article['metadata'] = {
            'source': 'interfax',
            'published_at': published_at,
            'published_at_iso': published_at_iso,
            'url': response.url,
            'header': title_text,
            'parsed_at': int(datetime.now().timestamp())
        }
        
        # Add optional metadata if available
        if author_text:
            article['metadata']['author'] = author_text
        if categories_list:
            article['metadata']['categories'] = categories_list
        if images_list:
            article['metadata']['images'] = images_list
        
        yield article 