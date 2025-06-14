import scrapy
from scrapy.spiders import SitemapSpider
from news_parser.items import NewsArticle
import uuid
import time
from datetime import datetime
from bs4 import BeautifulSoup
import logging

class RIASpider(SitemapSpider):
    name = 'ria'
    allowed_domains = ['ria.ru']
    
    def __init__(self, *args, **kwargs):
        super(RIASpider, self).__init__(*args, **kwargs)
        # Get today's date for sitemap
        today = datetime.now().strftime('%Y%m%d')
        self.sitemap_urls = [f'https://ria.ru/sitemap_article.xml?date_start={today}&date_end={today}']
        logging.info(f"Filtering for articles from: {today}")
    
    def parse(self, response):
        """Parse article page"""
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Generate unique ID
        article_id = str(uuid.uuid4())
        
        # Extract title
        title_elem = soup.select_one('.article__header .article__title')
        title_text = title_elem.get_text(strip=True) if title_elem else None
        
        # Extract text
        text_blocks = soup.select('.article__body .article__block[data-type="text"] .article__text')
        if text_blocks:
            article_text = ' '.join([block.get_text(strip=True) for block in text_blocks if block.get_text(strip=True)])
        else:
            # Try alternative selector if primary fails
            text_blocks = soup.select('.article__text p')
            article_text = ' '.join([block.get_text(strip=True) for block in text_blocks if block.get_text(strip=True)])
        
        # Handle publication date
        published_at = None
        published_at_iso = None
        published_meta = soup.select_one('meta[property="article:published_time"]')
        if published_meta and published_meta.get('content'):
            try:
                # Convert RIA.ru format (20250607T0609) to ISO format
                date_str = published_meta['content']
                dt = datetime.strptime(date_str, '%Y%m%dT%H%M')
                published_at = int(dt.timestamp())
                published_at_iso = dt.isoformat() + 'Z'
            except (ValueError, AttributeError) as e:
                self.logger.warning(f"Could not parse date: {date_str} - {str(e)}")
                # Set current time as fallback
                current_time = int(time.time())
                published_at = current_time
                published_at_iso = datetime.fromtimestamp(current_time).isoformat() + 'Z'
        else:
            # Set current time if no date available
            current_time = int(time.time())
            published_at = current_time
            published_at_iso = datetime.fromtimestamp(current_time).isoformat() + 'Z'
        
        # Create article with required structure
        article = NewsArticle()
        article['id'] = article_id
        article['text'] = article_text
        article['metadata'] = {
            'source': 'ria.ru',
            'published_at': published_at,
            'published_at_iso': published_at_iso,
            'url': response.url,
            'header': title_text,
            'parsed_at': int(time.time())
        }
        
        yield article 