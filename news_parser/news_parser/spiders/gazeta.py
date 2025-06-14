import scrapy
from datetime import datetime, timezone
from scrapy.spiders import SitemapSpider
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import logging
import uuid

class GazetaSpider(SitemapSpider):
    name = 'gazeta'
    allowed_domains = ['gazeta.ru']
    sitemap_urls = ['https://www.gazeta.ru/sitemap.xml']
    
    # Class-level set to track processed URLs across all instances
    processed_urls = set()
    
    def __init__(self, *args, **kwargs):
        super(GazetaSpider, self).__init__(*args, **kwargs)
        # Get today's date for filtering
        self.today = datetime.now().strftime('%Y-%m-%d')
        logging.info(f"Initializing Gazeta spider for date: {self.today}")
        logging.info(f"Using sitemap URL: {self.sitemap_urls[0]}")
        logging.info(f"Current processed URLs count: {len(self.processed_urls)}")

    def sitemap_filter(self, entries):
        for entry in entries:
            # Extract date from lastmod
            date = entry.get('lastmod', '').split('T')[0]  # Get date part from ISO format
            if date == self.today:
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
        
        # Extract main content using the correct selector
        text_parts = []
        content_div = soup.find('div', class_='b_article-text', attrs={'itemprop': 'articleBody'})
        
        if content_div:
            # Get all paragraphs
            paragraphs = content_div.find_all('p')
            for p in paragraphs:
                text = p.get_text(strip=True)
                if text:
                    text_parts.append(text)
            logging.info(f"Found {len(text_parts)} paragraphs in main content")
        else:
            logging.warning(f"Could not find main content div with selector .b_article-text[itemprop='articleBody']")
        
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
        
        # Get author if available
        author = response.css('div.article__author::text').get()
        author_text = author.strip() if author else None
            
        # Get categories/tags if available
        categories = response.css('div.article__tags a::text').getall()
        categories_list = [cat.strip() for cat in categories] if categories else None
            
        # Get images if available
        images = response.css('div.article__image img::attr(src)').getall()
        images_list = images if images else None
            
        # Create article with required structure
        article = NewsArticle()
        article['id'] = article_id
        article['text'] = '\n'.join(text_parts)
        article['metadata'] = {
            'source': 'gazeta',
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
        
        # Debug: Print found content
        logging.info(f"Processing article: {response.url} with ID: {article_id}")
        logging.info(f"Title found: {title_text}")
        logging.info(f"Text length: {len(article['text'])}")
        
        yield article

    def closed(self, reason):
        logging.info(f"Gazeta spider closed. Reason: {reason}")
        logging.info(f"Total URLs processed: {len(self.processed_urls)}") 