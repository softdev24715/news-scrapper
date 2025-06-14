import scrapy
from datetime import datetime
from scrapy.spiders import SitemapSpider
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import logging
import uuid

class KommersantSpider(SitemapSpider):
    name = 'kommersant'
    allowed_domains = ['kommersant.ru']
    sitemap_urls = ['https://www.kommersant.ru/sitemaps/sitemap_daily.xml']
    sitemap_rules = [
        ('/doc/', 'parse_article'),
        ('/news/', 'parse_article')
    ]

    def parse_article(self, response):
        # Generate unique ID
        article_id = str(uuid.uuid4())
        
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract main title from h1 tag
        title_elem = soup.find('h1', class_='doc_header__name')
        if title_elem:
            title_text = title_elem.get_text(strip=True)
        else:
            # Fallback to any h1 if specific class not found
            title_elem = soup.find('h1')
            title_text = title_elem.get_text(strip=True) if title_elem else None
        
        # Extract main content
        text_parts = []
        content_div = soup.find('div', class_='doc__body')
        
        if content_div:
            # Get all paragraphs with specific classes
            paragraphs = content_div.find_all('p', class_=['doc__text', 'doc__thought'])
            for p in paragraphs:
                # Skip author attribution paragraphs
                if 'document_authors' not in p.get('class', []):
                    text = p.get_text(strip=True)
                    if text:
                        text_parts.append(text)
        
        # Get article timestamp if available
        published_at = None
        published_at_iso = None
        timestamp = response.css('time.article__time::attr(datetime)').get()
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
            'source': 'kommersant',
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
        logging.info(f"Title found: {title_text}")
        logging.info(f"Text length: {len(article['text'])}")
        
        yield article 