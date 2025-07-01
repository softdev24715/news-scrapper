import scrapy
from datetime import datetime, timedelta
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import uuid
import logging
import re

class GovernmentSpider(scrapy.Spider):
    name = 'government'
    allowed_domains = ['government.ru']
    
    def start_requests(self):
        """
        Generate requests for today's and yesterday's news
        """
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        
        # Create URLs for today and yesterday
        urls = []
        for date in [yesterday, today]:
            date_str = date.strftime('%d.%m.%Y')
            url = f'http://government.ru/news/?dt.since={date_str}&dt.till={date_str}'
            urls.append((url, date_str))
        
        for url, date_str in urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={'date': date_str},
                headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                }
            )
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'LOG_LEVEL': 'DEBUG',
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
        },
        'HTTPPROXY_ENABLED': False
    }

    def parse(self, response):
        date_str = response.meta.get('date', 'unknown')
        logging.info(f"Parsing government news for date: {date_str}")
        
        # Save the HTML for inspection
       
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all news items using the correct selectors
        # Each news item has: headline_date, headline_title, and a link
        news_items = []
        
        # Find all headline_date spans
        date_spans = soup.find_all('span', class_='headline_date')
        logging.info(f"Found {len(date_spans)} date spans")
        
        for date_span in date_spans:
            # Get the date
            time_tag = date_span.find('time')
            if not time_tag:
                continue
                
            date_text = time_tag.get_text(strip=True)
            datetime_attr = time_tag.get('datetime', '')
            
            # Find the parent container that contains both date and title
            parent = date_span.parent
            if not parent:
                continue
                
            # Find the title
            title_span = parent.find('span', class_='headline_title_link')
            if not title_span:
                continue
                
            title = title_span.get_text(strip=True)
            
            # Find the link
            link_tag = parent.find('a', href=re.compile(r'/news/\d+/'))
            if not link_tag:
                continue
                
            article_url = response.urljoin(link_tag['href'])
            
            news_item = {
                'date': date_text,
                'datetime': datetime_attr,
                'title': title,
                'url': article_url
            }
            
            news_items.append(news_item)
            logging.info(f"Found news item: {date_text} - {title[:50]}...")
        
        logging.info(f"Found {len(news_items)} news items for date {date_str}")
        
        # Process each news item
        for item in news_items:
            yield scrapy.Request(
                url=item['url'],
                callback=self.parse_article,
                meta={
                    'date': item['date'],
                    'datetime': item['datetime'],
                    'title': item['title'],
                    'url': item['url']
                }
            )

    def parse_article(self, response):
        date = response.meta.get('date', '')
        datetime_str = response.meta.get('datetime', '')
        title = response.meta.get('title', '')
        url = response.meta.get('url', response.url)
        
        logging.info(f"Parsing government article: {url}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        
        
        # Extract the article headline using the correct selector
        headline_tag = soup.find('h3', class_='reader_article_headline')
        if headline_tag:
            extracted_title = headline_tag.get_text(strip=True)
            logging.info(f"Extracted headline: {extracted_title}")
            # Use extracted title if available, otherwise use the one from meta
            title = extracted_title if extracted_title else title
        
        # Extract the article main text using the correct selector
        content_div = soup.find('div', class_='reader_article_body')
        article_text = ''
        if content_div:
            paragraphs = content_div.find_all('p')
            article_text = '\n'.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
            logging.info(f"Found content using reader_article_body selector")
        else:
            logging.warning("Could not find reader_article_body div")
        
        # Parse the datetime to get proper timestamp
        published_at = int(datetime.now().timestamp())
        published_at_iso = datetime.now().isoformat()
        
        if datetime_str:
            try:
                # Parse datetime like "2025-06-20T19:00:00+04:00"
                dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
                published_at = int(dt.timestamp())
                published_at_iso = dt.isoformat()
            except ValueError:
                logging.warning(f"Could not parse datetime: {datetime_str}")
        
        # Create article with required structure matching Note.md format
        article = NewsArticle()
        article['id'] = str(uuid.uuid4())
        article['text'] = article_text
        
        # Create metadata structure exactly as specified in Note.md
        article['metadata'] = {
            'source': 'government',
            'published_at': published_at,
            'published_at_iso': published_at_iso,
            'url': url,
            'header': title,
            'parsed_at': int(datetime.now().timestamp())
        }
        
        logging.info(f"Yielding article: {url} with ID: {article['id']}")
        logging.info(f"Article text length: {len(article_text)} characters")
        yield article 