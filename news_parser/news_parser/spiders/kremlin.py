import scrapy
from datetime import datetime, timedelta
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import uuid
import logging

class KremlinSpider(scrapy.Spider):
    name = 'kremlin'
    allowed_domains = ['kremlin.ru']
    start_urls = ['http://kremlin.ru/events/president/news']
    
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
        },
        'HTTPPROXY_ENABLED': False
    }

    def parse(self, response):
        logging.info(f"Parsing main page: {response.url}")
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Try multiple dates: yesterday, today, and day before yesterday
        dates_to_try = [
            (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),  # yesterday
            datetime.now().strftime('%Y-%m-%d'),  # today
        ]
        
        today_header = None
        used_date = None
        
        for date_str in dates_to_try:
            logging.info(f"Looking for news block for date: {date_str}")
            
            # Find all date headers
            date_headers = soup.find_all('h2', class_='events__title')
            for h in date_headers:
                time_tag = h.find('time')
                if time_tag and time_tag.get('datetime') == date_str:
                    today_header = h
                    used_date = date_str
                    break
            if today_header:
                break
                
        if not today_header:
            logging.info(f"No news block found for any of the dates: {dates_to_try}")
            return
            
        logging.info(f"Found news block for date: {used_date}")

        # Collect all .hentry news items after this header until the next date header
        news_items = []
        for sib in today_header.find_next_siblings():
            if sib.name == 'h2' and 'events__title' in sib.get('class', []):
                break  # Stop at the next date header
            if sib.name == 'div' and 'hentry' in sib.get('class', []):
                news_items.append(sib)
        logging.info(f"Found {len(news_items)} news items for date: {used_date}")

        for item in news_items:
            h3 = item.find('h3', class_='hentry__title')
            if not h3:
                continue
            a_tag = h3.find('a')
            if not a_tag or not a_tag.get('href'):
                continue
            url = response.urljoin(a_tag['href'])
            logging.info(f"Found news URL: {url}")
            yield scrapy.Request(url, callback=self.parse_article)

    def parse_article(self, response):
        logging.info(f"Parsing article: {response.url}")
        soup = BeautifulSoup(response.text, 'html.parser')
        # Save the HTML for inspection (only for the first article)
        if not hasattr(self, '_saved_article_html'):
            with open('kremlin_article_debug.html', 'w', encoding='utf-8') as f:
                f.write(response.text)
            logging.info(f"Saved article HTML to kremlin_article_debug.html for inspection")
            self._saved_article_html = True
        # Extract the article title using the correct selector
        title_tag = soup.find('h1', class_='entry-title p-name')
        title = title_tag.get_text(strip=True) if title_tag else ''
        logging.info(f"Extracted title: {title}")
        # Extract the article content using the correct selector
        content_div = soup.find('div', class_='entry-content e-content read__internal_content')
        paragraphs = content_div.find_all('p') if content_div else []
        article_text = '\n'.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
        logging.info(f"Extracted text length: {len(article_text)} characters")
        # Create article with required structure matching Note.md format
        article = NewsArticle()
        article['id'] = str(uuid.uuid4())
        article['text'] = article_text
        
        # Create metadata structure exactly as specified in Note.md
        article['metadata'] = {
            'source': 'kremlin',
            'published_at': int(datetime.now().timestamp()),
            'published_at_iso': datetime.now().isoformat(),
            'url': response.url,
            'header': title,
            'parsed_at': int(datetime.now().timestamp())
        }
        
        logging.info(f"Yielding article: {response.url} with ID: {article['id']}")
        yield article 