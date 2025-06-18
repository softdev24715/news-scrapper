import scrapy
from datetime import datetime
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
        today_str = datetime.now().strftime('%Y-%m-%d')
        logging.info(f"Looking for news block for date: {today_str}")

        # Find all date headers
        date_headers = soup.find_all('h2', class_='events__title')
        today_header = None
        for h in date_headers:
            time_tag = h.find('time')
            if time_tag and time_tag.get('datetime') == today_str:
                today_header = h
                break
        if not today_header:
            logging.info(f"No news block found for today: {today_str}")
            return
        logging.info(f"Found today's news block header.")

        # Collect all .hentry news items after this header until the next date header
        news_items = []
        for sib in today_header.find_next_siblings():
            if sib.name == 'h2' and 'events__title' in sib.get('class', []):
                break  # Stop at the next date header
            if sib.name == 'div' and 'hentry' in sib.get('class', []):
                news_items.append(sib)
        logging.info(f"Found {len(news_items)} news items for today.")

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
        # Extract the article title
        title_tag = soup.find('h1', class_='publication__title')
        title = title_tag.get_text(strip=True) if title_tag else ''
        # Extract the article content
        content_div = soup.find('div', class_='publication__text')
        paragraphs = content_div.find_all('p') if content_div else []
        article_text = '\n'.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
        article = NewsArticle()
        article['id'] = str(uuid.uuid4())
        article['text'] = article_text
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