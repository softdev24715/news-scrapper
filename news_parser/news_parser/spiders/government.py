import scrapy
from datetime import datetime
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import uuid
import logging

class GovernmentSpider(scrapy.Spider):
    name = 'government'
    allowed_domains = ['government.ru']
    start_urls = ['http://government.ru/news/']
    
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

    def parse(self, response):
        logging.info(f"Parsing main page: {response.url}")
        # Save the first page HTML to a file
        with open('government_first_page.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        logging.info("Saved HTML to government_first_page.html")
        
        # Find all news items
        news_items = response.css('div.news-item')
        logging.info(f"Found {len(news_items)} news items on the page")
        
        for item in news_items:
            # Get article link
            link = item.css('a::attr(href)').get()
            if not link:
                continue
                
            url = response.urljoin(link)
            logging.info(f"Found article URL: {url}")
            
            # Get article title
            title = item.css('h3::text').get() or item.css('a::text').get()
            if not title:
                continue
            title = title.strip()
            logging.info(f"Found article title: {title}")
            
            # Get article time
            time_text = item.css('span.time::text').get()
            if not time_text:
                continue
            logging.info(f"Found article time: {time_text}")
            
            yield scrapy.Request(
                url=url,
                callback=self.parse_article,
                meta={
                    'time_text': time_text,
                    'url': url,
                    'title': title
                }
            )

    def parse_article(self, response):
        time_text = response.meta['time_text']
        url = response.meta['url']
        title = response.meta['title']
        logging.info(f"Parsing article: {url}")
        
        # Get article content
        content_html = response.css('div.article-text').get()
        if not content_html:
            content_html = response.css('div.article__text').get()
        
        soup = BeautifulSoup(content_html or '', 'html.parser')
        paragraphs = soup.find_all('p')
        article_text = '\n'.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
        
        article = NewsArticle()
        article['id'] = str(uuid.uuid4())
        article['text'] = article_text
        article['metadata'] = {
            'source': 'government',
            'published_at': int(datetime.now().timestamp()),
            'published_at_iso': datetime.now().isoformat(),
            'url': url,
            'header': title,
            'parsed_at': int(datetime.now().timestamp())
        }
        logging.info(f"Yielding article: {url} with ID: {article['id']}")
        yield article 