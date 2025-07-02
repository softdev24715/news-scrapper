import scrapy
from datetime import datetime, timedelta
from scrapy.spiders import XMLFeedSpider
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import logging
import uuid

class TASSSpider(XMLFeedSpider):
    name = 'tass'
    allowed_domains = ['tass.ru']
    start_urls = ['https://tass.ru/rss/yandex.xml']
    iterator = 'iternodes'
    itertag = 'item'
    
    # Class-level set to track processed URLs across all instances
    processed_urls = set()
    
    # Register namespaces
    namespaces = [
        ('yandex', 'http://news.yandex.ru'),
        ('media', 'http://search.yahoo.com/mrss/'),
        ('turbo', 'http://turbo.yandex.ru')
    ]
    
    def __init__(self, *args, **kwargs):
        super(TASSSpider, self).__init__(*args, **kwargs)
        # Get today's and yesterday's dates for filtering
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        self.target_dates = [
            today.strftime('%Y-%m-%d'),
            yesterday.strftime('%Y-%m-%d')
        ]
        logging.info(f"Initializing TASS spider for dates: {self.target_dates}")
        logging.info(f"Current processed URLs count: {len(self.processed_urls)}")

    def parse(self, response):
        # This method is required by XMLFeedSpider
        return self.parse_nodes(response, self.itertag)

    def parse_node(self, response, node):
        # Get URL and check if already processed
        url = node.xpath('link/text()').get()
        if not url:
            logging.debug("No URL found in node")
            return
            
        if url in self.processed_urls:
            logging.debug(f"Skipping already processed URL: {url}")
            return
        
        # Get publication date
        pub_date = node.xpath('pubDate/text()').get()
        if not pub_date:
            logging.debug(f"No publication date found for URL: {url}")
            return
            
        # Convert pubDate to datetime
        dt = datetime.strptime(pub_date, '%a, %d %b %Y %H:%M:%S %z')
        article_date = dt.strftime('%Y-%m-%d')
        # Check if article is from today or yesterday
        if article_date not in self.target_dates:
            logging.debug(f"Article not from today or yesterday ({article_date}): {url}")
            return
        else:
            logging.info(f"Article is from {article_date}: {url}")
        
        # Get full text from yandex:full-text
        full_text = node.xpath('yandex:full-text/text()').get()
        if not full_text:
            logging.debug(f"No full text found for URL: {url}")
            return
        
        # Mark URL as processed
        self.processed_urls.add(url)
        logging.info(f"Processing new article: {url}")
        
        # Generate unique ID
        article_id = str(uuid.uuid4())
        
        # Parse HTML content
        soup = BeautifulSoup(full_text, 'html.parser')
        # Get all paragraphs
        paragraphs = soup.find_all('p')
        article_text = '\n'.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
        
        # Create article with required structure matching Note.md format
        article = NewsArticle()
        article['id'] = article_id
        article['text'] = article_text
        
        # Create metadata structure exactly as specified in Note.md
        article['metadata'] = {
            'source': 'tass',
            'published_at': int(dt.timestamp()),
            'published_at_iso': dt.isoformat(),
            'url': url,
            'header': node.xpath('title/text()').get(),
            'parsed_at': int(datetime.now().timestamp())
        }
        
        logging.info(f"Yielding article: {url} with ID: {article_id}")
        yield article

    def closed(self, reason):
        logging.info(f"TASS spider closed. Reason: {reason}")
        logging.info(f"Total URLs processed: {len(self.processed_urls)}") 