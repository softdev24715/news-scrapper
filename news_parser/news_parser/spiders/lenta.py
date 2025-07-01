import scrapy
from datetime import datetime
from scrapy.spiders import XMLFeedSpider
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import logging
import uuid

class LentaSpider(XMLFeedSpider):
    name = 'lenta'
    allowed_domains = ['lenta.ru']
    start_urls = ['https://lenta.ru/rss']
    iterator = 'iternodes'
    itertag = 'item'
    
    def __init__(self, *args, **kwargs):
        super(LentaSpider, self).__init__(*args, **kwargs)
        # Get today's date for filtering
        self.today = datetime.now().strftime('%Y-%m-%d')

    def parse_node(self, response, node):
        # Get publication date
        pub_date = node.xpath('pubDate/text()').get()
        if pub_date:
            # Convert pubDate to datetime
            dt = datetime.strptime(pub_date, '%a, %d %b %Y %H:%M:%S %z')
            # Check if article is from today
            if dt.strftime('%Y-%m-%d') != self.today:
                return
            
            # Generate unique ID
            article_id = str(uuid.uuid4())
            
            # Store article metadata
            article_meta = {
                'id': article_id,
                'source': 'lenta',
                'url': node.xpath('link/text()').get(),
                'header': node.xpath('title/text()').get(),
                'published_at': int(dt.timestamp()),
                'published_at_iso': dt.isoformat()
            }
            
            # Get article content
            article_url = article_meta['url']
            yield scrapy.Request(
                url=article_url,
                callback=self.parse_article,
                meta={'article_meta': article_meta}
            )

    def parse_article(self, response):
        article_meta = response.meta['article_meta']
        
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Get article text from the main content area
        article_text = []
        
        # Find the main content container
        content_div = soup.find('div', class_='topic-body__content')
        if content_div:
            # Get all paragraphs and other text elements
            for element in content_div.find_all(['p', 'h2', 'h3', 'h4']):
                # Skip elements with specific classes that we don't want
                if element.get('class') and any(cls in ['topic-body__tags', 'topic-body__main-image'] for cls in element.get('class')):
                    continue
                    
                text = element.get_text(strip=True)
                if text:
                    article_text.append(text)
        
        # Create article with required structure matching Note.md format
        article = NewsArticle()
        article['id'] = article_meta['id']
        article['text'] = '\n'.join(article_text)
        
        # Create metadata structure exactly as specified in Note.md
        article['metadata'] = {
            'source': article_meta['source'],
            'published_at': article_meta['published_at'],
            'published_at_iso': article_meta['published_at_iso'],
            'url': article_meta['url'],
            'header': article_meta['header'],
            'parsed_at': int(datetime.now().timestamp())
        }
        
        yield article 