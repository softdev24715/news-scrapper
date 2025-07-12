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
        
        # Generate dates for the last 7 days
        dates_to_try = []
        for i in range(7):
            date = datetime.now() - timedelta(days=i)
            dates_to_try.append(date.strftime('%Y-%m-%d'))
        
        logging.info(f"Looking for news from the last 7 days: {dates_to_try}")
        
        # Find all date headers and collect articles from the last 7 days
        all_news_items = []
        date_headers = soup.find_all('h2', class_='events__title')
        
        for header in date_headers:
            time_tag = header.find('time')
            if time_tag and time_tag.get('datetime'):
                header_date = time_tag.get('datetime')
                if header_date in dates_to_try:
                    logging.info(f"Found news block for date: {header_date}")
                    
                    # Collect all .hentry news items after this header until the next date header
                    news_items = []
                    for sib in header.find_next_siblings():
                        if sib.name == 'h2' and 'events__title' in sib.get('class', []):
                            break  # Stop at the next date header
                        if sib.name == 'div' and 'hentry' in sib.get('class', []):
                            news_items.append(sib)
                    
                    logging.info(f"Found {len(news_items)} news items for date: {header_date}")
                    all_news_items.extend(news_items)
        
        if not all_news_items:
            logging.info(f"No news blocks found for any of the dates: {dates_to_try}")
            return
            
        logging.info(f"Total news items found across 7 days: {len(all_news_items)}")

        # Process all collected news items
        for item in all_news_items:
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
        # Extract publication date from the article
        published_at = None
        published_at_iso = None
        
        # Try to get date from time element with itemprop="datePublished"
        time_tag = soup.find('time', {'itemprop': 'datePublished'})
        if time_tag and time_tag.get('datetime'):
            try:
                # Parse the datetime attribute (format: "2025-07-11")
                date_str = time_tag.get('datetime')
                if date_str and isinstance(date_str, str):
                    # Add time if not present (default to 00:00:00)
                    if 'T' not in date_str:
                        date_str += 'T00:00:00'
                    
                    dt = datetime.fromisoformat(date_str)
                    published_at = int(dt.timestamp())
                    published_at_iso = dt.isoformat()
                    logging.info(f"Parsed publication date from time element: {dt}")
                else:
                    raise ValueError(f"Invalid date string: {date_str}")
            except (ValueError, TypeError) as e:
                logging.warning(f"Could not parse date from time element: {e}")
                # Fallback to current time
                current_time = datetime.now()
                published_at = int(current_time.timestamp())
                published_at_iso = current_time.isoformat()
        else:
            # Fallback to current time if no date found
            current_time = datetime.now()
            published_at = int(current_time.timestamp())
            published_at_iso = current_time.isoformat()
            logging.warning("No publication date found in article, using current time")
        
        # Create article with required structure matching Note.md format
        article = NewsArticle()
        article['id'] = str(uuid.uuid4())
        article['text'] = article_text
        
        # Create metadata structure exactly as specified in Note.md
        article['metadata'] = {
            'source': 'kremlin',
            'published_at': published_at,
            'published_at_iso': published_at_iso,
            'url': response.url,
            'header': title,
            'parsed_at': int(datetime.now().timestamp())
        }
        
        logging.info(f"Yielding article: {response.url} with ID: {article['id']}")
        yield article 