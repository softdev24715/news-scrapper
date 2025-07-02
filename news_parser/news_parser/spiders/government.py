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
    
    def __init__(self, *args, **kwargs):
        super(GovernmentSpider, self).__init__(*args, **kwargs)
        # Get today's and yesterday's dates for filtering
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        self.target_dates = [
            today.strftime('%Y-%m-%d'),
            yesterday.strftime('%Y-%m-%d')
        ]
        logging.info(f"Initializing Government spider for dates: {self.target_dates}")
    
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
            urls.append((url, date_str, date))
        
        for url, date_str, date_obj in urls:
            logging.info(f"Will fetch government news for {date_str}: {url}")
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={'date': date_str, 'date_obj': date_obj},
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

    def parse_date(self, date_text, datetime_str=None):
        """Parse various date formats from government.ru"""
        if not date_text and not datetime_str:
            return None
            
        # Try datetime attribute first (most reliable)
        if datetime_str:
            try:
                # Parse datetime like "2025-06-20T19:00:00+04:00"
                dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
                logging.info(f"Parsed date from datetime attribute: {dt}")
                return dt
            except ValueError:
                logging.warning(f"Could not parse datetime attribute: {datetime_str}")
        
        # Try parsing date text
        if date_text:
            date_text = date_text.strip()
            
            # Try different date formats
            date_formats = [
                '%d.%m.%Y %H:%M',       # Russian format with time
                '%d.%m.%Y',             # Russian date only
                '%Y-%m-%d %H:%M:%S',    # ISO format
                '%Y-%m-%d',             # Date only
            ]
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(date_text, fmt)
                    logging.info(f"Parsed date from text using format '{fmt}': {parsed_date}")
                    return parsed_date
                except ValueError:
                    continue
            
            # Try to extract date from text patterns
            today = datetime.now()
            yesterday = today - timedelta(days=1)
            
            # Russian relative dates
            if 'сегодня' in date_text.lower():
                # Extract time if present
                time_match = re.search(r'(\d{1,2}):(\d{2})', date_text)
                if time_match:
                    hour, minute = map(int, time_match.groups())
                    return today.replace(hour=hour, minute=minute, second=0, microsecond=0)
                return today
            
            if 'вчера' in date_text.lower():
                # Extract time if present
                time_match = re.search(r'(\d{1,2}):(\d{2})', date_text)
                if time_match:
                    hour, minute = map(int, time_match.groups())
                    return yesterday.replace(hour=hour, minute=minute, second=0, microsecond=0)
                return yesterday
            
            # Russian month names
            russian_months = {
                'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
                'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
                'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
            }
            
            for month_name, month_num in russian_months.items():
                if month_name in date_text.lower():
                    # Pattern: "15 декабря 2023" or "15 декабря"
                    match = re.search(r'(\d{1,2})\s+' + month_name + r'\s*(\d{4})?', date_text)
                    if match:
                        day = int(match.group(1))
                        year = int(match.group(2)) if match.group(2) else today.year
                        return datetime(year, month_num, day)
        
        logging.warning(f"Could not parse date: text='{date_text}', datetime='{datetime_str}'")
        return None

    def parse(self, response):
        date_str = response.meta.get('date', 'unknown')
        date_obj = response.meta.get('date_obj')
        logging.info(f"Parsing government news for date: {date_str}")
        
        # Save the HTML for inspection
        # with open(f'government_news_{date_str.replace(".", "_")}.html', 'w', encoding='utf-8') as f:
        #     f.write(response.text)
        logging.info(f"Saved HTML to government_news_{date_str.replace('.', '_')}.html")
        
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
            
            # Parse the date
            parsed_date = self.parse_date(date_text, datetime_attr)
            if not parsed_date:
                # If we can't parse the date, use the date from the URL
                parsed_date = date_obj
                logging.info(f"Using date from URL for article: {parsed_date}")
            
            # Check if the date is from today or yesterday
            date_str_check = parsed_date.strftime('%Y-%m-%d')
            if date_str_check not in self.target_dates:
                logging.debug(f"Skipping article from {date_str_check} (not today or yesterday)")
                continue
            
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
                'url': article_url,
                'parsed_date': parsed_date
            }
            
            news_items.append(news_item)
            logging.info(f"Found news item from {date_str_check}: {title[:50]}...")
        
        logging.info(f"Found {len(news_items)} relevant news items for date {date_str}")
        
        # Process each news item
        for item in news_items:
            yield scrapy.Request(
                url=item['url'],
                callback=self.parse_article,
                meta={
                    'date': item['date'],
                    'datetime': item['datetime'],
                    'title': item['title'],
                    'url': item['url'],
                    'parsed_date': item['parsed_date']
                }
            )

    def parse_article(self, response):
        date = response.meta.get('date', '')
        datetime_str = response.meta.get('datetime', '')
        title = response.meta.get('title', '')
        url = response.meta.get('url', response.url)
        parsed_date = response.meta.get('parsed_date')
        
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
        
        # Use parsed date if available, otherwise parse datetime string
        if parsed_date:
            published_at = int(parsed_date.timestamp())
            published_at_iso = parsed_date.isoformat()
        elif datetime_str:
            try:
                # Parse datetime like "2025-06-20T19:00:00+04:00"
                dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
                published_at = int(dt.timestamp())
                published_at_iso = dt.isoformat()
            except ValueError:
                logging.warning(f"Could not parse datetime: {datetime_str}")
                published_at = int(datetime.now().timestamp())
                published_at_iso = datetime.now().isoformat()
        else:
            published_at = int(datetime.now().timestamp())
            published_at_iso = datetime.now().isoformat()
        
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
        
        # Log the date information
        if parsed_date:
            date_str = parsed_date.strftime('%Y-%m-%d')
            logging.info(f"Yielding article from {date_str}: {url} with ID: {article['id']}")
        else:
            logging.info(f"Yielding article (date unknown): {url} with ID: {article['id']}")
        
        logging.info(f"Article text length: {len(article_text)} characters")
        yield article 