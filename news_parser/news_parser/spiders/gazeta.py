import scrapy
from datetime import datetime, timedelta
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import uuid
import logging
import re

class GazetaSpider(scrapy.Spider):
    name = 'gazeta'
    allowed_domains = ['gazeta.ru']
    start_urls = ['https://www.gazeta.ru/news/']
    
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

    def __init__(self, *args, **kwargs):
        super(GazetaSpider, self).__init__(*args, **kwargs)
        # Get today's and yesterday's dates for filtering
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        self.target_dates = [
            today.strftime('%Y-%m-%d'),
            yesterday.strftime('%Y-%m-%d')
        ]
        logging.info(f"Initializing Gazeta spider for dates: {self.target_dates}")

    def parse(self, response):
        logging.info(f"Parsing main page: {response.url}")
        # Save the first page HTML to a file
        # with open('gazeta_first_page.html', 'w', encoding='utf-8') as f:
        #     f.write(response.text)
        logging.info("Saved HTML to gazeta_first_page.html")
        
        # Find all news items - they are <a> tags with class "item"
        news_items = response.css('a.item[href*="/news/"]')
        logging.info(f"Found {len(news_items)} news items on the page")
        
        for item in news_items:
            # Get article link
            link = item.css('::attr(href)').get()
            if not link:
                continue
                
            url = response.urljoin(link)
            logging.info(f"Found article URL: {url}")
            
            # Get article title from div.item-text
            title = item.css('div.item-text::text').get()
            if not title:
                # Fallback to any text in the item
                title = item.css('::text').get()
            if not title:
                continue
            title = title.strip()
            logging.info(f"Found article title: {title}")
            
            # Get article time from time element
            time_text = item.css('time.time::text').get()
            if not time_text:
                # Try to get datetime attribute
                time_text = item.css('time.time::attr(datetime)').get()
            
            # Parse the date to check if it's from today or yesterday
            parsed_date = None
            
            if time_text:
                parsed_date = self.parse_date(time_text)
                if parsed_date:
                    logging.info(f"Parsed date from time text: {parsed_date}")
            
            # If we couldn't parse from time text, try to extract from URL
            if not parsed_date:
                parsed_date = self.extract_date_from_url(url)
                if parsed_date:
                    logging.info(f"Extracted date from URL: {parsed_date}")
            
            # If we still don't have a date, assume it's from today
            if not parsed_date:
                parsed_date = datetime.now()
                logging.info(f"Assuming article is from today: {parsed_date}")
            
            # Check if the date is from today or yesterday
            date_str = parsed_date.strftime('%Y-%m-%d')
            if date_str in self.target_dates:
                logging.info(f"Processing article from {date_str}: {url}")
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_article,
                    meta={
                        'time_text': time_text,
                        'url': url,
                        'title': title,
                        'parsed_date': parsed_date
                    }
                )
            else:
                logging.debug(f"Skipping article from {date_str} (not today or yesterday): {url}")

    def parse_date(self, date_text):
        """Parse various date formats from Gazeta.ru"""
        if not date_text:
            return None
            
        date_text = date_text.strip()
        
        # Try different date formats
        date_formats = [
            '%Y-%m-%dT%H:%M:%S%z',  # ISO format with timezone
            '%Y-%m-%dT%H:%M:%S',    # ISO format without timezone
            '%Y-%m-%d %H:%M:%S',    # Space-separated format
            '%d.%m.%Y %H:%M',       # Russian format
            '%d.%m.%Y',             # Russian date only
            '%Y-%m-%d',             # Date only
        ]
        
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_text, fmt)
                return parsed_date
            except ValueError:
                continue
        
        # Try to extract date from text patterns
        # Look for patterns like "сегодня 15:30", "вчера 14:20", "15 декабря 2023"
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
        
        # Handle time-only format (e.g., "10:37")
        time_match = re.match(r'^(\d{1,2}):(\d{2})$', date_text.strip())
        if time_match:
            hour, minute = map(int, time_match.groups())
            # Assume it's from today if only time is provided
            return today.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
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
        
        logging.warning(f"Could not parse date format: {date_text}")
        return None

    def extract_date_from_url(self, url):
        """Extract date from URL if available"""
        # Gazeta URLs often contain date: /news/2025/07/02/26175698.shtml
        date_match = re.search(r'/news/(\d{4})/(\d{2})/(\d{2})/', url)
        if date_match:
            year, month, day = map(int, date_match.groups())
            return datetime(year, month, day)
        return None

    def parse_article(self, response):
        time_text = response.meta['time_text']
        url = response.meta['url']
        title = response.meta['title']
        parsed_date = response.meta.get('parsed_date')
        
        logging.info(f"Parsing article: {url}")
        
        # Get article content - try multiple selectors including the correct one
        content_html = response.css('div.b_article-text').get()  # Correct class name
        if not content_html:
            content_html = response.css('div.article-text').get()  # Fallback
        if not content_html:
            content_html = response.css('div.article__text').get()  # Another fallback
        
        soup = BeautifulSoup(content_html or '', 'html.parser')
        paragraphs = soup.find_all('p')
        article_text = '\n'.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
        
        # Use parsed date if available, otherwise use current time
        if parsed_date:
            published_at = int(parsed_date.timestamp())
            published_at_iso = parsed_date.isoformat()
        else:
            published_at = int(datetime.now().timestamp())
            published_at_iso = datetime.now().isoformat()
        
        # Create article with required structure matching Note.md format
        article = NewsArticle()
        article['id'] = str(uuid.uuid4())
        article['text'] = article_text
        
        # Create metadata structure exactly as specified in Note.md
        article['metadata'] = {
            'source': 'gazeta',
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
        
        yield article 