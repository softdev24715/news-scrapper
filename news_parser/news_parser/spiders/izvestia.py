import scrapy
from datetime import datetime, timedelta
from news_parser.items import NewsArticle
import re
from bs4 import BeautifulSoup
import uuid
import logging

class IzvestiaSpider(scrapy.Spider):
    name = 'izvestia'
    allowed_domains = ['iz.ru']
    start_urls = ['https://iz.ru/export/sitemap/last/xml']

    def __init__(self, *args, **kwargs):
        super(IzvestiaSpider, self).__init__(*args, **kwargs)
        # Get today's and yesterday's dates for filtering
        today = datetime.now()  # ✅ Fixed: Use actual today
        yesterday = today - timedelta(days=1)
        self.target_dates = [
            today.strftime('%Y-%m-%d'),
            yesterday.strftime('%Y-%m-%d')
        ]
        logging.info(f"Initializing Izvestia spider for dates: {self.target_dates}")
        logging.info(f"Current time: {today}")
        logging.info(f"Yesterday time: {yesterday}")

    def start_requests(self):
        """
        Override start_requests to add custom headers for the sitemap XML
        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/xml,text/xml,application/xhtml+xml,text/html;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
        }
        
        for url in self.start_urls:
            yield scrapy.Request(url=url, headers=headers, callback=self.parse, errback=self.handle_error)

    def handle_error(self, failure):
        """
        Handle errors when accessing the sitemap
        """
        self.logger.error(f"Failed to access sitemap: {failure.value}")
        # Fallback to main news page if sitemap fails
        fallback_url = 'https://iz.ru/news'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        yield scrapy.Request(url=fallback_url, headers=headers, callback=self.parse_fallback)

    def parse(self, response):
        """
        This method parses the sitemap XML, extracts article URLs,
        and schedules them for scraping.
        """
        self.logger.info("Parsing sitemap XML")
        
        # Debug: Save the response content to see the structure
        
        # Define the namespace for the sitemap
        namespaces = {
            'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'
        }
        
        self.logger.info(f"Looking for articles from: {self.target_dates}")
        
        # Extract URLs with namespace handling
        # The structure is: <url><loc>article_url</loc><lastmod>date</lastmod><priority>priority</priority></url>
        urls_with_dates = response.xpath('//sm:url', namespaces=namespaces)
        
        self.logger.info(f"Found {len(urls_with_dates)} URL entries in sitemap")
        
        # Filter URLs by target dates and process them
        processed_count = 0
        for url_entry in urls_with_dates:
            loc = url_entry.xpath('sm:loc/text()', namespaces=namespaces).get()
            lastmod = url_entry.xpath('sm:lastmod/text()', namespaces=namespaces).get()
            
            if loc and lastmod:
                # Extract date from lastmod (format: 2025-07-02T10:30:00+03:00)
                entry_date = lastmod.split('T')[0] if 'T' in lastmod else lastmod[:10]
                
                # if entry_date in self.target_dates:
                self.logger.info(f"Found article from {entry_date}: {loc}")
                processed_count += 1
                yield scrapy.Request(
                    url=loc, 
                    callback=self.parse_article_page,
                    meta={'entry_date': entry_date}
                )
                    
                    # Limit to first 30 articles for testing
                    # if processed_count >= 30:
                    #     break
                # else:
                #     self.logger.debug(f"Skipping article from {entry_date} (not today or yesterday): {loc}")
        
        self.logger.info(f"Processed {processed_count} article URLs from target dates")

    def parse_fallback(self, response):
        """
        Fallback method to parse the main news page if sitemap fails
        """
        self.logger.info("Using fallback method - parsing main news page")
        
        # Try multiple selectors to find article links
        article_selectors = [
            'a[href*="/news/"]',
            'a[href*="/article/"]',
            '.news-item a',
            '.article-item a',
            'h2 a',
            'h3 a',
            '.title a'
        ]
        
        article_urls = set()  # Use set to avoid duplicates
        
        for selector in article_selectors:
            links = response.css(f'{selector}::attr(href)').getall()
            for link in links:
                if link and 'iz.ru' in link:
                    full_url = response.urljoin(link)
                    article_urls.add(full_url)
        
        self.logger.info(f"Found {len(article_urls)} article URLs in fallback")
        
        # Limit to first 15 articles for testing
        for url in list(article_urls)[:15]:
            yield scrapy.Request(
                url=url, 
                callback=self.parse_article_page,
                meta={'entry_date': datetime.now().strftime('%Y-%m-%d')}  # Use actual today for fallback
            )

    def parse_article_page(self, response):
        """
        This method parses an individual article page to extract detailed information.
        """
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Generate unique ID
        article_id = str(uuid.uuid4())
        
        # Basic article info
        source = 'iz.ru'
        url = response.url
        title_tag = soup.select_one('h1')
        title = title_tag.get_text(strip=True) if title_tag else None

        # Get publication date from meta tag
        pub_date_meta = soup.select_one('meta[property="article:published_time"]')
        article_date = None
        
        if pub_date_meta and pub_date_meta.has_attr('content'):
            datetime_attr = pub_date_meta['content']
            # Ensure it's a string, not a list
            if isinstance(datetime_attr, list):
                datetime_attr = datetime_attr[0] if datetime_attr else None
            
            if datetime_attr:
                try:
                    # Handle timezone format: 2025-07-08T08:00:00+03:00
                    if '+' in datetime_attr:
                        # Remove timezone for parsing
                        date_part = datetime_attr.split('+')[0]
                        dt = datetime.fromisoformat(date_part)
                    else:
                        dt = datetime.fromisoformat(datetime_attr)
                    
                    published_at = int(dt.timestamp())
                    published_at_iso = dt.isoformat()
                    article_date = dt.strftime('%Y-%m-%d')
                    self.logger.info(f"Parsed article date from meta tag: {article_date}")
                except ValueError as e:
                    # Fallback if parsing fails
                    current_time = datetime.now()
                    published_at = int(current_time.timestamp())
                    published_at_iso = current_time.isoformat()
                    article_date = current_time.strftime('%Y-%m-%d')
                    self.logger.warning(f"Could not parse date '{datetime_attr}' from meta tag: {e}, using current time")
            else:
                current_time = datetime.now()
                published_at = int(current_time.timestamp())
                published_at_iso = current_time.isoformat()
                article_date = current_time.strftime('%Y-%m-%d')
                self.logger.warning("No content attribute found in meta tag, using current time")
        else:
            # Fallback to time element if meta tag not found
            pub_date_str = soup.select_one('time.article-header__date')
            if pub_date_str and pub_date_str.has_attr('datetime'):
                datetime_attr = pub_date_str['datetime']
                if isinstance(datetime_attr, list):
                    datetime_attr = datetime_attr[0] if datetime_attr else None
                
                if datetime_attr:
                    try:
                        dt = datetime.fromisoformat(datetime_attr)
                        published_at = int(dt.timestamp())
                        published_at_iso = dt.isoformat()
                        article_date = dt.strftime('%Y-%m-%d')
                        self.logger.info(f"Parsed article date from time element: {article_date}")
                    except ValueError:
                        current_time = datetime.now()
                        published_at = int(current_time.timestamp())
                        published_at_iso = current_time.isoformat()
                        article_date = current_time.strftime('%Y-%m-%d')
                        self.logger.warning(f"Could not parse date '{datetime_attr}' from time element, using current time")
                else:
                    current_time = datetime.now()
                    published_at = int(current_time.timestamp())
                    published_at_iso = current_time.isoformat()
                    article_date = current_time.strftime('%Y-%m-%d')
                    self.logger.warning("No datetime attribute found in time element, using current time")
            else:
                current_time = datetime.now()
                published_at = int(current_time.timestamp())
                published_at_iso = current_time.isoformat()
                article_date = current_time.strftime('%Y-%m-%d')
                self.logger.warning("No date found in article (neither meta tag nor time element), using current time")

        # Check if the article date is from today or yesterday
        # if article_date not in self.target_dates:
        #     self.logger.debug(f"Skipping article from {article_date} (not today or yesterday): {url}")
        #     return

        # Get article text
        article_text = []
        content_containers = [
            'div.article-page__text',
            'div.text-article',
            'div[itemprop="articleBody"]',
            '.article-content',
            '.content'
        ]
        
        for container_selector in content_containers:
            content = soup.select_one(container_selector)
            if content:
                paragraphs = content.find_all('p')
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    if text:
                        article_text.append(text)
                break
        
        # Create article with required structure matching Note.md format
        article = NewsArticle()
        article['id'] = article_id
        article['text'] = '\n'.join(article_text)
        
        # Create metadata structure exactly as specified in Note.md
        article['metadata'] = {
            'source': source,
            'published_at': published_at,
            'published_at_iso': published_at_iso,
            'url': url,
            'header': title,
            'parsed_at': int(datetime.now().timestamp())
        }
        
        self.logger.info(f"Yielding article from {article_date}: {url}")
        self.logger.info(f"Title: {title}")
        self.logger.info(f"Text length: {len(article['text'])}")
        
        yield article 