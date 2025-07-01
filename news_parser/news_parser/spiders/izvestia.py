import scrapy
from datetime import datetime, timedelta
from news_parser.items import NewsArticle
import re
from bs4 import BeautifulSoup
import uuid

class IzvestiaSpider(scrapy.Spider):
    name = 'izvestia'
    allowed_domains = ['iz.ru']
    start_urls = ['https://iz.ru/export/sitemap/last/xml']

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
        
        # Get today's date in YYYY-MM-DD format
        today = datetime.now().strftime('%Y-%m-%d')
        self.logger.info(f"Looking for articles from today: {today}")
        
        # Extract URLs with namespace handling
        # The structure is: <url><loc>article_url</loc><lastmod>date</lastmod><priority>priority</priority></url>
        urls_with_dates = response.xpath('//sm:url', namespaces=namespaces)
        
        self.logger.info(f"Found {len(urls_with_dates)} URL entries in sitemap")
        
        # Filter URLs by today's date and process them
        processed_count = 0
        for url_entry in urls_with_dates:
            loc = url_entry.xpath('sm:loc/text()', namespaces=namespaces).get()
            lastmod = url_entry.xpath('sm:lastmod/text()', namespaces=namespaces).get()
            
            if loc and lastmod and today in lastmod:
                self.logger.info(f"Found today's article: {loc} (date: {lastmod})")
                processed_count += 1
                yield scrapy.Request(url=loc, callback=self.parse_article_page)
                
                # Limit to first 20 articles for testing
                if processed_count >= 20:
                    break
        
        self.logger.info(f"Processed {processed_count} article URLs from today")
        
        # If no articles found for today, try yesterday
        if processed_count == 0:
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            self.logger.info(f"No articles found for today, trying yesterday: {yesterday}")
            
            for url_entry in urls_with_dates:
                loc = url_entry.xpath('sm:loc/text()', namespaces=namespaces).get()
                lastmod = url_entry.xpath('sm:lastmod/text()', namespaces=namespaces).get()
                
                if loc and lastmod and yesterday in lastmod:
                    self.logger.info(f"Found yesterday's article: {loc} (date: {lastmod})")
                    processed_count += 1
                    yield scrapy.Request(url=loc, callback=self.parse_article_page)
                    
                    # Limit to first 10 articles for testing
                    if processed_count >= 10:
                        break
            
            self.logger.info(f"Processed {processed_count} article URLs from yesterday")

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
        
        # Limit to first 10 articles for testing
        for url in list(article_urls)[:10]:
            yield scrapy.Request(url=url, callback=self.parse_article_page)

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

        # Get publication date
        pub_date_str = soup.select_one('time.article-header__date')
        if pub_date_str and pub_date_str.has_attr('datetime'):
            try:
                dt = datetime.fromisoformat(pub_date_str['datetime'])
                published_at = int(dt.timestamp())
                published_at_iso = dt.isoformat()
            except ValueError:
                # Fallback if parsing fails
                current_time = datetime.now()
                published_at = int(current_time.timestamp())
                published_at_iso = current_time.isoformat()
        else:
            current_time = datetime.now()
            published_at = int(current_time.timestamp())
            published_at_iso = current_time.isoformat()

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
        
        yield article 