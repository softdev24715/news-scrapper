import scrapy
from datetime import datetime, timezone, timedelta
from scrapy_playwright.page import PageCoroutine
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import logging
import uuid
import json
from playwright_extra import add_stealth_js
from playwright_extra.stealth import stealth_async

class ForbesPlaywrightSpider(scrapy.Spider):
    name = 'forbes_playwright'
    allowed_domains = ['forbes.ru']
    start_urls = ['https://www.forbes.ru/newrss.xml']
    
    # Class-level set to track processed URLs across all instances
    processed_urls = set()
    
    custom_settings = {
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_playwright.handler.PlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.PlaywrightDownloadHandler",
        },
        'TWISTED_REACTOR': 'twisted.internet.asyncio.AsyncioSelectorReactor',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
            'args': [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding',
                '--disable-features=TranslateUI',
                '--disable-ipc-flooding-protection',
                '--disable-default-apps',
                '--disable-extensions',
                '--disable-plugins',
                '--disable-sync',
                '--disable-translate',
                '--hide-scrollbars',
                '--mute-audio',
                '--no-default-browser-check',
                '--safebrowsing-disable-auto-update',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--disable-blink-features=AutomationControlled',
                '--disable-features=site-per-process',
                '--disable-site-isolation-trials',
                '--disable-features=TranslateUI',
                '--disable-ipc-flooding-protection',
                '--disable-default-apps',
                '--disable-extensions',
                '--disable-plugins',
                '--disable-sync',
                '--disable-translate',
                '--hide-scrollbars',
                '--mute-audio',
                '--no-default-browser-check',
                '--safebrowsing-disable-auto-update',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--disable-blink-features=AutomationControlled',
                '--disable-features=site-per-process',
                '--disable-site-isolation-trials',
            ]
        },
        'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 30000,
        'PLAYWRIGHT_DEFAULT_NAVIGATION_WAIT_UNTIL': 'networkidle',
        'PLAYWRIGHT_HEADLESS': True,
        'DOWNLOAD_DELAY': 5,
        'CONCURRENT_REQUESTS': 1,
        'ROBOTSTXT_OBEY': False,
        'COOKIES_ENABLED': True,
        'DOWNLOAD_TIMEOUT': 60,
        'RETRY_TIMES': 0,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': None,
            'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': None,
            'scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware': None,
        },
    }
    
    def __init__(self, *args, **kwargs):
        super(ForbesPlaywrightSpider, self).__init__(*args, **kwargs)
        # Get today's and yesterday's dates for filtering
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        self.target_dates = [
            today.strftime('%Y-%m-%d'),
            yesterday.strftime('%Y-%m-%d')
        ]
        logging.info(f"Initializing Forbes Playwright spider for dates: {self.target_dates}")
        logging.info(f"Current processed URLs count: {len(self.processed_urls)}")

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                errback=self.handle_error,
                dont_filter=True,
                meta={
                    'playwright': True,
                    'playwright_include_page': True,
                    'playwright_page_methods': [
                        PageCoroutine('wait_for_load_state', 'networkidle'),
                        PageCoroutine('wait_for_timeout', 3000),  # Wait 3 seconds
                    ],
                    'playwright_page_coroutines': [
                        self.apply_stealth,
                    ],
                    'dont_redirect': True,
                    'handle_httpstatus_list': [302, 403, 503],
                    'dont_merge_cookies': False,
                    'max_retry_times': 0
                }
            )

    async def apply_stealth(self, page):
        """Apply stealth mode to the page"""
        try:
            # Apply stealth mode
            await stealth_async(page)
            
            # Additional stealth measures
            await page.add_init_script("""
                // Override webdriver property
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                // Override plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
                
                // Override languages
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en'],
                });
                
                // Override permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
                
                // Override chrome runtime
                window.chrome = {
                    runtime: {},
                };
                
                // Override permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
            """)
            
            # Set realistic viewport
            await page.set_viewport_size({'width': 1920, 'height': 1080})
            
            # Set realistic user agent
            await page.set_extra_http_headers({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br, zstd',
                'Cache-Control': 'max-age=0',
                'Sec-Ch-Ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Linux"',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
            })
            
            logging.info("Stealth mode applied successfully")
            
        except Exception as e:
            logging.error(f"Error applying stealth mode: {e}")

    def handle_error(self, failure):
        logging.error(f"Request failed: {failure.value}")
        if hasattr(failure.value, 'response'):
            response = failure.value.response
            logging.error(f"HTTP Error: {response.status} for URL: {response.url}")
            logging.debug(f"Response headers: {response.headers}")
            logging.debug(f"Response body: {response.text[:1000]}")

    def parse(self, response):
        logging.debug(f"Parsing URL: {response.url}")
        logging.debug(f"Response status: {response.status}")
        logging.debug(f"Response headers: {response.headers}")
        
        # Get the page object
        page = response.meta.get('playwright_page')
        
        # Handle captcha redirect
        if response.status == 302:
            location = response.headers.get('Location', b'').decode('utf-8')
            if 'captcha' in location.lower():
                logging.error(f"Captcha detected! Redirect URL: {location}")
                logging.error("Forbes is blocking the scraper. Consider using a different approach.")
                if page:
                    page.close()
                return
            else:
                logging.warning(f"Unexpected redirect to: {location}")
                if page:
                    page.close()
                return
        
        # Check if we got a valid XML response
        if not response.text.strip().startswith('<?xml'):
            logging.error(f"Invalid XML response from {response.url}")
            logging.debug(f"Response content: {response.text[:1000]}")  # Log first 1000 chars for debugging
            if page:
                page.close()
            return
            
        # Process the XML feed
        for node in response.xpath('//item'):
            yield from self.parse_node(response, node)
        
        # Close the page
        if page:
            page.close()

    def parse_node(self, response, node):
        # Check if URL already processed
        url = node.xpath('link/text()').get()
        if not url or url in self.processed_urls:
            logging.debug(f"Skipping already processed or invalid URL: {url}")
            return
        
        # Mark URL as processed
        self.processed_urls.add(url)
        
        # Generate unique ID
        article_id = str(uuid.uuid4())
            
        # Get publication date
        pub_date = node.xpath('pubDate/text()').get()
        if not pub_date:
            logging.warning(f"No publication date found for URL: {url}")
            return
            
        # Parse the date
        try:
            dt = datetime.strptime(pub_date, '%a, %d %b %Y %H:%M:%S %z')
            date_str = dt.strftime('%Y-%m-%d')
            
            # Only process today's and yesterday's articles
            if date_str not in self.target_dates:
                logging.debug(f"Skipping article from {date_str} (not today or yesterday): {url}")
                return
                
            published_at = int(dt.timestamp())
            published_at_iso = dt.isoformat()
            
            logging.info(f"Processing article from {date_str}: {url}")
        except Exception as e:
            logging.error(f"Error parsing date {pub_date}: {e}")
            return
        
        # Get title
        title = node.xpath('title/text()').get()
        if not title:
            logging.warning(f"No title found for URL: {url}")
            return
            
        # Get content - try content field first, then fall back to description
        content = node.xpath('content/text()').get()
        if not content:
            content = node.xpath('description/text()').get()
            if not content:
                logging.warning(f"No content found for URL: {url}")
                return
            
        # Parse content with BeautifulSoup to clean it up
        soup = BeautifulSoup(content, 'html.parser')
        text_parts = []
        for p in soup.find_all('p'):
            text = p.get_text(strip=True)
            if text:
                text_parts.append(text)
                
        if not text_parts:
            logging.warning(f"No text content found in article: {url}")
            return
            
        # Create article with required structure matching Note.md format
        article = NewsArticle()
        article['id'] = article_id
        article['text'] = '\n'.join(text_parts)
        
        # Create metadata structure exactly as specified in Note.md
        article['metadata'] = {
            'source': 'forbes',
            'published_at': published_at,
            'published_at_iso': published_at_iso,
            'url': url,
            'header': title,
            'parsed_at': int(datetime.now().timestamp())
        }
        
        # Debug: Print found content
        logging.info(f"Processing article: {url} with ID: {article_id}")
        logging.info(f"Title found: {title}")
        logging.info(f"Text length: {len(article['text'])}")
        logging.info(f"Article date: {date_str}")
        
        yield article

    def closed(self, reason):
        logging.info(f"Forbes Playwright spider closed. Reason: {reason}")
        logging.info(f"Total URLs processed: {len(self.processed_urls)}") 