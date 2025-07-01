import scrapy
from datetime import datetime
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import uuid
import logging
import xml.etree.ElementTree as ET
import requests
import urllib3
import os

# Disable SSL warnings for testing
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class MeduzaSimpleSpider(scrapy.Spider):
    name = 'meduza'
    allowed_domains = ['meduza.io']
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'LOG_LEVEL': 'DEBUG',
        'DOWNLOAD_DELAY': 2,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
    }

    def start_requests(self):
        """Use simple requests to fetch RSS feed and extract data directly from it"""
        logging.info("Using simple requests to fetch Meduza RSS feed")
        
        # Temporarily unset proxy environment variables
        original_http_proxy = os.environ.get('HTTP_PROXY')
        original_https_proxy = os.environ.get('HTTPS_PROXY')
        
        try:
            # Unset proxy environment variables
            if 'HTTP_PROXY' in os.environ:
                del os.environ['HTTP_PROXY']
            if 'HTTPS_PROXY' in os.environ:
                del os.environ['HTTPS_PROXY']
            
            logging.info("Proxy environment variables unset for direct connection")
            
            # Use the exact same approach as our working test script
            url = 'https://meduza.io/rss/all'
            
            try:
                logging.info(f"Fetching RSS from: {url}")
                
                # Simple requests call - no session, no retry logic
                resp = requests.get(url, verify=False, timeout=30)
                
                logging.info(f"Response status: {resp.status_code}")
                logging.info(f"Content length: {len(resp.text)}")
                
                if resp.status_code == 200 and resp.text.strip():
                    logging.info("Successfully fetched RSS feed")
                    
                    # Save response for debugging
                    with open('meduza_rss_response.txt', 'w', encoding='utf-8') as f:
                        f.write(resp.text[:2000])
                    
                    # Parse RSS and extract articles directly
                    for article in self.parse_rss_feed(resp.text):
                        yield article
                else:
                    logging.error(f"Failed to fetch RSS: status {resp.status_code}")
                    
            except Exception as e:
                logging.error(f"Error fetching RSS: {e}")
                
        finally:
            # Restore proxy environment variables
            if original_http_proxy:
                os.environ['HTTP_PROXY'] = original_http_proxy
            if original_https_proxy:
                os.environ['HTTPS_PROXY'] = original_https_proxy
            logging.info("Proxy environment variables restored")

    def parse_rss_feed(self, rss_content):
        """Parse RSS feed and extract article data directly"""
        try:
            root = ET.fromstring(rss_content)
            items = root.findall('.//item')
            
            logging.info(f"Found {len(items)} items in RSS feed")
            
            for item in items:  # Limit to 10 articles for testing
                title_elem = item.find('title')
                link_elem = item.find('link')
                desc_elem = item.find('description')
                pub_date_elem = item.find('pubDate')
                guid_elem = item.find('guid')
                
                if link_elem is not None and link_elem.text:
                    url = link_elem.text.strip()
                    title = title_elem.text.strip() if title_elem is not None and title_elem.text else ''
                    description = desc_elem.text.strip() if desc_elem is not None and desc_elem.text else ''
                    pub_date = pub_date_elem.text.strip() if pub_date_elem is not None and pub_date_elem.text else ''
                    guid = guid_elem.text.strip() if guid_elem is not None and guid_elem.text else ''
                    
                    logging.info(f"Processing article: {title}")
                    
                    # Extract content from description (RSS feed often contains full content)
                    article_text = self.extract_content_from_description(description)
                    
                    # Parse publication date
                    published_at = self.parse_publication_date(pub_date)
                    
                    # Create article with required structure matching Note.md format
                    article = NewsArticle()
                    article['id'] = str(uuid.uuid4())
                    article['text'] = article_text
                    
                    # Create metadata structure exactly as specified in Note.md
                    article['metadata'] = {
                        'source': 'meduza',
                        'published_at': published_at,
                        'published_at_iso': datetime.fromtimestamp(published_at).isoformat(),
                        'url': url,
                        'header': title,
                        'parsed_at': int(datetime.now().timestamp())
                    }
                    
                    logging.info(f"Successfully extracted article: {title}")
                    yield article
                    
        except Exception as e:
            logging.error(f"Error parsing RSS feed: {e}")

    def extract_content_from_description(self, description):
        """Extract clean text content from RSS description"""
        if not description:
            return ''
        
        try:
            # Parse HTML description
            soup = BeautifulSoup(description, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Get text content
            text = soup.get_text()
            
            # Clean up whitespace
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = ' '.join(chunk for chunk in chunks if chunk)
            
            return text
        except Exception as e:
            logging.warning(f"Error extracting content from description: {e}")
            return description

    def parse_publication_date(self, pub_date):
        """Parse publication date from RSS"""
        if not pub_date:
            return int(datetime.now().timestamp())
        
        try:
            # Try to parse various date formats
            from dateutil import parser
            parsed_date = parser.parse(pub_date)
            return int(parsed_date.timestamp())
        except Exception as e:
            logging.warning(f"Error parsing publication date '{pub_date}': {e}")
            return int(datetime.now().timestamp())

    def parse(self, response):
        """Not used with RSS-only approach"""
        pass

    def parse_article(self, response):
        """Not used with RSS-only approach"""
        pass 