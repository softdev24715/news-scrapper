import scrapy
from datetime import datetime
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import uuid
import logging
import xml.etree.ElementTree as ET
import requests
import urllib3

# Disable SSL warnings for testing
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class MeduzaSpider(scrapy.Spider):
    name = 'meduza'
    allowed_domains = ['meduza.io']
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'LOG_LEVEL': 'DEBUG',
    }

    def start_requests(self):
        """Use requests to fetch RSS feed with permissive SSL"""
        logging.info("Using requests to fetch Meduza RSS feed")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
        }
        
        # Try multiple endpoints
        endpoints = [
            'https://meduza.io/rss/all',
            'https://meduza.io/api/v3/search?chrono=news&locale=ru&page=0&per_page=20',
            'https://meduza.io/api/v3/collections/news',
        ]
        
        for endpoint in endpoints:
            try:
                logging.info(f"Trying endpoint: {endpoint}")
                resp = requests.get(
                    endpoint, 
                    headers=headers, 
                    timeout=30, 
                    verify=False,  # Disable SSL verification
                    allow_redirects=True
                )
                
                if resp.status_code == 200 and resp.text.strip():
                    logging.info(f"Successfully fetched from {endpoint}")
                    with open(f'meduza_response_{endpoint.split("/")[-1]}.txt', 'w', encoding='utf-8') as f:
                        f.write(resp.text[:1000])  # Save first 1000 chars for debugging
                    
                    if 'rss' in endpoint:
                        # Parse RSS feed
                        for request in self.parse_rss_feed(resp.text):
                            yield request
                    else:
                        # Parse API response
                        for request in self.parse_api_response(resp.text):
                            yield request
                    break  # Success, stop trying other endpoints
                else:
                    logging.error(f"Failed to fetch {endpoint}: status {resp.status_code}")
                    
            except Exception as e:
                logging.error(f"Error fetching {endpoint}: {e}")
                continue
        
        if not hasattr(self, '_endpoints_tried'):
            logging.error("All endpoints failed")
            self._endpoints_tried = True

    def parse_rss_feed(self, rss_content):
        """Parse RSS feed and yield article requests"""
        try:
            root = ET.fromstring(rss_content)
            # Handle namespaces
            namespaces = {'rss': 'http://purl.org/rss/1.0/'}
            
            items = root.findall('.//item') or root.findall('.//rss:item', namespaces)
            
            for item in items[:10]:  # Limit to 10 articles for testing
                title_elem = item.find('title') or item.find('rss:title', namespaces)
                link_elem = item.find('link') or item.find('rss:link', namespaces)
                desc_elem = item.find('description') or item.find('rss:description', namespaces)
                pub_date_elem = item.find('pubDate') or item.find('rss:pubDate', namespaces)
                
                if link_elem is not None and link_elem.text:
                    url = link_elem.text.strip()
                    title = title_elem.text.strip() if title_elem is not None and title_elem.text else ''
                    description = desc_elem.text.strip() if desc_elem is not None and desc_elem.text else ''
                    pub_date = pub_date_elem.text.strip() if pub_date_elem is not None and pub_date_elem.text else ''
                    
                    logging.info(f"Found article: {title} - {url}")
                    
                    yield scrapy.Request(
                        url=url,
                        callback=self.parse_article,
                        meta={
                            'title': title,
                            'description': description,
                            'pub_date': pub_date
                        }
                    )
        except Exception as e:
            logging.error(f"Error parsing RSS feed: {e}")

    def parse_api_response(self, api_content):
        """Parse API response and yield article requests"""
        try:
            import json
            data = json.loads(api_content)
            
            # Try different possible structures
            documents = data.get('documents', {})
            if not documents:
                documents = data.get('collection', {}).get('documents', {})
            
            for doc_id, doc_data in list(documents.items())[:10]:  # Limit to 10 articles
                url = f"https://meduza.io/news/{doc_id}"
                title = doc_data.get('title', '')
                
                logging.info(f"Found article from API: {title} - {url}")
                
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_article,
                    meta={
                        'title': title,
                        'description': '',
                        'pub_date': ''
                    }
                )
        except Exception as e:
            logging.error(f"Error parsing API response: {e}")

    def parse(self, response):
        """Not used with requests approach"""
        pass

    def parse_article(self, response):
        """Parse individual article page"""
        url = response.url
        title = response.meta.get('title', '')
        description = response.meta.get('description', '')
        pub_date = response.meta.get('pub_date', '')
        
        logging.info(f"Parsing Meduza article: {url}")
        soup = BeautifulSoup(response.text, 'html.parser')
        
        if not hasattr(self, '_saved_article_html'):
            with open('meduza_article_debug.html', 'w', encoding='utf-8') as f:
                f.write(response.text)
            self._saved_article_html = True
        
        # Extract title
        extracted_title = title
        for selector in ['h1', '.headline', '.title', '.article-title', 'h2']:
            title_elem = soup.select_one(selector)
            if title_elem:
                extracted_title = title_elem.get_text(strip=True)
                if extracted_title:
                    break
        
        # Extract content
        article_text = ''
        for selector in ['.article-content', '.content', '.article-text', '.text', 'article']:
            content_elem = soup.select_one(selector)
            if content_elem:
                article_text = content_elem.get_text(strip=True)
                if article_text:
                    break
        
        # Use description if no content found
        if not article_text and description:
            desc_soup = BeautifulSoup(description, 'html.parser')
            article_text = desc_soup.get_text(strip=True)
        
        # Parse publication date
        published_at = int(datetime.now().timestamp())
        if pub_date:
            try:
                # Try to parse various date formats
                from dateutil import parser
                parsed_date = parser.parse(pub_date)
                published_at = int(parsed_date.timestamp())
            except:
                pass
        
        if extracted_title and article_text:
            article = NewsArticle()
            article['id'] = str(uuid.uuid4())
            article['title'] = extracted_title
            article['content'] = article_text
            article['url'] = url
            article['source'] = 'meduza'
            article['published_at'] = published_at
            article['scraped_at'] = int(datetime.now().timestamp())
            
            logging.info(f"Successfully extracted article: {extracted_title}")
            yield article
        else:
            logging.warning(f"Failed to extract content from {url}") 