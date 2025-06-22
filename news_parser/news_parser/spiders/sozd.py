import scrapy
from datetime import datetime
import uuid
from bs4 import BeautifulSoup
import logging
import re

class SozdSpider(scrapy.Spider):
    name = 'sozd'
    allowed_domains = ['sozd.duma.gov.ru']
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 1,
    }

    def start_requests(self):
        """Start with the search page to find recent bills"""
        yield scrapy.Request(
            url='https://sozd.duma.gov.ru/search',
            callback=self.parse_search_page
        )

    def parse_search_page(self, response):
        """Parse the search page to find bill links"""
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find bill links
        bill_links = soup.find_all('a', href=re.compile(r'/bill/\d+'))
        
        logging.info(f"Found {len(bill_links)} bill links")
        
        # Visit each bill detail page
        for link in bill_links[:5]:  # Limit to first 5 for testing
            bill_url = response.urljoin(link['href'])
            yield scrapy.Request(
                url=bill_url,
                callback=self.parse_bill_detail,
                meta={'bill_url': bill_url}
            )

    def parse_bill_detail(self, response):
        """Parse individual bill detail page to extract content and metadata"""
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract bill number from URL
        bill_url = response.meta['bill_url']
        bill_number = bill_url.split('/')[-1] if '/' in bill_url else None
        
        # Extract title
        title_elem = soup.find('h1') or soup.find('title')
        title = title_elem.get_text(strip=True) if title_elem else None
        
        # Extract main content/text
        content_text = ""
        content_selectors = [
            '.bill-content', '.document-content', '.text-content',
            '.content', 'main', 'article', '.bill-text', '#content'
        ]
        
        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                content_text = content_elem.get_text(strip=True)
                break
        
        # If no specific content found, get all text from body
        if not content_text:
            body = soup.find('body')
            if body:
                content_text = body.get_text(strip=True)
        
        # Extract metadata
        registration_date = None
        stage = None
        
        # Look for registration date
        date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', response.text)
        if date_match:
            registration_date = date_match.group(1)
        
        # Look for stage
        stage_match = re.search(r'Стадия[:\s]*(.+)', response.text)
        if stage_match:
            stage = stage_match.group(1).strip()
        
        # Convert date to timestamp
        published_at = None
        if registration_date:
            try:
                date_obj = datetime.strptime(registration_date, '%d.%m.%Y')
                published_at = int(date_obj.timestamp())
            except ValueError:
                pass
        
        # Create the structured output with exact same keys as regulation.gov.ru
        yield {
            "id": str(uuid.uuid4()),
            "text": content_text,
            "lawMetadata": {
                "originalId": bill_number,
                "docKind": "bill",
                "title": title,
                "source": "sozd.duma.gov.ru",
                "url": bill_url,
                "publishedAt": published_at,
                "parsedAt": int(datetime.now().timestamp()),
                "jurisdiction": "RU",
                "language": "ru",
                "stage": stage,
                "discussionPeriod": None,
                "explanatoryNote": None,
                "summaryReports": [],
                "commentStats": None
            }
        } 