import scrapy
from datetime import datetime
import uuid
from bs4 import BeautifulSoup
import logging
import re

class EaeuSpider(scrapy.Spider):
    name = 'eaeu'
    allowed_domains = ['docs.eaeunion.org']
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 1,
    }

    def start_requests(self):
        """Start with the main page to find latest documents"""
        yield scrapy.Request(
            url='https://docs.eaeunion.org/',
            callback=self.parse_main_page
        )

    def parse_main_page(self, response):
        """Parse the main page to find latest document links"""
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find latest documents that came into force
        latest_docs = soup.find_all('div', class_=re.compile(r'latest|recent|document'))
        
        # Look for document links - EAEU uses /documents/{id}/ pattern
        doc_links = soup.find_all('a', href=re.compile(r'/documents/\d+/'))
        
        logging.info(f"Found {len(doc_links)} document links")
        
        # Visit each document detail page
        for link in doc_links[:10]:  # Limit to first 10 for testing
            doc_url = response.urljoin(link['href'])
            yield scrapy.Request(
                url=doc_url,
                callback=self.parse_document,
                meta={'doc_url': doc_url}
            )

    def parse_document(self, response):
        """Parse individual document page to extract content"""
        soup = BeautifulSoup(response.text, 'html.parser')
        
        logging.info(f"Parsing document page: {response.url}")
        
        # Extract document title - look for specific title elements
        title = ""
        title_elem = soup.find('h1') or soup.find('h2') or soup.find(class_=re.compile(r'title|heading'))
        if title_elem:
            title = title_elem.get_text(strip=True)
            logging.info(f"Found title: {title}")
        
        # Look for document number in the URL or page content
        doc_number = ""
        url_match = re.search(r'/documents/(\d+)/', response.url)
        if url_match:
            doc_number = url_match.group(1)
            logging.info(f"Extracted doc number from URL: {doc_number}")
        
        # If no number from URL, try to find it in the content
        if not doc_number:
            number_match = re.search(r'№\s*(\d+)', title or response.text)
            if number_match:
                doc_number = number_match.group(1)
                logging.info(f"Extracted doc number from content: {doc_number}")
        
        # Extract document content - look for main content areas
        content = ""
        
        # Try to find the main document content area
        content_selectors = [
            '.document-content',
            '.content',
            '.main-content',
            '.document-text',
            '.text-content',
            'article',
            'main',
            '.document-body',
            '.doc-content',
            '.document'
        ]
        
        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                # Remove navigation, menus, and other UI elements
                for elem in content_elem.find_all(['nav', 'menu', '.navigation', '.sidebar', '.menu', '.search', '.filter']):
                    elem.decompose()
                content = content_elem.get_text(separator=' ', strip=True)
                logging.info(f"Found content using selector '{selector}': {len(content)} chars")
                break
        
        # If no specific content area found, try to extract from the main body
        if not content:
            # Remove navigation and UI elements from the entire page
            for elem in soup.find_all(['nav', 'menu', '.navigation', '.sidebar', '.menu', '.search', '.filter', 'header', 'footer']):
                elem.decompose()
            
            # Try to get content from body
            body = soup.find('body')
            if body:
                content = body.get_text(separator=' ', strip=True)
                logging.info(f"Using body content: {len(content)} chars")
        
        # Clean up the content - remove excessive whitespace and navigation text
        if content:
            # Remove common navigation and UI text
            content = re.sub(r'Отображать документы.*?Найти', '', content, flags=re.DOTALL)
            content = re.sub(r'Страницы:.*?След\.', '', content, flags=re.DOTALL)
            content = re.sub(r'Результаты: найдено\d+', '', content)
            content = re.sub(r'Дата опубликования документа.*?Номер документа', '', content, flags=re.DOTALL)
            
            # Clean up whitespace
            content = re.sub(r'\s+', ' ', content).strip()
        
        # Extract document date
        doc_date = ""
        date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', title or content)
        if date_match:
            doc_date = date_match.group(1)
            logging.info(f"Extracted date: {doc_date}")
        
        # Generate unique ID
        doc_id = str(uuid.uuid4())
        
        # Convert date to timestamp
        published_at = int(datetime.now().timestamp())
        if doc_date:
            try:
                date_obj = datetime.strptime(doc_date, '%d.%m.%Y')
                published_at = int(date_obj.timestamp())
            except:
                pass
        
        # If title is still generic, try to extract a better title from content
        if not title or title.lower() in ['документы', 'документ', 'documents']:
            # Look for document titles in the content
            title_match = re.search(r'(Решение ВЕЭС № \d+.*?)(?=\s|$)', content)
            if title_match:
                title = title_match.group(1).strip()
                logging.info(f"Extracted better title from content: {title}")
        
        logging.info(f"Final title: {title}")
        logging.info(f"Final doc number: {doc_number}")
        logging.info(f"Content length: {len(content)}")
        
        yield {
            'id': doc_id,
            'text': content[:5000] if content else title,  # Limit content length
            'lawMetadata': {
                'originalId': doc_number,
                'docKind': 'act',
                'title': title or f"EAEU Document {doc_number}",
                'source': 'docs.eaeunion.org',
                'url': response.url,
                'publishedAt': published_at,
                'parsedAt': int(datetime.now().timestamp()),
                'jurisdiction': 'EAEU',
                'language': 'ru',
                'stage': None,
                'discussionPeriod': {
                    'start': None,
                    'end': None
                },
                'explanatoryNote': {
                    'fileId': None,
                    'url': None,
                    'mimeType': None
                },
                'summaryReports': [],
                'commentStats': {'total': 0}
            }
        } 