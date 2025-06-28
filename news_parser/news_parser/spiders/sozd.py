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
        
        # Extract main content/text - target specific content areas
        content_text = ""
        
        logging.info(f"Parsing bill detail page: {response.url}")
        
        # First, try to find the explanatory note or main bill content
        # Look for specific content areas that contain the actual bill text
        content_selectors = [
            '.explanatory-note',
            '.bill-text',
            '.document-text',
            '.main-content',
            '.content-area',
            '.text-content',
            '.bill-content',
            '.document-content',
            'article',
            'main',
            '.content',
            '#content',
            '.document',
            '.text',
            '.description',
            '.explanatory',
            '.note'
        ]
        
        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                # Remove navigation, menus, and other UI elements
                for elem in content_elem.find_all(['nav', 'menu', '.navigation', '.sidebar', '.menu', '.search', '.filter', 'header', 'footer', '.help', '.info']):
                    elem.decompose()
                content_text = content_elem.get_text(separator=' ', strip=True)
                logging.info(f"Found content using selector '{selector}': {len(content_text)} chars")
                if len(content_text) > 100:  # Only use if we got substantial content
                    break
        
        # If no specific content area found, try to extract from specific sections
        if not content_text or len(content_text) < 100:
            # Look for explanatory note text specifically
            explanatory_text = ""
            for text_elem in soup.find_all(['p', 'div', 'span']):
                text = text_elem.get_text(strip=True)
                if 'ПОЯСНИТЕЛЬНАЯ ЗАПИСКА' in text or 'проекту федерального закона' in text:
                    # Get the parent element that contains the full explanatory note
                    parent = text_elem.parent
                    if parent:
                        explanatory_text = parent.get_text(separator=' ', strip=True)
                        logging.info(f"Found explanatory note: {len(explanatory_text)} chars")
                        break
            
            if explanatory_text:
                content_text = explanatory_text
        
        # If still no content, try to find any substantial text content
        if not content_text or len(content_text) < 100:
            # Look for any div or p elements with substantial text
            for elem in soup.find_all(['div', 'p']):
                text = elem.get_text(strip=True)
                if len(text) > 200 and ('закон' in text.lower() or 'федеральный' in text.lower() or 'проект' in text.lower()):
                    content_text = text
                    logging.info(f"Found substantial text content: {len(content_text)} chars")
                    break
        
        # If still no content, try to get text from body but clean it up
        if not content_text or len(content_text) < 100:
            body = soup.find('body')
            if body:
                # Remove all script, style, nav, header, footer elements
                for elem in body.find_all(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                    elem.decompose()
                content_text = body.get_text(separator=' ', strip=True)
                logging.info(f"Using body content: {len(content_text)} chars")
        
        # Clean up the content - remove excessive whitespace and navigation text
        if content_text:
            # Remove common navigation and UI text patterns
            content_text = re.sub(r'Система обеспечениязаконодательной деятельности.*?СОЗД.*?Объекты законотворчества', '', content_text, flags=re.DOTALL)
            content_text = re.sub(r'©Государственная Дума Федерального Собрания Российской Федерации.*?СОЗД ГАС.*?©ГД РФ', '', content_text, flags=re.DOTALL)
            content_text = re.sub(r'НаверхЗакрытьПредупреждение.*?Закрыть', '', content_text, flags=re.DOTALL)
            content_text = re.sub(r'Карточка законопроекта.*?Видеоописание функционала страницы', '', content_text, flags=re.DOTALL)
            
            # Clean up whitespace
            content_text = re.sub(r'\s+', ' ', content_text).strip()
            
            # If content is still too long, try to extract just the explanatory note
            if len(content_text) > 5000:
                explanatory_match = re.search(r'(ПОЯСНИТЕЛЬНАЯ ЗАПИСКА.*?)(?=©|Наверх|Карточка|$)', content_text, flags=re.DOTALL)
                if explanatory_match:
                    content_text = explanatory_match.group(1).strip()
        
        logging.info(f"Final content length: {len(content_text)}")
        if content_text:
            logging.info(f"Content preview: {content_text[:200]}...")
        else:
            logging.warning("No content extracted!")
        
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
                "discussionPeriod": {
                    "start": None,
                    "end": None
                },
                "explanatoryNote": {
                    "fileId": None,
                    "url": None,
                    "mimeType": None
                },
                "summaryReports": [],
                "commentStats": {"total": 0}
            }
        } 