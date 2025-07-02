import scrapy
from datetime import datetime, timedelta
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
        # Get today's and yesterday's dates
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        
        # Format dates for URL parameters (if needed)
        today_str = today.strftime('%d.%m.%Y')
        yesterday_str = yesterday.strftime('%d.%m.%Y')
        
        logging.info(f"Looking for documents from {yesterday_str} and {today_str}")
        
        # Start with the main page
        yield scrapy.Request(
            url='https://docs.eaeunion.org/',
            callback=self.parse_main_page,
            meta={'today': today_str, 'yesterday': yesterday_str}
        )
        
        # Also try to access search/filter pages if they exist
        # Try different possible search URLs
        search_urls = [
            'https://docs.eaeunion.org/search/',
            'https://docs.eaeunion.org/documents/',
            'https://docs.eaeunion.org/filter/',
        ]
        
        for search_url in search_urls:
            yield scrapy.Request(
                url=search_url,
                callback=self.parse_search_page,
                meta={'today': today_str, 'yesterday': yesterday_str, 'search_url': search_url}
            )

    def parse_main_page(self, response):
        """Parse the main page to find latest document links"""
        soup = BeautifulSoup(response.text, 'html.parser')
        
        logging.info(f"Parsing main page: {response.url}")
        
        # Save the page HTML for debugging
        # with open('eaeu_main_page.html', 'w', encoding='utf-8') as f:
        #     f.write(response.text)
        logging.info("Saved main page HTML to eaeu_main_page.html")
        
        # Look for document links - try different patterns
        doc_links = []
        
        # Pattern 1: Direct document links
        links = soup.find_all('a', href=re.compile(r'/documents/\d+/'))
        doc_links.extend(links)
        
        # Pattern 2: Links with document IDs
        links = soup.find_all('a', href=re.compile(r'/documents/'))
        doc_links.extend(links)
        
        # Pattern 3: Any links that might be documents
        links = soup.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            if '/documents/' in href or 'doc' in href.lower():
                doc_links.append(link)
        
        # Remove duplicates
        unique_links = []
        seen_urls = set()
        for link in doc_links:
            url = response.urljoin(link['href'])
            if url not in seen_urls:
                seen_urls.add(url)
                unique_links.append(link)
        
        logging.info(f"Found {len(unique_links)} unique document links from main page")
        
        # Visit each document detail page
        for link in unique_links[:10]:  # Increased limit to get more documents
            doc_url = response.urljoin(link['href'])
            logging.info(f"Will visit document URL: {doc_url}")
            yield scrapy.Request(
                url=doc_url,
                callback=self.parse_document,
                meta={'doc_url': doc_url, 'source': 'main_page'}
            )

    def parse_search_page(self, response):
        """Parse search/filter pages to find documents"""
        soup = BeautifulSoup(response.text, 'html.parser')
        
        logging.info(f"Parsing search page: {response.url}")
        
        # Save the search page HTML for debugging
        search_page_name = response.url.split('/')[-2] if response.url.split('/')[-2] else 'search'
        # with open(f'eaeu_{search_page_name}_page.html', 'w', encoding='utf-8') as f:
        #     f.write(response.text)
        logging.info(f"Saved search page HTML to eaeu_{search_page_name}_page.html")
        
        # Look for document links on search pages
        doc_links = []
        
        # Pattern 1: Direct document links
        links = soup.find_all('a', href=re.compile(r'/documents/\d+/'))
        doc_links.extend(links)
        
        # Pattern 2: Links with document IDs
        links = soup.find_all('a', href=re.compile(r'/documents/'))
        doc_links.extend(links)
        
        # Pattern 3: Any links that might be documents
        links = soup.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            if '/documents/' in href or 'doc' in href.lower():
                doc_links.append(link)
        
        # Remove duplicates
        unique_links = []
        seen_urls = set()
        for link in doc_links:
            url = response.urljoin(link['href'])
            if url not in seen_urls:
                seen_urls.add(url)
                unique_links.append(link)
        
        logging.info(f"Found {len(unique_links)} unique document links from search page")
        
        # Visit each document detail page
        for link in unique_links[:10]:  # Increased limit to get more documents
            doc_url = response.urljoin(link['href'])
            logging.info(f"Will visit document URL from search: {doc_url}")
            yield scrapy.Request(
                url=doc_url,
                callback=self.parse_document,
                meta={'doc_url': doc_url, 'source': 'search_page'}
            )

    def parse_document(self, response):
        """Parse individual document page to extract content"""
        soup = BeautifulSoup(response.text, 'html.parser')
        
        logging.info(f"Parsing document page: {response.url}")
        
        # Save the document page HTML for debugging
        # with open(f'eaeu_doc_{response.url.split("/")[-2]}.html', 'w', encoding='utf-8') as f:
        #     f.write(response.text)
        logging.info(f"Saved document HTML to eaeu_doc_{response.url.split('/')[-2]}.html")
        
        # Extract document title - look for specific title elements
        title = ""
        title_selectors = [
            'h1',
            'h2', 
            '.title',
            '.heading',
            '.document-title',
            '.doc-title',
            'title'
        ]
        
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                title = title_elem.get_text(strip=True)
                logging.info(f"Found title using '{selector}': {title}")
                break
        
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
        
        # Extract document content - try multiple approaches
        content = ""
        
        # Approach 1: Look for specific content areas
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
            '.document',
            '.doc-text',
            '.legal-content'
        ]
        
        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                # Remove navigation, menus, and other UI elements
                for elem in content_elem.find_all(['nav', 'menu', '.navigation', '.sidebar', '.menu', '.search', '.filter', 'script', 'style']):
                    elem.decompose()
                content = content_elem.get_text(separator=' ', strip=True)
                logging.info(f"Found content using selector '{selector}': {len(content)} chars")
                break
        
        # Approach 2: If no specific content area, try to extract from body with cleaning
        if not content:
            # Remove navigation and UI elements from the entire page
            for elem in soup.find_all(['nav', 'menu', '.navigation', '.sidebar', '.menu', '.search', '.filter', 'header', 'footer', 'script', 'style']):
                elem.decompose()
            
            # Try to get content from body
            body = soup.find('body')
            if body:
                content = body.get_text(separator=' ', strip=True)
                logging.info(f"Using body content: {len(content)} chars")
        
        # Clean up the content
        if content:
            # Remove common navigation and UI text
            content = re.sub(r'Отображать документы.*?Найти', '', content, flags=re.DOTALL)
            content = re.sub(r'Страницы:.*?След\.', '', content, flags=re.DOTALL)
            content = re.sub(r'Результаты: найдено\d+', '', content)
            content = re.sub(r'Дата опубликования документа.*?Номер документа', '', content, flags=re.DOTALL)
            content = re.sub(r'Скачать.*?pdf', '', content, flags=re.DOTALL)
            content = re.sub(r'Рус.*?Кыр', '', content, flags=re.DOTALL)
            
            # Clean up whitespace
            content = re.sub(r'\s+', ' ', content).strip()
        
        # Extract document date - look for multiple date patterns
        doc_date = ""
        date_patterns = [
            r'(\d{2}\.\d{2}\.\d{4})',  # DD.MM.YYYY
            r'(\d{4}-\d{2}-\d{2})',    # YYYY-MM-DD
            r'(\d{1,2}\s+[а-я]+\s+\d{4})',  # Russian date format
        ]
        
        for pattern in date_patterns:
            date_match = re.search(pattern, title or content)
            if date_match:
                doc_date = date_match.group(1)
                logging.info(f"Extracted date using pattern '{pattern}': {doc_date}")
                break
        
        # Generate unique ID
        doc_id = str(uuid.uuid4())
        
        # Convert date to timestamp
        published_at = int(datetime.now().timestamp())
        if doc_date:
            try:
                # Try different date formats
                if re.match(r'\d{2}\.\d{2}\.\d{4}', doc_date):
                    date_obj = datetime.strptime(doc_date, '%d.%m.%Y')
                elif re.match(r'\d{4}-\d{2}-\d{2}', doc_date):
                    date_obj = datetime.strptime(doc_date, '%Y-%m-%d')
                else:
                    # For Russian date format, we'll need more complex parsing
                    # For now, use current date
                    date_obj = datetime.now()
                
                published_at = int(date_obj.timestamp())
                logging.info(f"Converted date {doc_date} to timestamp: {published_at}")
            except Exception as e:
                logging.warning(f"Failed to parse date {doc_date}: {e}")
        
        # Check if document is from today or yesterday
        doc_datetime = datetime.fromtimestamp(published_at)
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        doc_date_only = doc_datetime.date()
        
        # Only process documents from today or yesterday
        if doc_date_only not in [today, yesterday]:
            logging.info(f"Skipping document from {doc_date_only} (not today or yesterday)")
            return
        
        logging.info(f"Processing document from {doc_date_only} (today: {today}, yesterday: {yesterday})")
        
        # If title is still generic, try to extract a better title from content
        if not title or title.lower() in ['документы', 'документ', 'documents']:
            # Look for document titles in the content
            title_match = re.search(r'(Решение ВЕЭС № \d+.*?)(?=\s|$)', content)
            if title_match:
                title = title_match.group(1).strip()
                logging.info(f"Extracted better title from content: {title}")
        
        # If we still don't have a good title, use the URL
        if not title:
            title = f"EAEU Document {doc_number or 'Unknown'}"
        
        logging.info(f"Final title: {title}")
        logging.info(f"Final doc number: {doc_number}")
        logging.info(f"Content length: {len(content)}")
        logging.info(f"Document date: {doc_date_only}")
        
        # Only yield if we have meaningful content
        if content and len(content) > 50:
            yield {
                'id': doc_id,
                'text': content[:5000] if content else title,  # Limit content length
                'lawMetadata': {
                    'originalId': doc_number,
                    'docKind': 'act',
                    'title': title,
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
        else:
            logging.warning(f"No meaningful content found for {response.url}") 