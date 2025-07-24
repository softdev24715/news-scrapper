import scrapy
from datetime import datetime, timedelta
import uuid
from bs4 import BeautifulSoup
import logging
import re

class PravoSpider(scrapy.Spider):
    name = 'pravo'
    allowed_domains = ['publication.pravo.gov.ru']
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 1,
    }

    def start_requests(self):
        """Start with the documents page to find pagination and extract documents"""
        logging.info("Starting to fetch all Pravo documents")
        
        # Start with the first page to get pagination info
        yield scrapy.Request(
            url='http://publication.pravo.gov.ru/documents/daily?index=1&pageSize=200',
            callback=self.parse_documents_page,
            meta={'page': 1}
        )

    def parse_documents_page(self, response):
        """Parse documents page to extract pagination and document links"""
        soup = BeautifulSoup(response.text, 'html.parser')
        
        logging.info(f"Parsing documents page: {response.url}")
        
        # Save the page HTML for debugging
        # with open(f'pravo_page_{response.meta.get("page", 1)}.html', 'w', encoding='utf-8') as f:
        #     f.write(response.text)
        logging.info(f"Saved page HTML to pravo_page_{response.meta.get('page', 1)}.html")
        
        # Extract pagination info from the first page only
        if response.meta.get('page', 1) == 1:
            pagination_info = self.extract_pagination_info(soup)
            if pagination_info:
                first_page, last_page = pagination_info
                logging.info(f"Found pagination: first page = {first_page}, last page = {last_page}")
                
                # Generate requests for all pages
                for page_num in range(1, last_page + 1):  # Fetch all pages
                    page_url = f'http://publication.pravo.gov.ru/documents/daily?index={page_num}&pageSize=200'
                    logging.info(f"Will visit page {page_num}: {page_url}")
                    yield scrapy.Request(
                        url=page_url,
                        callback=self.parse_documents_page,
                        meta={'page': page_num}
                    )
        
        # Extract document items from current page
        document_items = self.extract_document_items(soup, response.url)
        logging.info(f"Found {len(document_items)} document items on page {response.meta.get('page', 1)}")
        
        # Process each document item
        for item in document_items:
            yield item

    def extract_pagination_info(self, soup):
        """Extract first and last page numbers from pagination"""
        # Look for pagination information in the page
        # The page shows "Показаны на странице: с 1 по 113 из 113"
        pagination_text = soup.get_text()
        
        # Find the pattern "с X по Y из Z"
        pagination_match = re.search(r'с\s+(\d+)\s+по\s+(\d+)\s+из\s+(\d+)', pagination_text)
        if pagination_match:
            start_item = int(pagination_match.group(1))
            end_item = int(pagination_match.group(2))
            total_items = int(pagination_match.group(3))
            
            # Calculate total pages based on pageSize=200
            page_size = 200
            total_pages = (total_items + page_size - 1) // page_size  # Ceiling division
            
            logging.info(f"Found pagination: items {start_item}-{end_item} of {total_items}, total pages: {total_pages}")
            return 1, total_pages
        
        # Fallback: look for any page numbers in the URL or links
        page_numbers = set()
        
        # Look for page links in the HTML
        all_links = soup.find_all('a', href=True)
        for link in all_links:
            href = link.get('href', '')
            # Look for index parameter
            page_match = re.search(r'index=(\d+)', href)
            if page_match:
                page_num = int(page_match.group(1))
                page_numbers.add(page_num)
                logging.info(f"Found page number from href: {page_num}")
        
        if page_numbers:
            first_page = 1
            last_page = max(page_numbers)
            logging.info(f"Found page numbers: {sorted(page_numbers)}, last page: {last_page}")
            return first_page, last_page
        
        # If no pagination found, assume it's a single page
        logging.warning("No pagination found, assuming single page")
        return 1, 1

    def extract_document_items(self, soup, page_url):
        """Extract document items from the page"""
        items = []
        
        # Find all document rows
        document_rows = soup.find_all('div', class_='documents-table-row')
        logging.info(f"Found {len(document_rows)} document rows")
        
        for doc_row in document_rows:
            try:
                item = self.parse_document_item(doc_row, page_url)
                if item:
                    items.append(item)
            except Exception as e:
                logging.error(f"Error parsing document item: {e}")
                continue
        
        return items

    def parse_document_item(self, doc_row, page_url):
        """Parse individual document item div"""
        # Extract document link and title
        link_elem = doc_row.find('a', class_='documents-item-name')
        if not link_elem:
            logging.warning("Document link not found")
            return None
        
        doc_url = link_elem.get('href')
        if not doc_url:
            logging.warning("Document URL not found")
            return None
        
        # Make URL absolute
        doc_url = f"http://publication.pravo.gov.ru{doc_url}"
        
        # Extract title (get all text content)
        title = link_elem.get_text(strip=True)
        
        # Extract publication number and date from info section
        info_div = doc_row.find('div', class_='infoindocumentlist')
        publication_number = ""
        publication_date = ""
        
        if info_div:
            # Extract publication number
            pub_num_elem = info_div.find('span', class_='info-data')
            if pub_num_elem:
                publication_number = pub_num_elem.get_text(strip=True)
            
            # Extract publication date
            date_elems = info_div.find_all('span', class_='info-data')
            if len(date_elems) >= 2:
                publication_date = date_elems[1].get_text(strip=True)
        
        # Extract document number from title
        doc_number = ""
        number_match = re.search(r'№\s*(\d+[-\w]*)', title)
        if number_match:
            doc_number = number_match.group(1)
        
        # Extract files information
        files = self.extract_files_info(doc_row)
        
        # Generate unique ID
        doc_id = str(uuid.uuid4())
        
        # Convert date to timestamp
        published_at = int(datetime.now().timestamp())
        if publication_date:
            try:
                date_obj = datetime.strptime(publication_date, '%d.%m.%Y')
                published_at = int(date_obj.timestamp())
                logging.info(f"Processing document from {date_obj.date()}")
            except Exception as e:
                logging.warning(f"Failed to parse date {publication_date}: {e}")
        else:
            logging.info(f"No date found for document: {title}, processing anyway")
        
        logging.info(f"Extracted document: {title}")
        logging.info(f"Document number: {doc_number}")
        logging.info(f"Publication number: {publication_number}")
        logging.info(f"Publication date: {publication_date}")
        
        return {
            'id': doc_id,
            'text': "",  # Use title as main text content
            'lawMetadata': {
                'originalId': publication_number or doc_number,
                'docKind': self.extract_doc_kind(title),
                'title': title,
                'source': 'publication.pravo.gov.ru',
                'url': doc_url,
                'publishedAt': published_at,
                'parsedAt': int(datetime.now().timestamp()),
                'jurisdiction': 'RU',
                'language': 'ru',
                'stage': 'опубликован',
                'discussionPeriod': None,
                'explanatoryNote': None,
                'summaryReports': None,
                'commentStats': None,
                'files': files
            }
        }

    def extract_files_info(self, doc_row):
        """Extract files information from document item"""
        files = []
        
        # Look for PDF download links
        pdf_links = doc_row.find_all('a', class_='documents-item-file')
        
        for link in pdf_links:
            href = link.get('href', '')
            if href.startswith('/'):
                href = f"http://publication.pravo.gov.ru{href}"
            
            # Extract file size from the link text
            size_text = link.get_text(strip=True)
            size_match = re.search(r'(\d+)\s*Kb', size_text)
            file_size = size_match.group(1) if size_match else "unknown"
            
            # Generate file ID from the eoNumber parameter
            eo_match = re.search(r'eoNumber=([^&]+)', href)
            file_id = eo_match.group(1) if eo_match else str(uuid.uuid4())[:8]
            
            file_info = {
                'fileId': file_id,
                'url': href,
                'mimeType': 'application/pdf'
            }
            
            files.append(file_info)
        
        logging.info(f"Extracted {len(files)} files")
        return files

    def extract_doc_kind(self, title):
        """Extract document kind from title"""
        if not title:
            return ''
            
        # Common document types in Russian legal system
        doc_types = [
            'Закон',
            'Постановление Правительства',
            'Распоряжение Правительства', 
            'Указ Президента',
            'Федеральный закон',
            'Приказ',
            'Постановление',
            'Распоряжение',
            'Указ Губернатора',
            'Постановление Губернатора',
            'Решение',
        ]
        
        for doc_type in doc_types:
            if doc_type in title:
                return doc_type
                
        return '' 