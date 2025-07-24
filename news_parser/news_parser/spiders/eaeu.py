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
        """Start with the documents page to find pagination and extract documents"""
        # Get today's and yesterday's dates for filtering
        today = datetime.now()
        yesterday = today - timedelta(days=10)
        self.target_dates = [
            today.strftime('%Y-%m-%d'),
            yesterday.strftime('%Y-%m-%d')
        ]
        
        # Track pagination state
        self.stop_pagination = False
        
        logging.info(f"Starting to fetch EAEU documents from: {self.target_dates}")
        
        # Start with the first page to get pagination info
        yield scrapy.Request(
            url='https://docs.eaeunion.org/documents/?PAGEN_1=1',
            callback=self.parse_documents_page,
            meta={'page': 1}
        )

    def parse_documents_page(self, response):
        """Parse documents page to extract pagination and document links"""
        soup = BeautifulSoup(response.text, 'html.parser')
        
        logging.info(f"Parsing documents page: {response.url}")
        
        # Check if pagination is already stopped FIRST (before any processing)
        if self.stop_pagination:
            logging.info(f"Skipping page {response.meta.get('page', 1)} - pagination already stopped")
            return
        
        # Save the page HTML for debugging
        # with open(f'eaeu_page_{response.meta.get("page", 1)}.html', 'w', encoding='utf-8') as f:
        #     f.write(response.text)
        logging.info(f"Saved page HTML to eaeu_page_{response.meta.get('page', 1)}.html")
        
        # Extract pagination info from the first page only
        if response.meta.get('page', 1) == 1:
            pagination_info = self.extract_pagination_info(soup)
            if pagination_info:
                first_page, last_page = pagination_info
                self.pagination_info = (first_page, last_page)
                logging.info(f"Found pagination: first page = {first_page}, last page = {last_page}")
        
        # Extract document links from current page
        document_items = self.extract_document_items(soup, response.url)
        logging.info(f"Found {len(document_items)} document items on page {response.meta.get('page', 1)}")
        
        # Process each document item
        for i, item in enumerate(document_items):
            # Check if we should stop pagination based on document date
            if self.should_stop_pagination(item):
                self.stop_pagination = True
                logging.info(f"Found document older than yesterday, stopping pagination")
                # Don't yield this item and stop processing
                break
            
            yield item
        
        # Check if we should continue to next page
        if not self.stop_pagination:
            current_page = response.meta.get('page', 1)
            if self.pagination_info and current_page < self.pagination_info[1]:
                next_page = current_page + 1
                page_url = f'https://docs.eaeunion.org/documents/?PAGEN_1={next_page}'
                logging.info(f"Continuing to page {next_page}: {page_url}")
                
                from scrapy import Request
                yield Request(
                    url=page_url,
                    callback=self.parse_documents_page,
                    meta={'page': next_page}
                )

    def extract_pagination_info(self, soup):
        """Extract first and last page numbers from pagination dynamically"""
        pagination_div = soup.find('div', class_='modern-page-navigation')
        if not pagination_div:
            logging.warning("Pagination div not found")
            return None
        
        page_numbers = set()  # Use set to avoid duplicates
        
        # Method 1: Extract from all href attributes
        all_links = pagination_div.find_all('a', href=True)
        for link in all_links:
            href = link.get('href', '')
            # Look for PAGEN_1 parameter
            page_match = re.search(r'PAGEN_1=(\d+)', href)
            if page_match:
                page_num = int(page_match.group(1))
                page_numbers.add(page_num)
                logging.info(f"Found page number from href: {page_num}")
        
        # Method 2: Extract from text content of all elements
        all_elements = pagination_div.find_all(['a', 'span'])
        for element in all_elements:
            text = element.get_text(strip=True)
            # Check if text is a pure number (page number)
            if text.isdigit() and len(text) <= 4:  # Reasonable page number length
                page_num = int(text)
                page_numbers.add(page_num)
                logging.info(f"Found page number from text: {page_num}")
        
        # Method 3: Look for specific patterns in the entire pagination HTML
        pagination_html = str(pagination_div)
        # Find all numbers that could be page numbers
        all_numbers = re.findall(r'PAGEN_1=(\d+)', pagination_html)
        for num_str in all_numbers:
            page_num = int(num_str)
            page_numbers.add(page_num)
            logging.info(f"Found page number from HTML pattern: {page_num}")
        
        # Method 4: Look for numbers that appear to be page numbers in the text
        text_numbers = re.findall(r'\b(\d{1,4})\b', pagination_div.get_text())
        for num_str in text_numbers:
            page_num = int(num_str)
            # Filter out obviously non-page numbers (like years, small numbers that are likely not pages)
            if page_num > 0 and page_num <= 10000:  # Reasonable page number range
                page_numbers.add(page_num)
                logging.info(f"Found potential page number from text: {page_num}")
        
        if page_numbers:
            first_page = 1  # Always start from 1
            last_page = max(page_numbers)
            sorted_pages = sorted(page_numbers)
            logging.info(f"All found page numbers: {sorted_pages}")
            logging.info(f"Last page determined: {last_page}")
            logging.info(f"Total pages to visit: {last_page}")
            return first_page, last_page
        
        logging.warning("No page numbers found in pagination")
        return None

    def should_stop_pagination(self, item):
        """Check if we should stop pagination based on document date"""
        # Get the publishedAt timestamp from the item
        published_at = item.get('lawMetadata', {}).get('publishedAt')
        if not published_at:
            return False
        
        # Convert timestamp to date
        try:
            doc_date = datetime.fromtimestamp(published_at).date()
            yesterday = datetime.now().date() - timedelta(days=10)
            
            # If document is older than yesterday, stop pagination
            if doc_date < yesterday:
                logging.info(f"Document from {doc_date} is older than yesterday ({yesterday}), should stop pagination")
                return True
        except Exception as e:
            logging.warning(f"Error checking document date: {e}")
        
        return False

    def extract_document_items(self, soup, page_url):
        """Extract document items from the page"""
        items = []
        
        # Find the parent container
        parent_div = soup.find('div', class_='DocSearchResult_Items')
        if not parent_div:
            logging.warning("DocSearchResult_Items container not found")
            return items
        
        # Find all document items
        document_divs = parent_div.find_all('div', class_='DocSearchResult_Item')
        logging.info(f"Found {len(document_divs)} document divs")
        
        for doc_div in document_divs:
            try:
                item = self.parse_document_item(doc_div, page_url)
                if item:
                    items.append(item)
            except Exception as e:
                logging.error(f"Error parsing document item: {e}")
                continue
        
        return items

    def parse_document_item(self, doc_div, page_url):
        """Parse individual document item div"""
        # Extract document link
        link_elem = doc_div.find('a', class_='DocSearchResult_Item__Link')
        if not link_elem:
            logging.warning("Document link not found")
            return None
        
        doc_url = link_elem.get('href')
        if not doc_url:
            logging.warning("Document URL not found")
            return None
        
        # Make URL absolute
        doc_url = f"https://docs.eaeunion.org{doc_url}"
        
        # Extract title
        title = link_elem.get_text(strip=True)
        
        # Extract document text/description
        text_elem = doc_div.find('div', class_='DocSearchResult_Item__Text')
        description = text_elem.get_text(strip=True) if text_elem else ""
        
        # Extract dates
        dates_div = doc_div.find('div', class_='DocSearchResult_Item__Dates')
        doc_date = ""
        if dates_div:
            # Look for adoption date
            adoption_div = dates_div.find('div', string=re.compile(r'Дата принятия документа:'))
            if adoption_div:
                date_text = adoption_div.get_text(strip=True)
                date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', date_text)
                if date_match:
                    doc_date = date_match.group(1)
        
        # Extract document number from title
        doc_number = ""
        number_match = re.search(r'№\s*(\d+)', title)
        if number_match:
            doc_number = number_match.group(1)
        
        # Extract files information
        files = self.extract_files_info(doc_div)
        if not files:
            files = None
        
        # Process all documents regardless of date
        if doc_date:
            try:
                date_obj = datetime.strptime(doc_date, '%d.%m.%Y')
                logging.info(f"Processing document from {date_obj.date()}")
            except Exception as e:
                logging.warning(f"Failed to parse date {doc_date}: {e}")
                # Continue processing even if date parsing fails
        else:
            logging.info(f"No date found for document: {title}, processing anyway")
        
        # Generate unique ID
        doc_id = str(uuid.uuid4())
        
        # Convert date to timestamp
        published_at = int(datetime.now().timestamp())
        if doc_date:
            try:
                date_obj = datetime.strptime(doc_date, '%d.%m.%Y')
                published_at = int(date_obj.timestamp())
            except Exception as e:
                logging.warning(f"Failed to parse date {doc_date}: {e}")
        
        # Combine title and description for content
        content = f"{title}. {description}".strip()
        
        # Discussion period: always null for EAEU
        discussion_period = None
        # Explanatory note: always null for EAEU
        explanatory_note = None
        # Summary reports: always null for EAEU
        summary_reports = None
        # Comment stats: always null for EAEU
        comment_stats = None
        
        logging.info(f"Extracted document: {title}")
        logging.info(f"Document number: {doc_number}")
        logging.info(f"Document date: {doc_date}")
        logging.info(f"Content length: {len(content)}")
        
        return {
            'id': doc_id,
            'text': content[:5000] if content else title,  # Limit content length
            'lawMetadata': {
                'originalId': doc_number if doc_number else None,
                'docKind': 'act',
                'title': title if title else None,
                'source': 'docs.eaeunion.org',
                'url': doc_url,
                'publishedAt': published_at if published_at else None,
                'parsedAt': int(datetime.now().timestamp()),
                'jurisdiction': 'EAEU',
                'language': 'ru',
                'stage': 'принято',
                'discussionPeriod': discussion_period,
                'explanatoryNote': explanatory_note,
                'summaryReports': summary_reports,
                'commentStats': comment_stats,
                'files': files
            }
        }

    def extract_files_info(self, doc_div):
        """Extract files information from document item"""
        files = []
        
        # Find the files section
        files_div = doc_div.find('div', class_='DocSearchResult_Item__Files')
        if not files_div:
            return files
        
        # Find all file groups
        file_groups = files_div.find_all('div', class_='DocDetail_Files_Group')
        
        for group in file_groups:
            # Check if this is an "Applications" group
            group_title = group.find('div', class_='DocDetail_Files_Title')
            is_application = group_title and 'Приложения' in group_title.get_text(strip=True)
            
            # Find all file items in this group
            file_items = group.find_all('div', class_='DocSearchResult_Item__File')
            
            for file_item in file_items:
                file_info = {}
                
                # Extract file name from the link
                file_link = file_item.find('a', href=True)
                if file_link:
                    file_url = file_link.get('href', '')
                    if file_url.startswith('/'):
                        file_url = f"https://docs.eaeunion.org{file_url}"
                    
                    # Extract file name from URL
                    file_name = file_url.split('/')[-1] if file_url else "unknown"
                    
                    # Determine mime type based on file extension
                    mime_type = "application/octet-stream"  # default
                    if file_name.lower().endswith('.pdf'):
                        mime_type = "application/pdf"
                    elif file_name.lower().endswith('.zip'):
                        mime_type = "application/zip"
                    elif file_name.lower().endswith('.doc') or file_name.lower().endswith('.docx'):
                        mime_type = "application/msword"
                    elif file_name.lower().endswith('.xls') or file_name.lower().endswith('.xlsx'):
                        mime_type = "application/vnd.ms-excel"
                    
                    # Generate a file ID (using part of the URL hash)
                    file_id = file_url.split('/')[-1].split('.')[0] if file_url else str(uuid.uuid4())[:8]
                    
                    file_info = {
                        'fileId': file_id,
                        'url': file_url,
                        'mimeType': mime_type
                    }
                    
                    files.append(file_info)
        
        logging.info(f"Extracted {len(files)} files")
        return files

    def parse_main_page(self, response):
        """Legacy method - kept for compatibility but not used"""
        pass

    def parse_search_page(self, response):
        """Legacy method - kept for compatibility but not used"""
        pass

    def parse_document(self, response):
        """Legacy method - kept for compatibility but not used"""
        pass 