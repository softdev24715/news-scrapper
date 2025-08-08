import scrapy
import json
import logging
from datetime import datetime
import html
import os
from news_parser.items import LegalDocument
import uuid

class CNTDSpider(scrapy.Spider):
    name = 'cntd'
    allowed_domains = ['docs.cntd.ru', 'api.docs.rz']
    
    def __init__(self, *args, **kwargs):
        super(CNTDSpider, self).__init__(*args, **kwargs)
        self.base_search_url = 'https://docs.cntd.ru/api/search'
        self.category = kwargs.get('category', '3')  # Default category
        self.date = kwargs.get('date', None)  # Optional date parameter (e.g., '2025')
        
        # Batch processing parameters
        self.start_page = int(kwargs.get('start_page', 1))
        self.end_page = int(kwargs.get('end_page', 3))
        self.batch_size = int(kwargs.get('batch_size', 10))
        
        # Round-robin pages parameter
        pages_param = kwargs.get('pages', '')
        if pages_param:
            # Parse specific pages from comma-separated string
            self.specific_pages = [int(p.strip()) for p in pages_param.split(',') if p.strip()]
            logging.info(f"Using specific pages: {self.specific_pages}")
        else:
            self.specific_pages = None
        
        # Setup error logging
        self.setup_error_logging()
        
        # Log configuration
        if self.date:
            logging.info(f"Initializing CNTD spider with category={self.category}, date={self.date}, pages {self.start_page}-{self.end_page}")
        else:
            logging.info(f"Initializing CNTD spider with category={self.category}, pages {self.start_page}-{self.end_page}")

    def build_search_url(self, page):
        """Build search URL with optional date parameter"""
        base_params = f"order_by=registration_date:desc&category={self.category}&page={page}"
        
        if self.date:
            # URL with date parameter
            search_url = f"{self.base_search_url}?{base_params}&date={self.date}"
        else:
            # URL without date parameter
            search_url = f"{self.base_search_url}?{base_params}"
        
        return search_url

    def setup_error_logging(self):
        """Setup error logging for failed documents"""
        # Create logs directory if it doesn't exist
        logs_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        
        # Create error log file with timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        self.error_log_file = os.path.join(logs_dir, f'cntd_failed_docs_{timestamp}.log')
        
        # Create error logger
        self.error_logger = logging.getLogger(f'cntd_errors_{timestamp}')
        self.error_logger.setLevel(logging.INFO)
        
        # Create file handler
        file_handler = logging.FileHandler(self.error_log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # Add handler to logger
        self.error_logger.addHandler(file_handler)
        
        logging.info(f"Error logging setup: {self.error_log_file}")

    def log_failed_document(self, doc_id, error_message, item_data=None):
        """Log failed document with details for later retry"""
        error_entry = {
            'doc_id': doc_id,
            'error': error_message,
            'timestamp': datetime.now().isoformat(),
            'batch': f"{self.start_page}-{self.end_page}",
            'category': self.category,
            'date': self.date
        }
        
        if item_data:
            error_entry['item_data'] = item_data
        
        # Log as JSON for easy parsing
        self.error_logger.info(json.dumps(error_entry, ensure_ascii=False))
        
        # Also log to console for immediate feedback
        logging.error(f"Failed document {doc_id}: {error_message}")

    def start_requests(self):
        """Start with the specified pages"""
        if self.specific_pages:
            # Use round-robin specific pages
            logging.info(f"Starting CNTD spider with specific pages: {self.specific_pages}")
            for page in self.specific_pages:
                search_url = self.build_search_url(page)
                logging.info(f"Queuing specific page {page}: {search_url}")
                
                yield scrapy.Request(
                    url=search_url,
                    callback=self.parse_search_results,
                    meta={'page': page}
                )
        else:
            # Use sequential page range (backward compatibility)
            logging.info(f"Starting CNTD spider batch for pages {self.start_page}-{self.end_page}")
            
            # Generate requests for all pages in the batch
            for page in range(self.start_page, self.end_page + 1):
                search_url = self.build_search_url(page)
                logging.info(f"Queuing page {page}: {search_url}")
                
                yield scrapy.Request(
                    url=search_url,
                    callback=self.parse_search_results,
                    meta={'page': page}
                )

    def parse_search_results(self, response):
        """Parse search results and request individual document pages"""
        try:
            data = json.loads(response.text)
            current_page = response.meta['page']
            logging.info(f"Parsing search results for page {current_page}")
            
            if 'data' not in data or not data['data']:
                logging.info(f"No data found on page {current_page} - batch complete")
                return
            
            documents = data['data']
            logging.info(f"Found {len(documents)} documents on page {current_page}")
            
            # Process each document
            for doc in documents:
                # Convert Unicode escape sequences to readable text
                converted_doc = self.convert_unicode_recursively(doc)
                
                # Log the first document with converted text
                if doc == documents[0]:
                    logging.info(f"Sample document on page {current_page} - ID: {converted_doc.get('id')}")
                    if 'names' in converted_doc and converted_doc['names']:
                        logging.info(f"Converted names: {converted_doc['names']}")
                
                # Request individual document page to get full text
                doc_id = converted_doc.get('id')
                if doc_id:
                    doc_url = f"https://docs.cntd.ru/document/{doc_id}"
                    logging.info(f"Requesting document page: {doc_url}")
                    
                    # Add page information to the search data
                    converted_doc['page_id'] = current_page
                    logging.info(f"Added page_id {current_page} to converted_doc for document {doc_id}")
                    
                    yield scrapy.Request(
                        url=doc_url,
                        callback=self.parse_document,
                        meta={'search_data': converted_doc}
                    )
                
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON from search results on page {response.meta['page']}: {e}")
        except Exception as e:
            logging.error(f"Error parsing search results on page {response.meta['page']}: {e}")

    def parse_document(self, response):
        """Individual document page to extract full text"""
        try:
            search_data = response.meta['search_data']
            doc_id = search_data.get('id')
            
            logging.info(f"Parsing document page for ID: {doc_id}")
            
            # Extract full text from document-content div
            full_text = ""
            
            # Find the main document-content div
            content_div = response.css('div.document-content')
            if content_div:
                # Find all textBlock1 and document-text_block divs
                text_blocks = content_div.css('div.textBlock1, div.document-text_block')
                
                for block in text_blocks:
                    # Extract all p tags text from each block
                    paragraphs = block.css('p::text').getall()
                    for p_text in paragraphs:
                        p_text = p_text.strip()
                        if p_text:
                            # Check if this paragraph starts with the disclaimer
                            if p_text.startswith('Электронный текст документа'):
                                logging.info(f"Found disclaimer, stopping text extraction for document {doc_id}")
                                break  # Stop including any more paragraphs
                            full_text += p_text + "\n"      
            # Clean up the text
            full_text = full_text.strip()
            
            if not full_text:
                logging.warning(f"No text content found for document {doc_id}")
                full_text = ""  # Save empty string instead of placeholder text
            
            logging.info(f"Extracted {len(full_text)} characters of text for document {doc_id}")
            
            # Create the final item with the specified structure
            item = self.create_final_item(search_data, full_text, response.url)
            yield item
            
        except Exception as e:
            logging.error(f"Error parsing document {doc_id}: {e}")

    def create_final_item(self, search_data, full_text, url):
        """Create the final item with the specified structure"""
        # Extract title from names field
        title = ""
        if 'names' in search_data and search_data['names']:
            title = search_data['names'][0]
        
        # Generate requisites from registration data
        requisites = self.generate_requisites(search_data)
        
        # Extract published_at_iso from registration data
        published_at_iso = None
        if 'registrations' in search_data and search_data['registrations']:
            reg = search_data['registrations'][0]
            date_str = reg.get('date')
            if date_str:
                try:
                    # Parse the date string to DateTime object
                    published_at_iso = datetime.strptime(date_str, '%Y-%m-%d')
                except ValueError:
                    logging.warning(f"Could not parse date: {date_str}")
                    published_at_iso = None
        
        # Get page information from meta
        page_number = search_data.get('page_id', None)
        doc_id = search_data.get('id')
        
        # Debug logging for page_number
        logging.info(f"Creating item for doc_id: {doc_id}, page_number: {page_number}")
        logging.info(f"search_data keys: {list(search_data.keys())}")
        if 'page_id' in search_data:
            logging.info(f"page_id value: {search_data['page_id']}")
        
        # Create the item with the exact structure you specified
        item = {
            'id': str(uuid.uuid4()),  # Generate unique UUID
            'doc_id': str(doc_id) if doc_id else "",  # Original CNTD document ID
            'page_number': page_number,  # Page number from API
            'title': title,
            'requisites': requisites,
            'text': full_text,
            'url': url,
            'parsed_at': int(datetime.now().timestamp()),
            'published_at_iso': published_at_iso,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        # Debug logging for final item
        logging.info(f"Final item page_number: {item['page_number']}")
        
        return item

    def generate_requisites(self, search_data):
        """Generate requisites string from registration data"""
        requisites = ""
        if 'registrations' in search_data and search_data['registrations']:
            reg = search_data['registrations'][0]  # Take the first registration
            
            # Extract components
            doctype_name = ""
            if 'doctype' in reg and reg['doctype'] and 'name' in reg['doctype'] and reg['doctype']['name']:
                doctype_name = reg['doctype']['name']
            
            date = reg.get('date')
            number = reg.get('number')
            # Convert date to Russian format (e.g., "155.")
            if date:
                try:
                    date_obj = datetime.strptime(date, '%Y-%m-%d')
                    # Russian month names
                    months = {
                        1: 'января', 2: 'февраля', 3: 'марта', 4: 'апреля',
                        5: 'мая', 6: 'июня', 7: 'июля', 8: 'августа',
                        9: 'сентября', 10: 'октября', 11: 'ноября', 12: 'декабря'
                    }
                    month_name = months[date_obj.month]
                    date_str = f"{date_obj.day} {month_name} {date_obj.year} г."
                except:
                    date_str = date
            else:
                date_str = ""
            
            # Build requisites string
            if doctype_name and date_str:
                requisites = f"{doctype_name} от {date_str}"
                if number:
                    requisites += f" № {number}"
            elif doctype_name:
                requisites = doctype_name
                if number:
                    requisites += f" № {number}"
            elif date_str:
                # If only date exists (no doctype), just show the date
                requisites = f"от {date_str}"
                if number:
                    requisites += f" № {number}"   
        return requisites

    def create_pipeline_item(self, doc):
        """Create a pipeline-compatible item from the document"""
        # Extract title from names field
        title = ""
        if 'names' in doc and doc['names']:
            title = doc['names'][0]
        
        # Create metadata structure
        metadata = {
            'title': title,
            'source': 'cntd',
            'url': f"https://docs.cntd.ru/document/{doc.get('id', '')}",
            'parsed_at': int(datetime.now().timestamp()),
            'published_at': doc.get('in_product_created'),
            'published_at_iso': doc.get('in_product_created'),
            'original_data': doc  # Store the full original data
        }
        
        # Create LegalDocument item
        item = LegalDocument()
        item['id'] = str(uuid.uuid4())
        item['text'] = title  # Use title as text since no content available
        item['lawMetadata'] = metadata
        
        return item

    def convert_unicode_recursively(self, data):
        """Convert Unicode escape sequences to readable text recursively"""
        if isinstance(data, str):
            return html.unescape(data)
        elif isinstance(data, list):
            return [self.convert_unicode_recursively(item) for item in data]
        elif isinstance(data, dict):
            return {key: self.convert_unicode_recursively(value) for key, value in data.items()}
        else:
            return data

    def closed(self, reason):
        logging.info(f"CNTD spider batch {self.start_page}-{self.end_page} closed. Reason: {reason}") 