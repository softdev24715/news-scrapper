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
    allowed_domains = ['docs.cntd.ru']
    
    def __init__(self, *args, **kwargs):
        super(CNTDSpider, self).__init__(*args, **kwargs)
        self.base_search_url = 'https://docs.cntd.ru/api/search'
        self.category = kwargs.get('category', '3')  # Default category
        self.current_page = 1
        
        logging.info(f"Initializing CNTD spider with category={self.category}")

    def start_requests(self):
        """Start with the first search page"""
        search_url = f"{self.base_search_url}?order_by=registration_date:desc&category={self.category}&page={self.current_page}"
        logging.info(f"Starting CNTD spider with search URL: {search_url}")
        
        yield scrapy.Request(
            url=search_url,
            callback=self.parse_search_results,
            meta={'page': self.current_page}
        )

    def parse_search_results(self, response):
        """Parse search results and request individual document pages"""
        try:
            data = json.loads(response.text)
            logging.info(f"Parsing search results for page {response.meta['page']}")
            
            if 'data' not in data or not data['data']:
                logging.info(f"No data found on page {response.meta['page']}")
                return
            
            documents = data['data']
            logging.info(f"Found {len(documents)} documents on page {response.meta['page']}")
            
            # Process each document
            for doc in documents:
                # Convert Unicode escape sequences to readable text
                converted_doc = self.convert_unicode_recursively(doc)
                
                # Log the first document with converted text
                if doc == documents[0]:
                    logging.info(f"Sample document - ID: {converted_doc.get('id')}")
                    if 'names' in converted_doc and converted_doc['names']:
                        logging.info(f"Converted names: {converted_doc['names']}")
                
                # Request individual document page to get full text
                doc_id = converted_doc.get('id')
                if doc_id:
                    doc_url = f"https://docs.cntd.ru/document/{doc_id}"
                    logging.info(f"Requesting document page: {doc_url}")
                    
                    yield scrapy.Request(
                        url=doc_url,
                        callback=self.parse_document,
                        meta={'search_data': converted_doc}
                    )
            
            # Continue to next page (no max limit - continue until no data)
            next_page = response.meta['page'] + 1
            logging.info(f"Moving to next page: {next_page}")
            
            next_search_url = f"{self.base_search_url}?order_by=registration_date:desc&category={self.category}&page={next_page}"
            
            yield scrapy.Request(
                url=next_search_url,
                callback=self.parse_search_results,
                meta={'page': next_page}
            )
                
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON from search results: {e}")
        except Exception as e:
            logging.error(f"Error parsing search results: {e}")

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
        
        # Create the item with the exact structure you specified
        item = {
            'id': str(uuid.uuid4()),  # Generate unique UUID
            'doc_id': search_data.get('id'),  # Original CNTD document ID
            'title': title,
            'requisites': requisites,
            'text': full_text,
            'url': url,
            'parsed_at': int(datetime.now().timestamp()),
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        return item

    def generate_requisites(self, search_data):
        """Generate requisites string from registration data"""
        requisites = ""
        if 'registrations' in search_data and search_data['registrations']:
            reg = search_data['registrations'][0]  # Take the first registration
            
            # Extract components
            doctype_name = ""
            if 'doctype' in reg and reg['doctype'] and 'name' in reg['doctype']:
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
        logging.info(f"CNTD spider closed. Reason: {reason}") 