#!/usr/bin/env python3
"""
Script to fetch missing CNTD documents by their document IDs and save them to database.
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime
from typing import List, Set, Dict
import logging
import os
from sqlalchemy import create_engine, text
import uuid
import html

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fetch_missing_docs.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MissingDocsFetcher:
    def __init__(self, db_url: str | None = None):
        self.db_url = db_url or os.getenv('DATABASE_URL', 'postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres')
        self.session: aiohttp.ClientSession | None = None
        self.successful_docs: List[str] = []
        self.failed_docs: List[Dict] = []
        self.engine = create_engine(self.db_url)
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    def load_missing_doc_ids(self, filename: str) -> List[str]:
        """Load missing document IDs from JSON file"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            missing_doc_ids = data.get('all_missing_doc_ids', [])
            logger.info(f"Loaded {len(missing_doc_ids)} missing document IDs from {filename}")
            return missing_doc_ids
            
        except Exception as e:
            logger.error(f"Error loading missing doc IDs from {filename}: {e}")
            return []
    
    async def fetch_document_data(self, doc_id: str, retries: int = 3) -> Dict | None:
        """Fetch document data from CNTD API"""
        api_url = f"https://docs.cntd.ru/api/document/{doc_id}"
        
        for attempt in range(retries):
            try:
                logger.info(f"Fetching document {doc_id} (attempt {attempt + 1})")
                
                if self.session is None:
                    logger.error("Session is not initialized")
                    return None
                    
                async with self.session.get(api_url) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data and isinstance(data, dict):
                            # Convert Unicode escape sequences
                            converted_data = self.convert_unicode_recursively(data)
                            logger.info(f"Successfully fetched document {doc_id}")
                            return converted_data
                        else:
                            logger.warning(f"Document {doc_id}: Unexpected response structure")
                            return None
                    else:
                        logger.warning(f"Document {doc_id}: HTTP {response.status}")
                        
            except Exception as e:
                logger.error(f"Document {doc_id}: Error on attempt {attempt + 1}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    
        logger.error(f"Document {doc_id}: Failed after {retries} attempts")
        return None
    
    async def fetch_document_page(self, doc_id: str, retries: int = 3) -> str:
        """Fetch full text from document page"""
        doc_url = f"https://docs.cntd.ru/document/{doc_id}"
        
        for attempt in range(retries):
            try:
                logger.info(f"Fetching document page {doc_id} (attempt {attempt + 1})")
                
                if self.session is None:
                    logger.error("Session is not initialized")
                    return ""
                    
                async with self.session.get(doc_url) as response:
                    if response.status == 200:
                        html_content = await response.text()
                        
                        # Extract full text from document-content div
                        full_text = self.extract_text_from_html(html_content)
                        
                        logger.info(f"Extracted {len(full_text)} characters for document {doc_id}")
                        return full_text
                    else:
                        logger.warning(f"Document page {doc_id}: HTTP {response.status}")
                        
            except Exception as e:
                logger.error(f"Document page {doc_id}: Error on attempt {attempt + 1}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    
        logger.error(f"Document page {doc_id}: Failed after {retries} attempts")
        return ""
    
    def extract_text_from_html(self, html_content: str) -> str:
        """Extract text from HTML content"""
        try:
            from bs4 import BeautifulSoup
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find the main document-content div
            content_div = soup.find('div', class_='document-content')
            if not content_div:
                return ""
            
            # Find all textBlock1 and document-text_block divs
            text_blocks = content_div.find_all(['div'], class_=['textBlock1', 'document-text_block'])
            
            full_text = ""
            for block in text_blocks:
                # Extract all p tags text from each block
                paragraphs = block.find_all('p')
                for p in paragraphs:
                    p_text = p.get_text().strip()
                    if p_text:
                        # Check if this paragraph starts with the disclaimer
                        if p_text.startswith('Электронный текст документа'):
                            logger.info("Found disclaimer, stopping text extraction")
                            break  # Stop including any more paragraphs
                        full_text += p_text + "\n"
            
            return full_text.strip()
            
        except Exception as e:
            logger.error(f"Error extracting text from HTML: {e}")
            return ""
    
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
    
    def generate_requisites(self, search_data: Dict) -> str:
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
    
    def save_document_to_db(self, doc_id: str, api_data: Dict, full_text: str) -> bool:
        """Save document to database"""
        try:
            # Extract title from names field
            title = ""
            if 'names' in api_data and api_data['names']:
                title = api_data['names'][0]
            
            # Generate requisites from registration data
            requisites = self.generate_requisites(api_data)
            
            # Extract published_at_iso from registration data
            published_at_iso = None
            if 'registrations' in api_data and api_data['registrations']:
                reg = api_data['registrations'][0]
                date_str = reg.get('date')
                if date_str:
                    try:
                        published_at_iso = datetime.strptime(date_str, '%Y-%m-%d')
                    except ValueError:
                        logger.warning(f"Could not parse date: {date_str}")
                        published_at_iso = None
            
            # Create the item with the exact structure
            item = {
                'id': str(uuid.uuid4()),  # Generate unique UUID
                'doc_id': doc_id,  # Original CNTD document ID
                'title': title,
                'requisites': requisites,
                'text': full_text,
                'url': f"https://docs.cntd.ru/document/{doc_id}",
                'parsed_at': int(datetime.now().timestamp()),
                'published_at_iso': published_at_iso,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            
            # Save to database
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO docs_cntd (
                        id, doc_id, title, requisites, text, url, 
                        parsed_at, published_at_iso, created_at, updated_at
                    ) VALUES (
                        :id, :doc_id, :title, :requisites, :text, :url,
                        :parsed_at, :published_at_iso, :created_at, :updated_at
                    )
                """), item)
                conn.commit()
            
            logger.info(f"✅ Saved document {doc_id} to database")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving document {doc_id} to database: {e}")
            return False
    
    async def fetch_and_save_document(self, doc_id: str, max_concurrent: int = 5):
        """Fetch and save a single document"""
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async with semaphore:
            try:
                # Step 1: Fetch document data from API
                api_data = await self.fetch_document_data(doc_id)
                if not api_data:
                    self.failed_docs.append({
                        'doc_id': doc_id,
                        'error': 'Failed to fetch API data',
                        'timestamp': datetime.now().isoformat()
                    })
                    return
                
                # Step 2: Fetch full text from document page
                full_text = await self.fetch_document_page(doc_id)
                
                # Step 3: Save to database
                success = self.save_document_to_db(doc_id, api_data, full_text)
                
                if success:
                    self.successful_docs.append(doc_id)
                else:
                    self.failed_docs.append({
                        'doc_id': doc_id,
                        'error': 'Failed to save to database',
                        'timestamp': datetime.now().isoformat()
                    })
                    
            except Exception as e:
                logger.error(f"Error processing document {doc_id}: {e}")
                self.failed_docs.append({
                    'doc_id': doc_id,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                })
    
    async def fetch_all_missing_docs(self, missing_doc_ids: List[str], max_concurrent: int = 5):
        """Fetch all missing documents with controlled concurrency"""
        logger.info(f"Starting to fetch {len(missing_doc_ids)} missing documents")
        
        # Create tasks for all documents
        tasks = [self.fetch_and_save_document(doc_id, max_concurrent) for doc_id in missing_doc_ids]
        
        # Execute all tasks
        await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info(f"Completed fetching missing documents")
        logger.info(f"Successful: {len(self.successful_docs)}")
        logger.info(f"Failed: {len(self.failed_docs)}")
    
    def save_results(self, filename: str | None = None):
        """Save results to JSON file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"fetch_missing_docs_results_{timestamp}.json"
        
        data = {
            'timestamp': datetime.now().isoformat(),
            'successful_docs': self.successful_docs,
            'failed_docs': self.failed_docs,
            'total_successful': len(self.successful_docs),
            'total_failed': len(self.failed_docs)
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Results saved to {filename}")
        return filename

async def main():
    """Main function to fetch missing documents"""
    start_time = time.time()
    
    # Find the most recent missing_doc_ids file
    missing_files = [f for f in os.listdir('.') if f.startswith('missing_doc_ids_') and f.endswith('.json')]
    if not missing_files:
        logger.error("No missing_doc_ids files found. Please run find_missing_doc_ids.py first.")
        return
    
    # Sort by timestamp and get the most recent
    missing_files.sort(reverse=True)
    latest_file = missing_files[0]
    logger.info(f"Using missing doc IDs file: {latest_file}")
    
    async with MissingDocsFetcher() as fetcher:
        # Load missing document IDs
        missing_doc_ids = fetcher.load_missing_doc_ids(latest_file)
        
        if not missing_doc_ids:
            logger.error("No missing document IDs found")
            return
        
        # Fetch all missing documents
        await fetcher.fetch_all_missing_docs(missing_doc_ids, max_concurrent=5)
        
        # Display results
        print("\n" + "="*60)
        print("MISSING DOCUMENTS FETCHING RESULTS")
        print("="*60)
        print(f"Total processed: {len(missing_doc_ids)}")
        print(f"Successful: {len(fetcher.successful_docs)}")
        print(f"Failed: {len(fetcher.failed_docs)}")
        print(f"Success rate: {len(fetcher.successful_docs) / len(missing_doc_ids) * 100:.2f}%")
        
        if fetcher.successful_docs:
            print(f"\nSample successful documents:")
            for doc_id in fetcher.successful_docs[:5]:
                print(f"  {doc_id}")
        
        if fetcher.failed_docs:
            print(f"\nSample failed documents:")
            for failed in fetcher.failed_docs[:5]:
                print(f"  {failed['doc_id']}: {failed['error']}")
        
        # Save results
        filename = fetcher.save_results()
        
        elapsed_time = time.time() - start_time
        print(f"\nElapsed time: {elapsed_time:.2f} seconds")
        print(f"Results saved to: {filename}")

if __name__ == "__main__":
    asyncio.run(main()) 