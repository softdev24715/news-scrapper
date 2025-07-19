#!/usr/bin/env python3
"""
Script to fetch all document IDs from all 500 pages of CNTD spider
and count them to verify completeness.
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime
from typing import List, Set, Dict
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fetch_doc_ids.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CNTDDocIDFetcher:
    def __init__(self, category: int = 3):
        self.category = category
        self.base_url = "https://docs.cntd.ru/api/search"
        self.session: aiohttp.ClientSession | None = None
        self.all_doc_ids: Set[str] = set()
        self.page_results: Dict[int, List[str]] = {}
        self.failed_pages: List[int] = []
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def fetch_page_doc_ids(self, page: int, retries: int = 3) -> List[str]:
        """Fetch document IDs from a specific page using the correct API endpoint"""
        search_url = f"{self.base_url}?category={self.category}&page={page}"
        
        for attempt in range(retries):
            try:
                logger.info(f"Fetching page {page} (attempt {attempt + 1})")
                
                if self.session is None:
                    logger.error("Session is not initialized")
                    return []
                    
                async with self.session.get(search_url) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if 'data' in data and isinstance(data['data'], list):
                            doc_ids = []
                            for item in data['data']:
                                if 'id' in item:
                                    doc_ids.append(str(item['id']))
                            
                            logger.info(f"Page {page}: Found {len(doc_ids)} document IDs")
                            return doc_ids
                        else:
                            logger.warning(f"Page {page}: Unexpected response structure - no 'data' field")
                            return []
                    else:
                        logger.warning(f"Page {page}: HTTP {response.status}")
                        
            except Exception as e:
                logger.error(f"Page {page}: Error on attempt {attempt + 1}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    
        logger.error(f"Page {page}: Failed after {retries} attempts")
        self.failed_pages.append(page)
        return []
    
    async def fetch_all_pages(self, max_pages: int = 500, max_concurrent: int = 10):
        """Fetch document IDs from all available pages (up to 500)"""
        logger.info(f"Starting to fetch document IDs from all {max_pages} pages")
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(page):
            async with semaphore:
                return await self.fetch_page_doc_ids(page)
        
        # Create tasks for all pages with explicit page tracking
        tasks = []
        for page in range(1, max_pages + 1):
            task = fetch_with_semaphore(page)
            tasks.append((page, task))
        
        # Execute all tasks
        results = []
        for page, task in tasks:
            try:
                result = await task
                results.append((page, result))
            except Exception as e:
                logger.error(f"Page {page}: Exception occurred: {e}")
                self.failed_pages.append(page)
                results.append((page, e))
        
        # Process results with correct page mapping
        for page, result in results:
            if isinstance(result, Exception):
                logger.error(f"Page {page}: Exception occurred: {result}")
                self.failed_pages.append(page)
            elif isinstance(result, list):
                self.page_results[page] = result
                self.all_doc_ids.update(result)
                logger.info(f"Page {page}: Added {len(result)} doc IDs: {result[:3]}... (showing first 3)")
                logger.info(f"Page {page}: Total unique so far: {len(self.all_doc_ids)}")
            else:
                logger.error(f"Page {page}: Unexpected result type: {type(result)}")
                self.failed_pages.append(page)
    
    def generate_report(self) -> Dict:
        """Generate a comprehensive report of the fetching results"""
        total_pages = len(self.page_results)
        total_doc_ids = len(self.all_doc_ids)
        failed_pages_count = len(self.failed_pages)
        
        # Calculate expected documents (assuming ~20 per page)
        expected_docs = total_pages * 20
        
        # Find pages with unusual document counts
        unusual_pages = []
        for page, doc_ids in self.page_results.items():
            if len(doc_ids) < 15 or len(doc_ids) > 25:  # Allow some variance
                unusual_pages.append((page, len(doc_ids)))
        
        # Count total raw doc_ids (including duplicates across pages)
        total_raw_doc_ids = sum(len(doc_ids) for doc_ids in self.page_results.values())
        
        # Find duplicate doc_ids across pages
        all_doc_ids_list = []
        for doc_ids in self.page_results.values():
            all_doc_ids_list.extend(doc_ids)
        
        duplicates = []
        seen = set()
        for doc_id in all_doc_ids_list:
            if doc_id in seen:
                duplicates.append(doc_id)
            else:
                seen.add(doc_id)
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'category': self.category,
            'total_pages_processed': total_pages,
            'total_pages_failed': failed_pages_count,
            'total_raw_doc_ids': total_raw_doc_ids,  # Total including duplicates
            'total_unique_doc_ids': total_doc_ids,   # Total unique (after deduplication)
            'duplicate_doc_ids': list(set(duplicates)),
            'duplicate_count': len(set(duplicates)),
            'expected_documents': expected_docs,
            'difference_from_expected': total_doc_ids - expected_docs,
            'failed_pages': self.failed_pages,
            'unusual_pages': unusual_pages,
            'pages_with_zero_docs': [page for page, doc_ids in self.page_results.items() if len(doc_ids) == 0],
            'pages_with_max_docs': [page for page, doc_ids in self.page_results.items() if len(doc_ids) >= 20]
        }
        
        return report
    
    def find_doc_id_pages(self, doc_id: str) -> List[int]:
        """Find which pages contain a specific document ID"""
        pages = []
        for page, doc_ids in self.page_results.items():
            if doc_id in doc_ids:
                pages.append(page)
        return pages
    
    def check_specific_doc_id(self, doc_id: str):
        """Debug function to check a specific document ID across pages"""
        pages = self.find_doc_id_pages(doc_id)
        logger.info(f"Document ID {doc_id} found on pages: {pages}")
        
        for page in pages:
            doc_ids = self.page_results.get(page, [])
            logger.info(f"Page {page} doc_ids: {doc_ids}")
        
        return pages
    
    def save_results(self, filename: str | None = None):
        """Save results to JSON file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"cntd_doc_ids_{timestamp}.json"
        
        data = {
            'report': self.generate_report(),
            'page_results': self.page_results,
            'all_doc_ids': list(self.all_doc_ids)
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Results saved to {filename}")
        return filename

async def main():
    """Main function to run the document ID fetching"""
    start_time = time.time()
    
    async with CNTDDocIDFetcher(category=3) as fetcher:
        # Fetch document IDs from all 500 pages
        await fetcher.fetch_all_pages(max_pages=500, max_concurrent=10)
        
        # Generate and display report
        report = fetcher.generate_report()
        
        print("\n" + "="*60)
        print("CNTD DOCUMENT ID FETCHING REPORT")
        print("="*60)
        print(f"Category: {report['category']}")
        print(f"Total pages processed: {report['total_pages_processed']}")
        print(f"Total pages failed: {report['total_pages_failed']}")
        print(f"Total raw document IDs: {report['total_raw_doc_ids']}")
        print(f"Total unique document IDs: {report['total_unique_doc_ids']}")
        print(f"Duplicate document IDs: {report['duplicate_count']}")
        print(f"Expected documents: {report['expected_documents']}")
        print(f"Difference from expected: {report['difference_from_expected']}")
        
        if report['failed_pages']:
            print(f"\nFailed pages: {report['failed_pages']}")
        
        if report['unusual_pages']:
            print(f"\nUnusual pages (not 15-25 docs):")
            for page, count in report['unusual_pages'][:10]:  # Show first 10
                print(f"  Page {page}: {count} documents")
        
        if report['duplicate_doc_ids']:
            print(f"\nSample duplicate doc_ids: {report['duplicate_doc_ids'][:5]}")
        
        # Check specific document IDs if they exist in results
        for doc_id in ['1313277743', '1313277144']:
            if doc_id in fetcher.all_doc_ids:
                print(f"\nChecking document ID {doc_id}:")
                fetcher.check_specific_doc_id(doc_id)
        
        # Save results
        filename = fetcher.save_results()
        
        elapsed_time = time.time() - start_time
        print(f"\nElapsed time: {elapsed_time:.2f} seconds")
        print(f"Results saved to: {filename}")

if __name__ == "__main__":
    asyncio.run(main()) 