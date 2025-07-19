#!/usr/bin/env python3
"""
Script to fetch all document IDs from CNTD API and find missing ones in the database.
Uses concurrent processing for efficiency.
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime
from typing import List, Set, Dict
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('find_missing_doc_ids.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MissingDocIDFinder:
    def __init__(self, category: int = 3, db_url: str | None = None):
        self.category = category
        self.base_url = "https://docs.cntd.ru/api/search"
        self.db_url = db_url or os.getenv('DATABASE_URL', 'postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres')
        self.session: aiohttp.ClientSession | None = None
        self.all_api_doc_ids: Set[str] = set()
        self.db_doc_ids: Set[str] = set()
        self.missing_doc_ids: Set[str] = set()
        self.page_results: Dict[int, List[str]] = {}
        self.failed_pages: List[int] = []
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def fetch_page_doc_ids(self, page: int, retries: int = 3) -> List[str]:
        """Fetch document IDs from a specific page using the same API as the spider"""
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
    
    async def fetch_all_api_doc_ids(self, max_pages: int = 500, max_concurrent: int = 10):
        """Fetch all document IDs from the API"""
        logger.info(f"Starting to fetch document IDs from all {max_pages} pages")
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(page):
            async with semaphore:
                return await self.fetch_page_doc_ids(page)
        
        # Create tasks for all pages
        tasks = [fetch_with_semaphore(page) for page in range(1, max_pages + 1)]
        
        # Execute all tasks
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for page, result in enumerate(results, 1):
            if isinstance(result, Exception):
                logger.error(f"Page {page}: Exception occurred: {result}")
                self.failed_pages.append(page)
            elif isinstance(result, list):
                self.page_results[page] = result
                self.all_api_doc_ids.update(result)
                logger.info(f"Page {page}: Added {len(result)} doc IDs, total unique: {len(self.all_api_doc_ids)}")
            else:
                logger.error(f"Page {page}: Unexpected result type: {type(result)}")
                self.failed_pages.append(page)
        
        logger.info(f"API fetching complete. Total unique doc IDs: {len(self.all_api_doc_ids)}")
    
    def fetch_db_doc_ids(self, batch_size: int = 1000):
        """Fetch all document IDs from the database using concurrent processing"""
        logger.info("Starting to fetch document IDs from database")
        
        def fetch_batch(offset):
            """Fetch a batch of document IDs from database"""
            try:
                engine = create_engine(self.db_url)
                with engine.connect() as conn:
                    # Fetch doc_ids from the docs_cntd table (CNTD documents)
                    result = conn.execute(text("""
                        SELECT doc_id FROM docs_cntd 
                        WHERE doc_id IS NOT NULL 
                        ORDER BY doc_id 
                        LIMIT :limit OFFSET :offset
                    """), {"limit": batch_size, "offset": offset})
                    
                    results = result.fetchall()
                    doc_ids = [str(row[0]) for row in results if row[0] is not None]
                    
                return doc_ids
                
            except Exception as e:
                logger.error(f"Error fetching batch at offset {offset}: {e}")
                return []
        
        # Get total count first
        try:
            engine = create_engine(self.db_url)
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM docs_cntd WHERE doc_id IS NOT NULL"))
                total_count = result.scalar()
            
            logger.info(f"Total CNTD documents in database: {total_count}")
            
        except Exception as e:
            logger.error(f"Error getting total count: {e}")
            return
        
        # Use ThreadPoolExecutor for concurrent database access
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Create batch tasks
            futures = []
            if total_count is not None:
                for offset in range(0, total_count, batch_size):
                    future = executor.submit(fetch_batch, offset)
                    futures.append(future)
            
            # Collect results
            for future in as_completed(futures):
                try:
                    batch_doc_ids = future.result()
                    self.db_doc_ids.update(batch_doc_ids)
                    logger.info(f"Database batch: Added {len(batch_doc_ids)} doc IDs, total: {len(self.db_doc_ids)}")
                except Exception as e:
                    logger.error(f"Error processing batch: {e}")
        
        logger.info(f"Database fetching complete. Total unique doc IDs: {len(self.db_doc_ids)}")
    
    def find_missing_doc_ids(self):
        """Find document IDs that are in API but not in database"""
        logger.info("Finding missing document IDs...")
        
        # Find doc_ids that are in API but not in database
        self.missing_doc_ids = self.all_api_doc_ids - self.db_doc_ids
        
        logger.info(f"Missing document IDs: {len(self.missing_doc_ids)}")
        logger.info(f"API total: {len(self.all_api_doc_ids)}")
        logger.info(f"Database total: {len(self.db_doc_ids)}")
        logger.info(f"Coverage: {len(self.db_doc_ids) / len(self.all_api_doc_ids) * 100:.2f}%")
    
    def generate_report(self) -> Dict:
        """Generate a comprehensive report"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'category': self.category,
            'api_total_doc_ids': len(self.all_api_doc_ids),
            'db_total_doc_ids': len(self.db_doc_ids),
            'missing_doc_ids': len(self.missing_doc_ids),
            'coverage_percentage': len(self.db_doc_ids) / len(self.all_api_doc_ids) * 100 if self.all_api_doc_ids else 0,
            'failed_pages': self.failed_pages,
            'total_pages_processed': len(self.page_results),
            'sample_missing_doc_ids': list(self.missing_doc_ids)[:10] if self.missing_doc_ids else []
        }
        
        return report
    
    def save_results(self, filename: str | None = None):
        """Save results to JSON file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"missing_doc_ids_{timestamp}.json"
        
        data = {
            'report': self.generate_report(),
            'all_missing_doc_ids': list(self.missing_doc_ids),
            'all_api_doc_ids': list(self.all_api_doc_ids),
            'all_db_doc_ids': list(self.db_doc_ids)
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Results saved to {filename}")
        return filename

async def main():
    """Main function to run the missing document ID finder"""
    start_time = time.time()
    
    async with MissingDocIDFinder(category=3) as finder:
        # Step 1: Fetch all document IDs from API
        logger.info("Step 1: Fetching document IDs from API...")
        await finder.fetch_all_api_doc_ids(max_pages=500, max_concurrent=10)
        
        # Step 2: Fetch all document IDs from database
        logger.info("Step 2: Fetching document IDs from database...")
        finder.fetch_db_doc_ids(batch_size=1000)
        
        # Step 3: Find missing document IDs
        logger.info("Step 3: Finding missing document IDs...")
        finder.find_missing_doc_ids()
        
        # Generate and display report
        report = finder.generate_report()
        
        print("\n" + "="*60)
        print("MISSING DOCUMENT ID ANALYSIS REPORT")
        print("="*60)
        print(f"Category: {report['category']}")
        print(f"API Total Document IDs: {report['api_total_doc_ids']}")
        print(f"Database Total Document IDs: {report['db_total_doc_ids']}")
        print(f"Missing Document IDs: {report['missing_doc_ids']}")
        print(f"Coverage: {report['coverage_percentage']:.2f}%")
        print(f"Pages Processed: {report['total_pages_processed']}")
        print(f"Failed Pages: {len(report['failed_pages'])}")
        
        if report['sample_missing_doc_ids']:
            print(f"\nSample Missing Document IDs:")
            for doc_id in report['sample_missing_doc_ids']:
                print(f"  {doc_id}")
        
        if report['failed_pages']:
            print(f"\nFailed Pages: {report['failed_pages']}")
        
        # Save results
        filename = finder.save_results()
        
        elapsed_time = time.time() - start_time
        print(f"\nElapsed time: {elapsed_time:.2f} seconds")
        print(f"Results saved to: {filename}")

if __name__ == "__main__":
    asyncio.run(main()) 