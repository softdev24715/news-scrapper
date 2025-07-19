#!/usr/bin/env python3
"""
Script to compare API document IDs with database document IDs to find missing ones.
"""

import json
import os
import logging
from datetime import datetime
from typing import List, Set
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('compare_doc_ids.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_api_doc_ids(filename: str) -> Set[str]:
    """Load API document IDs from JSON file"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        api_doc_ids = set(data.get('all_doc_ids', []))
        logger.info(f"Loaded {len(api_doc_ids)} document IDs from API file: {filename}")
        return api_doc_ids
        
    except Exception as e:
        logger.error(f"Error loading API doc IDs from {filename}: {e}")
        return set()

def fetch_db_doc_ids(db_url: str, batch_size: int = 1000) -> Set[str]:
    """Fetch all document IDs from database using concurrent processing"""
    logger.info("Starting to fetch document IDs from database")
    
    def fetch_batch(offset):
        """Fetch a batch of document IDs from database"""
        try:
            engine = create_engine(db_url)
            with engine.connect() as conn:
                # Fetch doc_ids from the docs_cntd table
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
        engine = create_engine(db_url)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM docs_cntd WHERE doc_id IS NOT NULL"))
            total_count = result.scalar()
        
        logger.info(f"Total CNTD documents in database: {total_count}")
        
    except Exception as e:
        logger.error(f"Error getting total count: {e}")
        return set()
    
    # Use ThreadPoolExecutor for concurrent database access
    db_doc_ids = set()
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
                db_doc_ids.update(batch_doc_ids)
                logger.info(f"Database batch: Added {len(batch_doc_ids)} doc IDs, total: {len(db_doc_ids)}")
            except Exception as e:
                logger.error(f"Error processing batch: {e}")
    
    logger.info(f"Database fetching complete. Total unique doc IDs: {len(db_doc_ids)}")
    return db_doc_ids

def find_missing_doc_ids(api_doc_ids: Set[str], db_doc_ids: Set[str]) -> Set[str]:
    """Find document IDs that are in API but not in database"""
    missing_doc_ids = api_doc_ids - db_doc_ids
    logger.info(f"Missing document IDs: {len(missing_doc_ids)}")
    logger.info(f"API total: {len(api_doc_ids)}")
    logger.info(f"Database total: {len(db_doc_ids)}")
    logger.info(f"Coverage: {len(db_doc_ids) / len(api_doc_ids) * 100:.2f}%")
    return missing_doc_ids

def save_results(missing_doc_ids: Set[str], api_doc_ids: Set[str], db_doc_ids: Set[str], filename: str | None = None):
    """Save results to JSON file"""
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"missing_doc_ids_{timestamp}.json"
    
    data = {
        'timestamp': datetime.now().isoformat(),
        'api_total_doc_ids': len(api_doc_ids),
        'db_total_doc_ids': len(db_doc_ids),
        'missing_doc_ids': len(missing_doc_ids),
        'coverage_percentage': len(db_doc_ids) / len(api_doc_ids) * 100 if api_doc_ids else 0,
        'all_missing_doc_ids': list(missing_doc_ids),
        'all_api_doc_ids': list(api_doc_ids),
        'all_db_doc_ids': list(db_doc_ids)
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Results saved to {filename}")
    return filename

def main():
    """Main function to compare document IDs"""
    # Find the most recent cntd_doc_ids file
    api_files = [f for f in os.listdir('.') if f.startswith('cntd_doc_ids_') and f.endswith('.json')]
    if not api_files:
        logger.error("No cntd_doc_ids files found. Please run fetch_all_doc_ids.py first.")
        return
    
    # Sort by timestamp and get the most recent
    api_files.sort(reverse=True)
    latest_file = api_files[0]
    logger.info(f"Using API doc IDs file: {latest_file}")
    
    # Load API document IDs
    api_doc_ids = load_api_doc_ids(latest_file)
    if not api_doc_ids:
        logger.error("No API document IDs loaded")
        return
    
    # Get database URL
    db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres')
    
    # Fetch database document IDs
    db_doc_ids = fetch_db_doc_ids(db_url)
    
    # Find missing document IDs
    missing_doc_ids = find_missing_doc_ids(api_doc_ids, db_doc_ids)
    
    # Display results
    print("\n" + "="*60)
    print("DOCUMENT ID COMPARISON RESULTS")
    print("="*60)
    print(f"API Total Document IDs: {len(api_doc_ids)}")
    print(f"Database Total Document IDs: {len(db_doc_ids)}")
    print(f"Missing Document IDs: {len(missing_doc_ids)}")
    print(f"Coverage: {len(db_doc_ids) / len(api_doc_ids) * 100:.2f}%")
    
    if missing_doc_ids:
        print(f"\nSample Missing Document IDs:")
        for doc_id in list(missing_doc_ids)[:10]:
            print(f"  {doc_id}")
    
    # Save results
    filename = save_results(missing_doc_ids, api_doc_ids, db_doc_ids)
    print(f"\nResults saved to: {filename}")

if __name__ == "__main__":
    main() 