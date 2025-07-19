#!/usr/bin/env python3
"""
Simple script to fetch all document IDs from the database.
"""

import os
import json
import logging
from datetime import datetime
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_doc_ids():
    """Fetch all document IDs from database"""
    db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres')
    
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            # Get total count
            result = conn.execute(text("SELECT COUNT(*) FROM docs_cntd WHERE doc_id IS NOT NULL"))
            total_count = result.scalar()
            logger.info(f"Total CNTD documents in database: {total_count}")
            
            # Fetch all doc_ids
            result = conn.execute(text("""
                SELECT doc_id FROM docs_cntd 
                WHERE doc_id IS NOT NULL 
                ORDER BY doc_id
            """))
            
            doc_ids = []
            for row in result:
                doc_ids.append(str(row[0]))
            
            logger.info(f"Fetched {len(doc_ids)} document IDs from database")
            
            # Show sample
            print(f"\nSample document IDs:")
            for doc_id in doc_ids[:10]:
                print(f"  {doc_id}")
            
            return doc_ids
            
    except Exception as e:
        logger.error(f"Error fetching document IDs: {e}")
        return []

def save_to_json(doc_ids, filename=None):
    """Save document IDs to JSON file"""
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"db_doc_ids_{timestamp}.json"
    
    data = {
        'timestamp': datetime.now().isoformat(),
        'total_doc_ids': len(doc_ids),
        'all_doc_ids': doc_ids
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Database document IDs saved to {filename}")
    return filename

if __name__ == "__main__":
    doc_ids = get_db_doc_ids()
    print(f"\nTotal document IDs in database: {len(doc_ids)}")
    
    if doc_ids:
        filename = save_to_json(doc_ids)
        print(f"Results saved to: {filename}") 