#!/usr/bin/env python3
"""
Script to find missing document IDs by comparing all_doc_ids and db arrays from JSON file.
"""

import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def find_missing_doc_ids(filename: str):
    """Find missing document IDs from JSON file"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Get the arrays
        all_doc_ids = set(data.get('all_doc_ids', []))
        db_doc_ids = set(data.get('db', []))
        
        logger.info(f"Loaded {len(all_doc_ids)} document IDs from API")
        logger.info(f"Loaded {len(db_doc_ids)} document IDs from database")
        
        # Find missing ones (in API but not in DB)
        missing_doc_ids = all_doc_ids - db_doc_ids
        
        logger.info(f"Missing document IDs: {len(missing_doc_ids)}")
        logger.info(f"Coverage: {len(db_doc_ids) / len(all_doc_ids) * 100:.2f}%")
        
        return missing_doc_ids, all_doc_ids, db_doc_ids
        
    except Exception as e:
        logger.error(f"Error processing file {filename}: {e}")
        return set(), set(), set()

def save_results(missing_doc_ids, all_doc_ids, db_doc_ids, filename=None):
    """Save results to JSON file"""
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"missing_doc_ids_{timestamp}.json"
    
    data = {
        'timestamp': datetime.now().isoformat(),
        'api_total_doc_ids': len(all_doc_ids),
        'db_total_doc_ids': len(db_doc_ids),
        'missing_doc_ids': len(missing_doc_ids),
        'coverage_percentage': len(db_doc_ids) / len(all_doc_ids) * 100 if all_doc_ids else 0,
        'all_missing_doc_ids': list(missing_doc_ids),
        'all_api_doc_ids': list(all_doc_ids),
        'all_db_doc_ids': list(db_doc_ids)
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Results saved to {filename}")
    return filename

def main():
    """Main function"""
    # Find the JSON file with both arrays
    import os
    json_files = [f for f in os.listdir('.') if f.endswith('.json') and ('all' in f or 'doc_ids' in f)]
    
    if not json_files:
        logger.error("No JSON files found. Please provide the filename.")
        return
    
    # Use the most recent file or ask user
    if len(json_files) == 1:
        filename = json_files[0]
    else:
        print("Available JSON files:")
        for i, f in enumerate(json_files):
            print(f"  {i+1}. {f}")
        choice = input("Enter the number of the file to process: ")
        filename = json_files[int(choice) - 1]
    
    logger.info(f"Processing file: {filename}")
    
    # Find missing document IDs
    missing_doc_ids, all_doc_ids, db_doc_ids = find_missing_doc_ids(filename)
    
    # Display results
    print("\n" + "="*60)
    print("MISSING DOCUMENT ID ANALYSIS")
    print("="*60)
    print(f"API Total Document IDs: {len(all_doc_ids)}")
    print(f"Database Total Document IDs: {len(db_doc_ids)}")
    print(f"Missing Document IDs: {len(missing_doc_ids)}")
    print(f"Coverage: {len(db_doc_ids) / len(all_doc_ids) * 100:.2f}%")
    
    if missing_doc_ids:
        print(f"\nSample Missing Document IDs:")
        for doc_id in list(missing_doc_ids)[:10]:
            print(f"  {doc_id}")
    
    # Save results
    filename = save_results(missing_doc_ids, all_doc_ids, db_doc_ids)
    print(f"\nResults saved to: {filename}")

if __name__ == "__main__":
    main() 