#!/usr/bin/env python3
"""
CNTD Total Document Counter
Counts total documents across all thematic IDs by querying the API
"""

import os
import sys
import json
import time
import logging
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

# Add the news_parser directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def setup_logging():
    """Setup logging for document counting"""
    logs_dir = Path(__file__).parent / 'logs'
    logs_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    log_file = logs_dir / f'document_count_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return log_file

def load_thematic_ids():
    """Load thematic IDs from the JSON file"""
    thematic_file = Path(__file__).parent / '..' / 'extra' / 'cntd_sub.json'
    
    try:
        with open(thematic_file, 'r') as f:
            thematic_ids = json.load(f)
        logging.info(f"Loaded {len(thematic_ids)} thematic IDs from {thematic_file}")
        return thematic_ids
    except Exception as e:
        logging.error(f"Error loading thematic IDs: {e}")
        return []

def get_thematic_document_count(thematic_id: int, session: requests.Session) -> Tuple[int, int, str]:
    """
    Get document count for a specific thematic ID
    
    Returns:
        Tuple of (total_documents, total_pages, error_message)
    """
    url = f"https://docs.cntd.ru/api/search"
    params = {
        'category_join': 'and',
        'order_by': 'registration_date:desc',
        'category[]': [1, 3],
        'thematic': thematic_id,
        'page': 1
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    }
    
    try:
        response = session.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract pagination info
        if 'pagination' in data:
            pagination = data['pagination']
            total_documents = pagination.get('total', 0)
            total_pages = pagination.get('last_page', 0)
            
            logging.info(f"Thematic {thematic_id}: {total_documents:,} documents across {total_pages} pages")
            return total_documents, total_pages, ""
        else:
            error_msg = f"No pagination data found for thematic {thematic_id}"
            logging.error(error_msg)
            return 0, 0, error_msg
            
    except requests.exceptions.RequestException as e:
        error_msg = f"Request failed for thematic {thematic_id}: {e}"
        logging.error(error_msg)
        return 0, 0, error_msg
    except json.JSONDecodeError as e:
        error_msg = f"JSON decode error for thematic {thematic_id}: {e}"
        logging.error(error_msg)
        return 0, 0, error_msg
    except Exception as e:
        error_msg = f"Unexpected error for thematic {thematic_id}: {e}"
        logging.error(error_msg)
        return 0, 0, error_msg

def count_all_documents(thematic_ids: List[int], delay: float = 1.0) -> Dict:
    """
    Count documents for all thematic IDs
    
    Args:
        thematic_ids: List of thematic IDs to process
        delay: Delay between requests in seconds
    
    Returns:
        Dictionary with counting results
    """
    session = requests.Session()
    
    results = {
        'thematic_counts': {},
        'total_documents': 0,
        'total_pages': 0,
        'successful_thematics': 0,
        'failed_thematics': 0,
        'errors': []
    }
    
    logging.info(f"Starting document count for {len(thematic_ids)} thematics")
    
    for i, thematic_id in enumerate(thematic_ids, 1):
        logging.info(f"Processing thematic {i}/{len(thematic_ids)}: {thematic_id}")
        
        total_docs, total_pages, error = get_thematic_document_count(thematic_id, session)
        
        if error:
            results['failed_thematics'] += 1
            results['errors'].append({
                'thematic_id': thematic_id,
                'error': error
            })
        else:
            results['successful_thematics'] += 1
            results['thematic_counts'][thematic_id] = {
                'documents': total_docs,
                'pages': total_pages
            }
            results['total_documents'] += total_docs
            results['total_pages'] += total_pages
        
        # Progress update
        progress = (i / len(thematic_ids)) * 100
        logging.info(f"Progress: {progress:.1f}% ({i}/{len(thematic_ids)})")
        
        # Delay between requests to be respectful
        if i < len(thematic_ids):
            time.sleep(delay)
    
    session.close()
    return results

def save_results(results: Dict, output_file: str):
    """Save results to JSON file"""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logging.info(f"Results saved to {output_file}")
    except Exception as e:
        logging.error(f"Error saving results: {e}")

def print_summary(results: Dict, thematic_ids: List[int]):
    """Print summary of results"""
    print("\n" + "="*80)
    print("CNTD DOCUMENT COUNT SUMMARY")
    print("="*80)
    
    print(f"📊 Overall Statistics:")
    print(f"   Total Thematics: {len(thematic_ids)}")
    print(f"   Successful: {results['successful_thematics']}")
    print(f"   Failed: {results['failed_thematics']}")
    print(f"   Success Rate: {(results['successful_thematics']/len(thematic_ids)*100):.1f}%")
    
    print(f"\n📄 Document Counts:")
    print(f"   Total Documents: {results['total_documents']:,}")
    print(f"   Total Pages: {results['total_pages']:,}")
    print(f"   Average Documents per Thematic: {results['total_documents']/results['successful_thematics']:,.0f}")
    print(f"   Average Pages per Thematic: {results['total_pages']/results['successful_thematics']:,.0f}")
    
    if results['thematic_counts']:
        print(f"\n📈 Top 10 Thematics by Document Count:")
        sorted_thematics = sorted(
            results['thematic_counts'].items(), 
            key=lambda x: x[1]['documents'], 
            reverse=True
        )
        
        for i, (thematic_id, data) in enumerate(sorted_thematics[:10], 1):
            print(f"   {i:2d}. Thematic {thematic_id}: {data['documents']:,} docs ({data['pages']} pages)")
    
    if results['errors']:
        print(f"\n❌ Failed Thematics:")
        for error in results['errors']:
            print(f"   Thematic {error['thematic_id']}: {error['error']}")
    
    print("\n" + "="*80)

def estimate_processing_time(results: Dict, batch_size: int = 10, max_concurrent: int = 3):
    """Estimate processing time based on document counts"""
    if results['successful_thematics'] == 0:
        return
    
    total_docs = results['total_documents']
    total_pages = results['total_pages']
    
    # Rough estimates
    docs_per_minute = 100  # Conservative estimate
    minutes_per_batch = total_pages / (batch_size * docs_per_minute)
    total_batches = (results['successful_thematics'] + batch_size - 1) // batch_size
    total_minutes = (total_batches * minutes_per_batch) / max_concurrent
    total_hours = total_minutes / 60
    
    print(f"\n⏱️  Processing Time Estimates:")
    print(f"   Total Documents: {total_docs:,}")
    print(f"   Total Pages: {total_pages:,}")
    print(f"   Estimated Time: {total_hours:.1f} hours ({total_minutes:.0f} minutes)")
    print(f"   With {batch_size} thematics per batch, {max_concurrent} concurrent processes")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='CNTD Total Document Counter')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='Delay between requests in seconds (default: 1.0)')
    parser.add_argument('--output', type=str, default=None,
                       help='Output file for results (default: auto-generated)')
    parser.add_argument('--batch-size', type=int, default=10,
                       help='Batch size for time estimation (default: 10)')
    parser.add_argument('--max-concurrent', type=int, default=3,
                       help='Max concurrent processes for time estimation (default: 3)')
    
    args = parser.parse_args()
    
    # Setup logging
    log_file = setup_logging()
    print(f"🔍 CNTD Document Counter - Log: {log_file}")
    
    # Load thematic IDs
    thematic_ids = load_thematic_ids()
    if not thematic_ids:
        print("❌ Cannot load thematic IDs")
        return 1
    
    # Generate output filename if not provided
    if not args.output:
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        args.output = f"document_counts_{timestamp}.json"
    
    print(f"📊 Starting document count for {len(thematic_ids)} thematics")
    print(f"⏱️  Delay between requests: {args.delay} seconds")
    print(f"💾 Results will be saved to: {args.output}")
    print()
    
    # Count documents
    start_time = datetime.now()
    results = count_all_documents(thematic_ids, args.delay)
    end_time = datetime.now()
    
    # Calculate processing time
    processing_time = end_time - start_time
    
    # Save results
    save_results(results, args.output)
    
    # Print summary
    print_summary(results, thematic_ids)
    
    # Print processing time
    print(f"\n⏱️  Counting completed in: {processing_time}")
    
    # Estimate processing time
    estimate_processing_time(results, args.batch_size, args.max_concurrent)
    
    return 0 if results['failed_thematics'] == 0 else 1

if __name__ == '__main__':
    sys.exit(main()) 