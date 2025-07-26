#!/usr/bin/env python3
"""
CNTD Batch Processing Script
Runs multiple CNTD spider instances in parallel for different thematic batches
"""

import os
import sys
import json
import subprocess
import time
import logging
from datetime import datetime
from pathlib import Path

# Add the news_parser directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def setup_logging():
    """Setup logging for batch processing"""
    logs_dir = Path(__file__).parent / 'logs'
    logs_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    log_file = logs_dir / f'cntd_batch_{timestamp}.log'
    
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

def create_thematic_batches(thematic_ids, batch_size=10):
    """Split thematic IDs into batches"""
    batches = []
    for i in range(0, len(thematic_ids), batch_size):
        batch = thematic_ids[i:i + batch_size]
        batches.append(batch)
    
    logging.info(f"Created {len(batches)} batches of {batch_size} thematics each")
    return batches

def save_batch_to_file(batch, batch_num):
    """Save a batch of thematic IDs to a temporary file"""
    batch_dir = Path(__file__).parent / 'temp_batches'
    batch_dir.mkdir(exist_ok=True)
    
    batch_file = batch_dir / f'batch_{batch_num:03d}.json'
    with open(batch_file, 'w') as f:
        json.dump(batch, f, indent=2)
    
    return batch_file

def run_spider_process(batch_file, batch_num, max_pages=10, start_page=1, end_page=None):
    """Run a single spider process for a batch"""
    cmd = [
        'scrapy', 'crawl', 'cntd',
        '-a', f'thematic_ids_file={batch_file}',
        '-a', f'max_pages_per_thematic={max_pages}',
        '-a', f'start_page={start_page}'
    ]
    
    if end_page:
        cmd.extend(['-a', f'end_page={end_page}'])
    
    # Add output file
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    output_file = f'cntd_batch_{batch_num:03d}_{timestamp}.json'
    cmd.extend(['-o', output_file])
    
    logging.info(f"Starting batch {batch_num}: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        if result.returncode == 0:
            logging.info(f"Batch {batch_num} completed successfully")
            logging.info(f"Output: {result.stdout}")
        else:
            logging.error(f"Batch {batch_num} failed with return code {result.returncode}")
            logging.error(f"Error: {result.stderr}")
            
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        logging.error(f"Batch {batch_num} timed out after 1 hour")
        return False
    except Exception as e:
        logging.error(f"Error running batch {batch_num}: {e}")
        return False

def run_parallel_batches(batches, max_concurrent=3, max_pages=10, start_page=1, end_page=None):
    """Run batches in parallel with limited concurrency"""
    import concurrent.futures
    
    batch_files = []
    processes = []
    
    # Create batch files
    for i, batch in enumerate(batches, 1):
        batch_file = save_batch_to_file(batch, i)
        batch_files.append(batch_file)
    
    logging.info(f"Starting {len(batches)} batches with max {max_concurrent} concurrent processes")
    
    # Run batches in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        futures = []
        
        for i, batch_file in enumerate(batch_files, 1):
            future = executor.submit(
                run_spider_process, 
                batch_file, 
                i, 
                max_pages, 
                start_page, 
                end_page
            )
            futures.append(future)
        
        # Wait for all batches to complete
        successful = 0
        failed = 0
        
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                if future.result():
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                logging.error(f"Batch {i} failed with exception: {e}")
                failed += 1
    
    # Cleanup batch files
    for batch_file in batch_files:
        try:
            batch_file.unlink()
        except:
            pass
    
    logging.info(f"All batches completed: {successful} successful, {failed} failed")
    return successful, failed

def run_sequential_batches(batches, max_pages=10, start_page=1, end_page=None):
    """Run batches sequentially (one after another)"""
    batch_files = []
    successful = 0
    failed = 0
    
    # Create batch files
    for i, batch in enumerate(batches, 1):
        batch_file = save_batch_to_file(batch, i)
        batch_files.append(batch_file)
    
    logging.info(f"Starting {len(batches)} batches sequentially")
    
    # Run batches one by one
    for i, batch_file in enumerate(batch_files, 1):
        logging.info(f"Processing batch {i}/{len(batches)}")
        
        if run_spider_process(batch_file, i, max_pages, start_page, end_page):
            successful += 1
        else:
            failed += 1
        
        # Small delay between batches
        time.sleep(2)
    
    # Cleanup batch files
    for batch_file in batch_files:
        try:
            batch_file.unlink()
        except:
            pass
    
    logging.info(f"All batches completed: {successful} successful, {failed} failed")
    return successful, failed

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='CNTD Batch Processing')
    parser.add_argument('--batch-size', type=int, default=10, 
                       help='Number of thematics per batch (default: 10)')
    parser.add_argument('--max-concurrent', type=int, default=3,
                       help='Maximum concurrent processes (default: 3)')
    parser.add_argument('--max-pages', type=int, default=500,
                       help='Maximum pages per thematic (default: 500)')
    parser.add_argument('--start-page', type=int, default=1,
                       help='Starting page number (default: 1)')
    parser.add_argument('--end-page', type=int, default=None,
                       help='Ending page number (optional)')
    parser.add_argument('--sequential', action='store_true',
                       help='Run batches sequentially instead of in parallel')
    
    args = parser.parse_args()
    
    # Setup logging
    log_file = setup_logging()
    logging.info(f"CNTD Batch Processing started - Log: {log_file}")
    
    # Load thematic IDs
    thematic_ids = load_thematic_ids()
    if not thematic_ids:
        logging.error("No thematic IDs loaded. Exiting.")
        return 1
    
    # Create batches
    batches = create_thematic_batches(thematic_ids, args.batch_size)
    
    # Run batches
    if args.sequential:
        successful, failed = run_sequential_batches(
            batches, args.max_pages, args.start_page, args.end_page
        )
    else:
        successful, failed = run_parallel_batches(
            batches, args.max_concurrent, args.max_pages, args.start_page, args.end_page
        )
    
    # Summary
    logging.info("=" * 50)
    logging.info(f"BATCH PROCESSING COMPLETE")
    logging.info(f"Total batches: {len(batches)}")
    logging.info(f"Successful: {successful}")
    logging.info(f"Failed: {failed}")
    logging.info(f"Success rate: {(successful/(successful+failed)*100):.1f}%")
    logging.info("=" * 50)
    
    return 0 if failed == 0 else 1

if __name__ == '__main__':
    sys.exit(main()) 