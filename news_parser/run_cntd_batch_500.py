#!/usr/bin/env python3
"""
CNTD Spider Batch Runner for 500 pages

This script runs 10 CNTD spider processes in parallel, each handling 50 pages:
- Process 1: pages 1-50
- Process 2: pages 51-100
- Process 3: pages 101-150
- ...
- Process 10: pages 451-500

Usage: python run_cntd_batch_500.py
"""

import subprocess
import logging
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import sys

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def create_batches(total_pages=500, num_processes=10):
    """Create round-robin batches of page ranges"""
    batches = []
    
    for process_id in range(num_processes):
        # Each process gets pages: process_id, process_id+10, process_id+20, etc.
        pages = []
        for page in range(process_id + 1, total_pages + 1, num_processes):
            pages.append(page)
        
        if pages:  # Only create batch if there are pages
            batches.append(pages)
    
    return batches

def run_spider_batch(pages, category='3'):
    """Run a single CNTD spider batch with specific pages"""
    try:
        # Convert pages list to start_page and end_page for the spider
        # The spider will handle the page distribution internally
        start_page = min(pages)
        end_page = max(pages)
        
        cmd = [
            'scrapy', 'crawl', 'cntd',
            '-a', f'start_page={start_page}',
            '-a', f'end_page={end_page}',
            '-a', f'category={category}',
            '-a', f'pages={",".join(map(str, pages))}',  # Pass specific pages
            '-s', 'LOG_LEVEL=INFO'
        ]
        
        logging.info(f"Starting batch with pages {pages[:5]}{'...' if len(pages) > 5 else ''} (total: {len(pages)} pages)")
        logging.info(f"Command: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True,
            text=True,
            timeout=7200  # 2 hours timeout per batch
        )
        
        if result.returncode == 0:
            logging.info(f"✅ Batch with {len(pages)} pages completed successfully")
            return True, f"Batch {len(pages)} pages", result.stdout
        else:
            logging.error(f"❌ Batch with {len(pages)} pages failed: {result.stderr}")
            return False, f"Batch {len(pages)} pages", result.stderr
            
    except subprocess.TimeoutExpired:
        logging.error(f"⏰ Batch with {len(pages)} pages timed out after 2 hours")
        return False, f"Batch {len(pages)} pages", "Timeout"
    except Exception as e:
        logging.error(f"💥 Batch with {len(pages)} pages error: {e}")
        return False, f"Batch {len(pages)} pages", str(e)

def main():
    """Main function"""
    # Configuration
    TOTAL_PAGES = 500
    MAX_PROCESSES = 10
    CATEGORY = '3'
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('cntd_batch_500.log'),
            logging.StreamHandler()
        ]
    )
    
    logging.info("🚀 Starting CNTD batch processing for 500 pages (Round-Robin)")
    logging.info(f"📊 Configuration:")
    logging.info(f"   - Total pages: {TOTAL_PAGES}")
    logging.info(f"   - Number of processes: {MAX_PROCESSES}")
    logging.info(f"   - Category: {CATEGORY}")
    logging.info(f"   - Distribution: Round-robin (each process gets every {MAX_PROCESSES}th page)")
    
    # Create batches
    batches = create_batches(TOTAL_PAGES, MAX_PROCESSES)
    logging.info(f"📋 Created {len(batches)} round-robin batches:")
    
    for i, pages in enumerate(batches, 1):
        # Show first few and last few pages for readability
        if len(pages) <= 10:
            page_str = str(pages)
        else:
            page_str = f"{pages[:5]}...{pages[-5:]} (total: {len(pages)} pages)"
        logging.info(f"   Process {i}: pages {page_str}")
    
    # Run batches in parallel
    successful_batches = 0
    failed_batches = 0
    
    start_time = time.time()
    
    with ProcessPoolExecutor(max_workers=MAX_PROCESSES) as executor:
        # Submit all batches
        future_to_batch = {}
        for pages in batches:
            future = executor.submit(run_spider_batch, pages, CATEGORY)
            future_to_batch[future] = pages
        
        # Wait for all batches to complete
        for future in as_completed(future_to_batch):
            pages = future_to_batch[future]
            try:
                success, batch_name, output = future.result()
                if success:
                    successful_batches += 1
                    logging.info(f"✅ {batch_name} completed successfully")
                else:
                    failed_batches += 1
                    logging.error(f"❌ {batch_name} failed: {output}")
            except Exception as e:
                failed_batches += 1
                logging.error(f"💥 Exception in {batch_name}: {e}")
    
    end_time = time.time()
    duration = end_time - start_time
    
    # Final summary
    logging.info("=" * 60)
    logging.info("📈 FINAL SUMMARY")
    logging.info("=" * 60)
    logging.info(f"⏱️  Total duration: {duration:.2f} seconds ({duration/3600:.2f} hours)")
    logging.info(f"✅ Successful batches: {successful_batches}/{len(batches)}")
    logging.info(f"❌ Failed batches: {failed_batches}/{len(batches)}")
    logging.info(f"📊 Success rate: {(successful_batches/len(batches)*100):.1f}%")
    
    if failed_batches == 0:
        logging.info("🎉 All batches completed successfully!")
    else:
        logging.warning(f"⚠️  {failed_batches} batches failed. Check logs for details.")
    
    logging.info("=" * 60)

if __name__ == "__main__":
    main() 