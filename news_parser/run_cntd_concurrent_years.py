#!/usr/bin/env python3
"""
CNTD Concurrent Year Scraping Script
Runs 46 concurrent spiders for years 1980-2025
"""

import subprocess
import time
import logging
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import argparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cntd_concurrent_years.log'),
        logging.StreamHandler()
    ]
)

class CNTDConcurrentScraper:
    def __init__(self, category, start_page=1, end_page=500, max_workers=46):
        self.category = category
        self.start_page = start_page
        self.end_page = end_page
        self.max_workers = max_workers
        self.years = list(range(1980, 2026))  # 1980 to 2025 (46 years)
        self.processes = []
        self.results = {}
        
        # Create logs directory
        os.makedirs('logs', exist_ok=True)
        
        logging.info(f"Initializing CNTD Concurrent Scraper")
        logging.info(f"Category: {self.category}")
        logging.info(f"Page range: {self.start_page}-{self.end_page}")
        logging.info(f"Years: {len(self.years)} (1980-2025)")
        logging.info(f"Max concurrent workers: {self.max_workers}")

    def run_single_year(self, year):
        """Run spider for a single year"""
        try:
            # Create year-specific log file
            log_file = f"logs/cntd_year_{year}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            
            # Build scrapy command
            cmd = [
                'scrapy', 'crawl', 'cntd',
                '-a', f'category={self.category}',
                '-a', f'date={year}',
                '-a', f'start_page={self.start_page}',
                '-a', f'end_page={self.end_page}',
                '-L', 'INFO'
            ]
            
            logging.info(f"Starting year {year} with command: {' '.join(cmd)}")
            
            # Run the process
            process = subprocess.Popen(
                cmd,
                stdout=open(log_file, 'w'),
                stderr=subprocess.STDOUT,
                cwd='.',
                text=True
            )
            
            self.processes.append((year, process, log_file))
            
            # Wait for completion
            return_code = process.wait()
            
            if return_code == 0:
                logging.info(f"✅ Year {year} completed successfully")
                return {'year': year, 'status': 'success', 'return_code': return_code}
            else:
                logging.error(f"❌ Year {year} failed with return code {return_code}")
                return {'year': year, 'status': 'failed', 'return_code': return_code}
                
        except Exception as e:
            logging.error(f"❌ Error running year {year}: {e}")
            return {'year': year, 'status': 'error', 'error': str(e)}

    def run_concurrent(self):
        """Run all years concurrently"""
        logging.info(f"🚀 Starting concurrent scraping for {len(self.years)} years...")
        start_time = time.time()
        
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all year tasks
                future_to_year = {
                    executor.submit(self.run_single_year, year): year 
                    for year in self.years
                }
                
                # Process completed tasks
                completed = 0
                successful = 0
                failed = 0
                
                for future in as_completed(future_to_year):
                    year = future_to_year[future]
                    try:
                        result = future.result()
                        self.results[year] = result
                        completed += 1
                        
                        if result['status'] == 'success':
                            successful += 1
                        else:
                            failed += 1
                        
                        # Progress update
                        progress = (completed / len(self.years)) * 100
                        logging.info(f"📊 Progress: {completed}/{len(self.years)} ({progress:.1f}%) - "
                                   f"✅ {successful} successful, ❌ {failed} failed")
                        
                    except Exception as e:
                        logging.error(f"❌ Exception for year {year}: {e}")
                        self.results[year] = {'year': year, 'status': 'exception', 'error': str(e)}
                        failed += 1
                        completed += 1
        
        except KeyboardInterrupt:
            logging.info("⚠️  Received interrupt signal, stopping all processes...")
            self.stop_all_processes()
            return
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Final summary
        self.print_summary(duration, successful, failed)

    def stop_all_processes(self):
        """Stop all running processes"""
        logging.info("🛑 Stopping all running processes...")
        for year, process, log_file in self.processes:
            if process.poll() is None:  # Process is still running
                try:
                    process.terminate()
                    logging.info(f"Terminated process for year {year}")
                except Exception as e:
                    logging.error(f"Error terminating year {year}: {e}")

    def print_summary(self, duration, successful, failed):
        """Print final summary"""
        logging.info("=" * 60)
        logging.info("📋 FINAL SUMMARY")
        logging.info("=" * 60)
        logging.info(f"⏱️  Total duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        logging.info(f"✅ Successful years: {successful}")
        logging.info(f"❌ Failed years: {failed}")
        logging.info(f"📊 Success rate: {(successful/len(self.years)*100):.1f}%")
        
        # Show failed years
        if failed > 0:
            failed_years = [year for year, result in self.results.items() 
                          if result['status'] != 'success']
            logging.info(f"❌ Failed years: {sorted(failed_years)}")
        
        # Show successful years
        successful_years = [year for year, result in self.results.items() 
                          if result['status'] == 'success']
        logging.info(f"✅ Successful years: {sorted(successful_years)}")
        
        logging.info("=" * 60)

def main():
    parser = argparse.ArgumentParser(description='Run CNTD spiders concurrently for years 1980-2025')
    parser.add_argument('--category', type=int, required=True, 
                       help='Category number (e.g., 21, 22, 23)')
    parser.add_argument('--start-page', type=int, default=1,
                       help='Starting page number (default: 1)')
    parser.add_argument('--end-page', type=int, default=500,
                       help='Ending page number (default: 500)')
    parser.add_argument('--max-workers', type=int, default=46,
                       help='Maximum concurrent workers (default: 46)')
    
    args = parser.parse_args()
    
    # Validate inputs
    if args.category < 1:
        logging.error("Category must be a positive integer")
        sys.exit(1)
    
    if args.start_page < 1 or args.end_page < args.start_page:
        logging.error("Invalid page range")
        sys.exit(1)
    
    if args.max_workers < 1 or args.max_workers > 50:
        logging.error("Max workers must be between 1 and 50")
        sys.exit(1)
    
    # Create and run scraper
    scraper = CNTDConcurrentScraper(
        category=args.category,
        start_page=args.start_page,
        end_page=args.end_page,
        max_workers=args.max_workers
    )
    
    # Handle interrupt signal
    def signal_handler(signum, frame):
        logging.info("⚠️  Received interrupt signal")
        scraper.stop_all_processes()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run the concurrent scraping
    scraper.run_concurrent()

if __name__ == '__main__':
    main() 