#!/usr/bin/env python3
"""
Test script to demonstrate real-time saving of scraped data to database.
This script shows how data is saved one by one as it's scraped, rather than at the end.
"""

import subprocess
import time
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_realtime.log'),
        logging.StreamHandler()
    ]
)

def run_spider_with_realtime_saving(spider_name, max_items=20):
    """
    Run a spider and monitor real-time saving progress.
    
    Args:
        spider_name (str): Name of the spider to run
        max_items (int): Maximum number of items to scrape (for testing)
    """
    
    logging.info(f"🚀 Starting {spider_name} spider with real-time saving...")
    logging.info(f"📊 Will monitor progress and save data one by one to database")
    
    # Command to run the spider
    cmd = [
        'scrapy', 'crawl', spider_name,
        '-s', 'CLOSESPIDER_ITEMCOUNT=20',  # Stop after 20 items for testing
        '-L', 'INFO'
    ]
    
    try:
        # Start the spider process
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        
        logging.info(f"✅ Spider process started with PID: {process.pid}")
        
        # Monitor the output in real-time
        items_saved = 0
        start_time = time.time()
        
        for line in process.stdout:
            line = line.strip()
            
            # Print all output
            print(line)
            
            # Track specific events
            if "✅ Saved news article:" in line or "✅ Saved legal document:" in line:
                items_saved += 1
                elapsed = time.time() - start_time
                logging.info(f"📈 Progress: {items_saved} items saved in {elapsed:.1f}s")
                
            elif "🔄 Duplicate" in line:
                logging.info(f"🔄 Duplicate detected - skipping")
                
            elif "❌ Error" in line:
                logging.warning(f"❌ Error occurred during processing")
                
            elif "📊 Spider" in line and "final stats:" in line:
                logging.info(f"🎉 Spider completed! Final statistics:")
                
        # Wait for process to complete
        return_code = process.wait()
        
        if return_code == 0:
            logging.info(f"✅ Spider {spider_name} completed successfully!")
        else:
            logging.error(f"❌ Spider {spider_name} failed with return code: {return_code}")
            
    except KeyboardInterrupt:
        logging.info("🛑 Spider interrupted by user")
        process.terminate()
        process.wait()
    except Exception as e:
        logging.error(f"❌ Error running spider: {e}")

def main():
    """Main function to test real-time saving"""
    
    print("=" * 60)
    print("🧪 TESTING REAL-TIME DATA SAVING")
    print("=" * 60)
    print()
    print("This test demonstrates how data is saved to the database")
    print("one by one as it's scraped, rather than at the end.")
    print()
    print("Benefits:")
    print("✅ Fault tolerance - no data loss if process crashes")
    print("✅ Memory efficiency - no need to hold all data in memory")
    print("✅ Real-time monitoring - see progress as it happens")
    print("✅ Better debugging - isolate issues with individual items")
    print()
    
    # Test with Forbes spider (smaller dataset)
    print("Testing with Forbes spider (RSS feed, smaller dataset)...")
    run_spider_with_realtime_saving('forbes', max_items=10)
    
    print()
    print("=" * 60)
    print("✅ Test completed!")
    print("=" * 60)
    print()
    print("Check the logs above to see:")
    print("- Real-time saving progress")
    print("- Individual item processing")
    print("- Error handling for duplicates")
    print("- Final statistics")
    print()
    print("You can also check your database to see the saved data.")

if __name__ == "__main__":
    main() 