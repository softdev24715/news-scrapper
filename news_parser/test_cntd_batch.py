#!/usr/bin/env python3
"""
Test CNTD Spider Batch

This script runs a single CNTD spider batch for testing.
Usage: python test_cntd_batch.py --start-page 1 --end-page 10
"""

import subprocess
import argparse
import logging
import os
import sys

def main():
    parser = argparse.ArgumentParser(description='Test a single CNTD spider batch')
    parser.add_argument('--start-page', type=int, default=1, 
                       help='Starting page number (default: 1)')
    parser.add_argument('--end-page', type=int, default=10, 
                       help='Ending page number (default: 10)')
    parser.add_argument('--category', type=str, default='3', 
                       help='CNTD category (default: 3)')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logging.info(f"🧪 Testing CNTD spider batch: pages {args.start_page}-{args.end_page}")
    
    # Build command
    cmd = [
        'scrapy', 'crawl', 'cntd',
        '-a', f'start_page={args.start_page}',
        '-a', f'end_page={args.end_page}',
        '-a', f'category={args.category}',
        '-s', 'LOG_LEVEL=INFO'
    ]
    
    logging.info(f"Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        if result.returncode == 0:
            logging.info("✅ Test batch completed successfully!")
            if result.stdout:
                logging.info("Output:")
                for line in result.stdout.splitlines():
                    if line.strip():
                        logging.info(f"  {line.strip()}")
        else:
            logging.error("❌ Test batch failed!")
            if result.stderr:
                logging.error("Error output:")
                for line in result.stderr.splitlines():
                    if line.strip():
                        logging.error(f"  {line.strip()}")
                        
    except subprocess.TimeoutExpired:
        logging.error("⏰ Test batch timed out after 1 hour")
    except Exception as e:
        logging.error(f"💥 Test batch error: {e}")

if __name__ == "__main__":
    main() 