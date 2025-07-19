#!/usr/bin/env python3
"""
Debug script to check what document IDs are actually on specific pages
"""

import asyncio
import aiohttp
import json
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def check_page_content(page: int, category: int = 3):
    """Check what document IDs are actually on a specific page"""
    base_url = "https://docs.cntd.ru/api/search"
    search_url = f"{base_url}?category={category}&page={page}"
    
    async with aiohttp.ClientSession() as session:
        try:
            logger.info(f"Fetching page {page}: {search_url}")
            
            async with session.get(search_url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if 'data' in data and isinstance(data['data'], list):
                        doc_ids = []
                        for item in data['data']:
                            if 'id' in item:
                                doc_ids.append(str(item['id']))
                        
                        logger.info(f"Page {page}: Found {len(doc_ids)} document IDs")
                        logger.info(f"Page {page} doc_ids: {doc_ids}")
                        return doc_ids
                    else:
                        logger.warning(f"Page {page}: Unexpected response structure")
                        return []
                else:
                    logger.warning(f"Page {page}: HTTP {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Page {page}: Error: {e}")
            return []

async def main():
    """Check specific pages for debugging"""
    pages_to_check = [262, 263, 429, 430]
    
    print("="*60)
    print("DEBUGGING PAGE CONTENT")
    print("="*60)
    
    for page in pages_to_check:
        print(f"\nChecking page {page}:")
        doc_ids = await check_page_content(page)
        print(f"Page {page} contains: {doc_ids}")
        
        # Check for specific document IDs
        if '1313277144' in doc_ids:
            print(f"  ✓ Document ID 1313277144 found on page {page}")
        else:
            print(f"  ✗ Document ID 1313277144 NOT found on page {page}")
            
        if '1312842122' in doc_ids:
            print(f"  ✓ Document ID 1312842122 found on page {page}")
        else:
            print(f"  ✗ Document ID 1312842122 NOT found on page {page}")

if __name__ == "__main__":
    asyncio.run(main()) 