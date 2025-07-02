#!/usr/bin/env python3
"""
Script to update all spiders to use BaseSpider for date range handling.
This will allow spiders to collect news from both yesterday and today.
"""

import os
import re
from pathlib import Path

def update_spider_file(file_path):
    """Update a single spider file to use BaseSpider"""
    print(f"Updating {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    
    # Add import for BaseSpider
    if 'from news_parser.spiders.base_spider import BaseSpider' not in content:
        # Find the import section and add BaseSpider import
        import_pattern = r'(import scrapy\n)'
        if re.search(import_pattern, content):
            content = re.sub(import_pattern, r'\1from news_parser.spiders.base_spider import BaseSpider\n', content)
    
    # Update class inheritance
    # Pattern for different spider types
    patterns = [
        (r'class (\w+)Spider\(scrapy\.Spider\):', r'class \1Spider(BaseSpider, scrapy.Spider):'),
        (r'class (\w+)Spider\(XMLFeedSpider\):', r'class \1Spider(BaseSpider, XMLFeedSpider):'),
    ]
    
    for pattern, replacement in patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            break
    
    # Update __init__ method to handle date parameters
    init_pattern = r'def __init__\(self, \*args, \*\*kwargs\):'
    if re.search(init_pattern, content):
        # Replace with new __init__ that handles date parameters
        new_init = '''    def __init__(self, start_date=None, end_date=None, *args, **kwargs):
        # Initialize BaseSpider first for date range handling
        BaseSpider.__init__(self, start_date=start_date, end_date=end_date, *args, **kwargs)
        # Initialize the original spider class
        super().__init__(*args, **kwargs)'''
        
        content = re.sub(init_pattern, new_init, content)
    
    # Update date filtering logic
    # Remove target_date/today assignments
    content = re.sub(r'self\.target_date = datetime\.now\(\)\.strftime\(\'%Y-%m-%d\'\)', '', content)
    content = re.sub(r'self\.today = datetime\.now\(\)\.strftime\(\'%Y-%m-%d\'\)', '', content)
    
    # Update date comparison logic
    # Replace "if date_str != self.target_date:" with base spider method
    content = re.sub(
        r'if dt\.strftime\(\'%Y-%m-%d\'\) != self\.today:',
        'if not self.should_process_article(dt.strftime(\'%Y-%m-%d\'), url):',
        content
    )
    content = re.sub(
        r'if date_str != self\.target_date:',
        'if not self.should_process_article(date_str, url):',
        content
    )
    
    # Update date parsing to use base spider method
    # This is more complex and might need manual review for each spider
    # For now, we'll just add a comment suggesting manual review
    if 'datetime.strptime(' in content and 'pubDate' in content:
        content += '\n        # TODO: Consider using self.parse_date_string() for consistent date parsing\n'
    
    # Only write if content changed
    if content != original_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  ✓ Updated {file_path}")
        return True
    else:
        print(f"  - No changes needed for {file_path}")
        return False

def main():
    """Main function to update all spider files"""
    spiders_dir = Path('news_parser/news_parser/spiders')
    
    if not spiders_dir.exists():
        print(f"Error: Spiders directory not found at {spiders_dir}")
        return
    
    spider_files = list(spiders_dir.glob('*.py'))
    spider_files = [f for f in spider_files if f.name != '__init__.py' and f.name != 'base_spider.py']
    
    print(f"Found {len(spider_files)} spider files to update:")
    for spider_file in spider_files:
        print(f"  - {spider_file.name}")
    
    print("\nStarting updates...")
    updated_count = 0
    
    for spider_file in spider_files:
        try:
            if update_spider_file(spider_file):
                updated_count += 1
        except Exception as e:
            print(f"  ✗ Error updating {spider_file}: {e}")
    
    print(f"\nUpdate complete! Updated {updated_count} out of {len(spider_files)} spider files.")
    print("\nNote: Some spiders may need manual review for date parsing logic.")
    print("Consider using self.parse_date_string() method from BaseSpider for consistent date handling.")

if __name__ == "__main__":
    main() 