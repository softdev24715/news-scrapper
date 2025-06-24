#!/usr/bin/env python3
import requests
import xml.etree.ElementTree as ET
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def test_meduza_rss():
    """Test fetching and parsing Meduza RSS feed"""
    url = 'https://meduza.io/rss/all'
    
    try:
        # Fetch RSS feed
        resp = requests.get(url, verify=False, timeout=30)
        print(f"Status: {resp.status_code}")
        print(f"Content length: {len(resp.text)}")
        
        if resp.status_code == 200:
            # Parse RSS
            root = ET.fromstring(resp.text)
            items = root.findall('.//item')
            
            print(f"Found {len(items)} items")
            
            # Show first 3 items
            for i, item in enumerate(items[:3]):
                title_elem = item.find('title')
                link_elem = item.find('link')
                desc_elem = item.find('description')
                
                title = title_elem.text if title_elem is not None else 'No title'
                link = link_elem.text if link_elem is not None else 'No link'
                desc = desc_elem.text if desc_elem is not None else 'No description'
                
                print(f"\nItem {i+1}:")
                print(f"Title: {title}")
                print(f"Link: {link}")
                print(f"Description: {desc[:100]}...")
                
            return True
        else:
            print(f"Failed to fetch RSS: {resp.status_code}")
            return False
            
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    test_meduza_rss() 