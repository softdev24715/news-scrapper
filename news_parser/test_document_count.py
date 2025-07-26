#!/usr/bin/env python3
"""
Quick test of document counting with a single thematic ID
"""

import requests
import json

def test_single_thematic():
    """Test document counting with thematic 746501363"""
    thematic_id = 746501363
    
    url = "https://docs.cntd.ru/api/search"
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
    
    print(f"🧪 Testing thematic {thematic_id}...")
    print(f"URL: {url}")
    print(f"Params: {params}")
    print()
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract pagination info
        if 'pagination' in data:
            pagination = data['pagination']
            total_documents = pagination.get('total', 0)
            total_pages = pagination.get('last_page', 0)
            current_page = pagination.get('current_page', 0)
            per_page = pagination.get('per_page', 0)
            
            print(f"✅ Success!")
            print(f"📊 Results for Thematic {thematic_id}:")
            print(f"   Total Documents: {total_documents:,}")
            print(f"   Total Pages: {total_pages}")
            print(f"   Current Page: {current_page}")
            print(f"   Documents per Page: {per_page}")
            
            # Show sample data
            if 'data' in data and data['data']:
                sample_doc = data['data'][0]
                print(f"\n📄 Sample Document:")
                print(f"   ID: {sample_doc.get('id')}")
                print(f"   Title: {sample_doc.get('names', [''])[0][:100]}...")
                print(f"   Status: {sample_doc.get('status', {}).get('name', 'Unknown')}")
            
            return True
        else:
            print(f"❌ No pagination data found")
            print(f"Response keys: {list(data.keys())}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return False
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

if __name__ == '__main__':
    print("🔍 CNTD Document Count Test")
    print("=" * 50)
    
    success = test_single_thematic()
    
    if success:
        print(f"\n🎉 Test passed! You can now run the full document count:")
        print(f"   python count_total_documents.py")
    else:
        print(f"\n⚠️  Test failed. Please check the issues above.") 