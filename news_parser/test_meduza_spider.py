#!/usr/bin/env python3
"""
Test script to verify Meduza spider can save data to database
"""
import os
import sys
import subprocess
import time
from datetime import datetime

def test_meduza_spider():
    print("Testing Meduza Spider Database Save")
    print("=" * 40)
    
    # Activate virtual environment
    venv_path = os.path.join(os.path.dirname(__file__), '..', 'venv', 'bin', 'activate')
    
    # Set environment variables
    env = os.environ.copy()
    env['PYTHONPATH'] = os.path.dirname(__file__)
    
    # Run the spider
    print("Running Meduza spider...")
    try:
        result = subprocess.run([
            'python', '-m', 'scrapy', 'crawl', 'meduza'
        ], cwd=os.path.dirname(__file__), env=env, capture_output=True, text=True)
        
        print(f"Spider exit code: {result.returncode}")
        print(f"Spider stdout: {result.stdout}")
        print(f"Spider stderr: {result.stderr}")
        
        if result.returncode == 0:
            print("✓ Spider ran successfully")
            
            # Check if any articles were saved
            time.sleep(2)  # Give database time to update
            
            # Try to query the database to see if articles were saved
            try:
                from sqlalchemy import create_engine, text
                from dotenv import load_dotenv
                
                load_dotenv()
                db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres')
                engine = create_engine(db_url)
                
                with engine.connect() as conn:
                    # Check articles table
                    result = conn.execute(text("SELECT COUNT(*) FROM articles WHERE source = 'meduza'"))
                    article_count = result.scalar()
                    print(f"✓ Found {article_count} articles from Meduza in database")
                    
                    if article_count > 0:
                        # Show latest article
                        result = conn.execute(text("""
                            SELECT id, header, url, published_at_iso 
                            FROM articles 
                            WHERE source = 'meduza' 
                            ORDER BY created_at DESC 
                            LIMIT 1
                        """))
                        latest = result.fetchone()
                        if latest:
                            print(f"✓ Latest article: {latest[1]}")
                            print(f"  URL: {latest[2]}")
                            print(f"  Published: {latest[3]}")
                    
                    # Check spider status
                    result = conn.execute(text("SELECT status, last_update FROM spider_status WHERE name = 'meduza'"))
                    spider_status = result.fetchone()
                    if spider_status:
                        print(f"✓ Spider status: {spider_status[0]}")
                        print(f"  Last update: {spider_status[1]}")
                
            except Exception as e:
                print(f"✗ Error checking database: {e}")
                
        else:
            print("✗ Spider failed to run")
            return False
            
    except Exception as e:
        print(f"✗ Error running spider: {e}")
        return False
    
    print("\n" + "=" * 40)
    print("✓ Test completed!")
    return True

if __name__ == "__main__":
    test_meduza_spider() 