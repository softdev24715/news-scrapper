#!/usr/bin/env python3
"""
Database monitoring script to watch data being saved in real-time.
Run this in a separate terminal while the spider is running.
"""

import time
import psycopg2
from datetime import datetime
import os

# Database connection settings (from your config)
DB_CONFIG = {
    'host': '90.156.204.42',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': '1e3Xdfsdf23'
}

def get_db_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        return None

def count_articles():
    """Count articles in the database"""
    conn = get_db_connection()
    if not conn:
        return 0
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM articles")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except Exception as e:
        print(f"❌ Error counting articles: {e}")
        return 0

def count_legal_documents():
    """Count legal documents in the database"""
    conn = get_db_connection()
    if not conn:
        return 0
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM legal_documents")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except Exception as e:
        print(f"❌ Error counting legal documents: {e}")
        return 0

def get_recent_items(limit=5):
    """Get recent items from both tables"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        
        # Get recent articles
        cursor.execute("""
            SELECT 'article' as type, id, article_metadata->>'source' as source, 
                   article_metadata->>'url' as url, created_at
            FROM articles 
            ORDER BY created_at DESC 
            LIMIT %s
        """, (limit,))
        articles = cursor.fetchall()
        
        # Get recent legal documents
        cursor.execute("""
            SELECT 'legal' as type, id, law_metadata->>'source' as source, 
                   law_metadata->>'url' as url, created_at
            FROM legal_documents 
            ORDER BY created_at DESC 
            LIMIT %s
        """, (limit,))
        legal_docs = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Combine and sort by creation time
        all_items = articles + legal_docs
        all_items.sort(key=lambda x: x[4], reverse=True)  # Sort by created_at
        return all_items[:limit]
        
    except Exception as e:
        print(f"❌ Error getting recent items: {e}")
        return []

def monitor_database():
    """Monitor database for new items in real-time"""
    
    print("=" * 60)
    print("📊 DATABASE MONITORING")
    print("=" * 60)
    print("Monitoring database for new items...")
    print("Press Ctrl+C to stop monitoring")
    print()
    
    # Get initial counts
    initial_articles = count_articles()
    initial_legal = count_legal_documents()
    
    print(f"📈 Initial counts:")
    print(f"   Articles: {initial_articles}")
    print(f"   Legal Documents: {initial_legal}")
    print(f"   Total: {initial_articles + initial_legal}")
    print()
    
    last_articles = initial_articles
    last_legal = initial_legal
    
    try:
        while True:
            # Get current counts
            current_articles = count_articles()
            current_legal = count_legal_documents()
            
            # Check for changes
            articles_added = current_articles - last_articles
            legal_added = current_legal - last_legal
            
            if articles_added > 0 or legal_added > 0:
                timestamp = datetime.now().strftime('%H:%M:%S')
                print(f"[{timestamp}] 🆕 New items detected:")
                
                if articles_added > 0:
                    print(f"   📰 Articles: +{articles_added} (Total: {current_articles})")
                
                if legal_added > 0:
                    print(f"   📋 Legal Documents: +{legal_added} (Total: {current_legal})")
                
                # Show recent items
                recent_items = get_recent_items(3)
                if recent_items:
                    print(f"   📝 Recent items:")
                    for item_type, item_id, source, url, created_at in recent_items:
                        url_short = url[:50] + "..." if url and len(url) > 50 else url
                        print(f"      • {item_type.upper()}: {source} - {url_short}")
                
                print()
                
                # Update last counts
                last_articles = current_articles
                last_legal = current_legal
            
            # Wait before next check
            time.sleep(2)
            
    except KeyboardInterrupt:
        print()
        print("🛑 Monitoring stopped by user")
        
        # Final summary
        final_articles = count_articles()
        final_legal = count_legal_documents()
        
        print()
        print("📊 Final Summary:")
        print(f"   Articles: {initial_articles} → {final_articles} (+{final_articles - initial_articles})")
        print(f"   Legal Documents: {initial_legal} → {final_legal} (+{final_legal - initial_legal})")
        print(f"   Total: {initial_articles + initial_legal} → {final_articles + final_legal} (+{(final_articles + final_legal) - (initial_articles + initial_legal)})")

def main():
    """Main function"""
    print("Database monitoring tool for real-time data saving")
    print()
    print("This tool monitors the database to see data being saved")
    print("in real-time as the spider scrapes and processes items.")
    print()
    
    # Test database connection
    print("Testing database connection...")
    conn = get_db_connection()
    if conn:
        print("✅ Database connection successful!")
        conn.close()
    else:
        print("❌ Database connection failed!")
        return
    
    print()
    monitor_database()

if __name__ == "__main__":
    main() 