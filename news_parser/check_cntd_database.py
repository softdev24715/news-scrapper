#!/usr/bin/env python3
"""
Check CNTD Database

This script analyzes the CNTD database table to understand the data.
Usage: python check_cntd_database.py
"""

import os
import sys
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, func

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from news_parser.models import CNTDDocument, init_db

def analyze_cntd_database():
    """Analyze the CNTD database table"""
    
    # Get database URL from environment
    db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres')
    
    try:
        session = init_db(db_url)
        print("✅ Connected to database")
        
        # Get total count
        total_count = session.query(CNTDDocument).count()
        print(f"\n📊 Total CNTD documents: {total_count}")
        
        if total_count == 0:
            print("❌ No CNTD documents found in database")
            return
        
        # Get count by date (last 7 days)
        week_ago = datetime.utcnow() - timedelta(days=7)
        recent_count = session.query(CNTDDocument).filter(
            CNTDDocument.created_at >= week_ago
        ).count()
        print(f"📅 Documents added in last 7 days: {recent_count}")
        
        # Get count by today
        today = datetime.utcnow().date()
        today_count = session.query(CNTDDocument).filter(
            func.date(CNTDDocument.created_at) == today
        ).count()
        print(f"📅 Documents added today: {today_count}")
        
        # Get sample documents
        print(f"\n📋 Sample documents (first 5):")
        sample_docs = session.query(CNTDDocument).limit(5).all()
        for i, doc in enumerate(sample_docs, 1):
            print(f"  {i}. doc_id: {doc.doc_id}")
            print(f"     title: {doc.title[:50]}{'...' if len(doc.title) > 50 else ''}")
            print(f"     created_at: {doc.created_at}")
            print(f"     published_at: {doc.published_at_iso}")
            print()
        
        # Check for duplicates
        print("🔍 Checking for duplicate doc_ids...")
        duplicate_query = session.query(
            CNTDDocument.doc_id,
            func.count(CNTDDocument.doc_id).label('count')
        ).group_by(CNTDDocument.doc_id).having(func.count(CNTDDocument.doc_id) > 1)
        
        duplicates = duplicate_query.all()
        if duplicates:
            print(f"⚠️  Found {len(duplicates)} duplicate doc_ids:")
            for doc_id, count in duplicates[:10]:  # Show first 10
                print(f"     {doc_id}: {count} times")
            if len(duplicates) > 10:
                print(f"     ... and {len(duplicates) - 10} more")
        else:
            print("✅ No duplicate doc_ids found")
        
        # Get documents by date range
        print(f"\n📈 Documents by date (last 10 days):")
        for i in range(10):
            date = datetime.utcnow().date() - timedelta(days=i)
            count = session.query(CNTDDocument).filter(
                func.date(CNTDDocument.created_at) == date
            ).count()
            print(f"  {date}: {count} documents")
        
        # Check for documents with empty text
        empty_text_count = session.query(CNTDDocument).filter(
            CNTDDocument.text == '' or CNTDDocument.text.is_(None)
        ).count()
        print(f"\n📝 Documents with empty text: {empty_text_count}")
        
        # Check for documents with long titles
        long_title_count = session.query(CNTDDocument).filter(
            func.length(CNTDDocument.title) > 500
        ).count()
        print(f"📝 Documents with long titles (>500 chars): {long_title_count}")
        
        # Get the most recent documents
        print(f"\n🕒 Most recent documents:")
        recent_docs = session.query(CNTDDocument).order_by(
            CNTDDocument.created_at.desc()
        ).limit(5).all()
        
        for i, doc in enumerate(recent_docs, 1):
            print(f"  {i}. {doc.doc_id} - {doc.created_at}")
        
        session.close()
        
    except Exception as e:
        print(f"❌ Error analyzing database: {e}")
        import traceback
        traceback.print_exc()

def check_table_structure():
    """Check the table structure"""
    db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres')
    
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            # Check if table exists
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'docs_cntd'
                );
            """))
            
            if not result.scalar():
                print("❌ Table 'docs_cntd' does not exist!")
                return
            
            # Get table structure
            result = conn.execute(text("""
                SELECT column_name, data_type, is_nullable, character_maximum_length
                FROM information_schema.columns 
                WHERE table_name = 'docs_cntd'
                ORDER BY ordinal_position;
            """))
            
            print("📋 Table structure:")
            for row in result:
                nullable = "NULL" if row[2] == "YES" else "NOT NULL"
                max_length = f"({row[3]})" if row[3] else ""
                print(f"  {row[0]}: {row[1]}{max_length} {nullable}")
            
            # Get table size
            result = conn.execute(text("""
                SELECT 
                    pg_size_pretty(pg_total_relation_size('docs_cntd')) as total_size,
                    pg_size_pretty(pg_relation_size('docs_cntd')) as table_size,
                    pg_size_pretty(pg_total_relation_size('docs_cntd') - pg_relation_size('docs_cntd')) as index_size;
            """))
            
            size_info = result.fetchone()
            print(f"\n💾 Table size: {size_info[0]} (table: {size_info[1]}, indexes: {size_info[2]})")
            
    except Exception as e:
        print(f"❌ Error checking table structure: {e}")

def main():
    """Main function"""
    print("🔍 CNTD Database Analysis")
    print("=" * 50)
    
    check_table_structure()
    print("\n" + "=" * 50)
    analyze_cntd_database()

if __name__ == "__main__":
    main() 