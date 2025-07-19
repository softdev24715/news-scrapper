#!/usr/bin/env python3
"""
Check Missing CNTD Pages

This script checks which pages from 1-500 are missing from the CNTD database.
Usage: python check_missing_pages.py
"""

import os
import sys
from datetime import datetime
from sqlalchemy import create_engine, text, func

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def check_missing_pages():
    """Check which pages are missing from the database"""
    
    # Get database URL from environment
    db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres')
    
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            print("🔍 Checking CNTD database for missing pages...")
            
            # Get total count
            result = conn.execute(text("SELECT COUNT(*) FROM docs_cntd"))
            total_count = result.scalar()
            print(f"📊 Total documents in database: {total_count}")
            
            # Get documents by page (extract page number from URL)
            result = conn.execute(text("""
                SELECT 
                    CASE 
                        WHEN url ~ '/document/([0-9]+)' 
                        THEN substring(url from '/document/([0-9]+)')::integer 
                        ELSE NULL 
                    END as doc_id,
                    url,
                    created_at
                FROM docs_cntd 
                WHERE url LIKE '%docs.cntd.ru/document/%'
                ORDER BY created_at DESC
            """))
            
            documents = result.fetchall()
            print(f"📋 Found {len(documents)} documents with valid URLs")
            
            # Extract page numbers from doc_ids (assuming doc_id corresponds to page order)
            # This is a rough estimation - CNTD might not have sequential doc_ids
            doc_ids = [doc[0] for doc in documents if doc[0] is not None]
            doc_ids.sort()
            
            print(f"📈 Document ID range: {min(doc_ids)} to {max(doc_ids)}")
            print(f"📈 Unique document IDs: {len(set(doc_ids))}")
            
            # Check for gaps in document IDs
            gaps = []
            for i in range(len(doc_ids) - 1):
                if doc_ids[i+1] - doc_ids[i] > 1:
                    gaps.append((doc_ids[i], doc_ids[i+1]))
            
            if gaps:
                print(f"⚠️  Found {len(gaps)} gaps in document IDs:")
                for start, end in gaps[:10]:  # Show first 10 gaps
                    print(f"     Gap: {start} to {end} (missing {end - start - 1} documents)")
                if len(gaps) > 10:
                    print(f"     ... and {len(gaps) - 10} more gaps")
            else:
                print("✅ No gaps found in document IDs")
            
            # Check recent activity
            result = conn.execute(text("""
                SELECT 
                    DATE(created_at) as date,
                    COUNT(*) as count
                FROM docs_cntd 
                WHERE created_at >= NOW() - INTERVAL '7 days'
                GROUP BY DATE(created_at)
                ORDER BY date DESC
            """))
            
            print(f"\n📅 Recent activity (last 7 days):")
            for date, count in result:
                print(f"  {date}: {count} documents")
            
            # Check for documents with errors
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM docs_cntd 
                WHERE text = '' OR text IS NULL
            """))
            empty_text_count = result.scalar()
            print(f"\n📝 Documents with empty text: {empty_text_count}")
            
            # Estimate pages processed
            # Assuming ~20 documents per page on average
            estimated_pages = total_count / 20
            print(f"\n📊 Estimation:")
            print(f"  Expected total: 10,000 documents (500 pages × 20 docs/page)")
            print(f"  Actual total: {total_count} documents")
            print(f"  Missing: {10000 - total_count} documents")
            print(f"  Estimated pages processed: {estimated_pages:.1f} out of 500")
            print(f"  Completion rate: {(total_count/10000)*100:.1f}%")
            
            if total_count < 10000:
                print(f"\n🔧 Recommendations:")
                print(f"  1. Check failed documents log: logs/cntd_failed_documents.log")
                print(f"  2. Run retry script: python retry_failed_cntd.py")
                print(f"  3. Check spider logs for failed pages")
                print(f"  4. Consider running specific missing page ranges")
            
    except Exception as e:
        print(f"❌ Error checking database: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function"""
    print("🔍 CNTD Missing Pages Analysis")
    print("=" * 50)
    check_missing_pages()

if __name__ == "__main__":
    main() 