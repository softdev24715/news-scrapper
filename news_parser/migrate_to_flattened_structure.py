#!/usr/bin/env python3
"""
Migration script to convert from JSON metadata structure to flattened database structure
"""

import os
import sys
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_current_structure():
    """Check if tables are using JSON metadata or flattened structure"""
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Check articles table structure
            result = conn.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'articles' 
                ORDER BY ordinal_position
            """))
            
            articles_columns = [row[0] for row in result.fetchall()]
            
            # Check legal_documents table structure
            result = conn.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'legal_documents' 
                ORDER BY ordinal_position
            """))
            
            legal_columns = [row[0] for row in result.fetchall()]
            
            print("📋 Current table structures:")
            print(f"Articles columns: {articles_columns}")
            print(f"Legal documents columns: {legal_columns}")
            
            # Check if using JSON metadata
            has_article_metadata = 'article_metadata' in articles_columns
            has_law_metadata = 'law_metadata' in legal_columns
            
            if has_article_metadata or has_law_metadata:
                print("⚠️ Found JSON metadata columns - migration needed")
                return True, articles_columns, legal_columns
            else:
                print("✅ Tables already using flattened structure")
                return False, articles_columns, legal_columns
                
    except Exception as e:
        logging.error(f"Error checking table structure: {str(e)}")
        return None, [], []

def migrate_articles_table():
    """Migrate articles table from JSON metadata to flattened structure"""
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Check if we need to migrate
            result = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.columns 
                WHERE table_name = 'articles' AND column_name = 'article_metadata'
            """))
            
            if result.scalar() == 0:
                print("ℹ️ Articles table already migrated")
                return True
            
            print("🔄 Migrating articles table...")
            
            # Create new flattened columns if they don't exist
            conn.execute(text("""
                ALTER TABLE articles 
                ADD COLUMN IF NOT EXISTS source VARCHAR,
                ADD COLUMN IF NOT EXISTS published_at INTEGER,
                ADD COLUMN IF NOT EXISTS published_at_iso TIMESTAMP,
                ADD COLUMN IF NOT EXISTS url VARCHAR,
                ADD COLUMN IF NOT EXISTS header VARCHAR,
                ADD COLUMN IF NOT EXISTS parsed_at INTEGER
            """))
            
            # Update data from JSON metadata
            conn.execute(text("""
                UPDATE articles 
                SET 
                    source = article_metadata->>'source',
                    published_at = (article_metadata->>'published_at')::integer,
                    published_at_iso = (article_metadata->>'published_at_iso')::timestamp,
                    url = article_metadata->>'url',
                    header = article_metadata->>'header',
                    parsed_at = (article_metadata->>'parsed_at')::integer
                WHERE article_metadata IS NOT NULL
            """))
            
            # Get count of migrated records
            result = conn.execute(text("SELECT COUNT(*) FROM articles WHERE source IS NOT NULL"))
            migrated_count = result.scalar()
            
            print(f"✅ Migrated {migrated_count} articles")
            
            # Drop the old JSON column
            conn.execute(text("ALTER TABLE articles DROP COLUMN IF EXISTS article_metadata"))
            print("🗑️ Dropped article_metadata column")
            
            conn.commit()
            return True
            
    except Exception as e:
        logging.error(f"Error migrating articles table: {str(e)}")
        return False

def migrate_legal_documents_table():
    """Migrate legal_documents table from JSON metadata to flattened structure"""
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Check if we need to migrate
            result = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.columns 
                WHERE table_name = 'legal_documents' AND column_name = 'law_metadata'
            """))
            
            if result.scalar() == 0:
                print("ℹ️ Legal documents table already migrated")
                return True
            
            print("🔄 Migrating legal_documents table...")
            
            # Create new flattened columns if they don't exist
            conn.execute(text("""
                ALTER TABLE legal_documents 
                ADD COLUMN IF NOT EXISTS original_id VARCHAR,
                ADD COLUMN IF NOT EXISTS doc_kind VARCHAR,
                ADD COLUMN IF NOT EXISTS title TEXT,
                ADD COLUMN IF NOT EXISTS source VARCHAR,
                ADD COLUMN IF NOT EXISTS url VARCHAR,
                ADD COLUMN IF NOT EXISTS published_at INTEGER,
                ADD COLUMN IF NOT EXISTS parsed_at INTEGER,
                ADD COLUMN IF NOT EXISTS jurisdiction VARCHAR,
                ADD COLUMN IF NOT EXISTS language VARCHAR,
                ADD COLUMN IF NOT EXISTS stage TEXT,
                ADD COLUMN IF NOT EXISTS discussion_period JSONB,
                ADD COLUMN IF NOT EXISTS explanatory_note JSONB,
                ADD COLUMN IF NOT EXISTS summary_reports JSONB,
                ADD COLUMN IF NOT EXISTS comment_stats JSONB
            """))
            
            # Update data from JSON metadata
            conn.execute(text("""
                UPDATE legal_documents 
                SET 
                    original_id = law_metadata->>'originalId',
                    doc_kind = law_metadata->>'docKind',
                    title = law_metadata->>'title',
                    source = law_metadata->>'source',
                    url = law_metadata->>'url',
                    published_at = (law_metadata->>'publishedAt')::integer,
                    parsed_at = (law_metadata->>'parsedAt')::integer,
                    jurisdiction = law_metadata->>'jurisdiction',
                    language = law_metadata->>'language',
                    stage = law_metadata->>'stage',
                    discussion_period = law_metadata->'discussionPeriod',
                    explanatory_note = law_metadata->'explanatoryNote',
                    summary_reports = law_metadata->'summaryReports',
                    comment_stats = law_metadata->'commentStats'
                WHERE law_metadata IS NOT NULL
            """))
            
            # Get count of migrated records
            result = conn.execute(text("SELECT COUNT(*) FROM legal_documents WHERE source IS NOT NULL"))
            migrated_count = result.scalar()
            
            print(f"✅ Migrated {migrated_count} legal documents")
            
            # Drop the old JSON column
            conn.execute(text("ALTER TABLE legal_documents DROP COLUMN IF EXISTS law_metadata"))
            print("🗑️ Dropped law_metadata column")
            
            conn.commit()
            return True
            
    except Exception as e:
        logging.error(f"Error migrating legal_documents table: {str(e)}")
        return False

def verify_migration():
    """Verify that migration was successful"""
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Check articles table
            result = conn.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'articles' 
                ORDER BY ordinal_position
            """))
            
            articles_columns = [row[0] for row in result.fetchall()]
            
            # Check legal_documents table
            result = conn.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'legal_documents' 
                ORDER BY ordinal_position
            """))
            
            legal_columns = [row[0] for row in result.fetchall()]
            
            print("\n📋 Post-migration table structures:")
            print(f"Articles columns: {articles_columns}")
            print(f"Legal documents columns: {legal_columns}")
            
            # Check for JSON metadata columns
            has_article_metadata = 'article_metadata' in articles_columns
            has_law_metadata = 'law_metadata' in legal_columns
            
            if has_article_metadata or has_law_metadata:
                print("❌ Migration incomplete - JSON metadata columns still exist")
                return False
            else:
                print("✅ Migration successful - all tables using flattened structure")
                return True
                
    except Exception as e:
        logging.error(f"Error verifying migration: {str(e)}")
        return False

def main():
    print("🔧 Starting database structure migration...")
    
    # Check current structure
    needs_migration, articles_cols, legal_cols = check_current_structure()
    
    if needs_migration is None:
        print("❌ Could not determine current structure")
        sys.exit(1)
    
    if not needs_migration:
        print("✅ No migration needed")
        return
    
    # Perform migration
    print("\n🔄 Starting migration process...")
    
    # Migrate articles table
    if not migrate_articles_table():
        print("❌ Failed to migrate articles table")
        sys.exit(1)
    
    # Migrate legal documents table
    if not migrate_legal_documents_table():
        print("❌ Failed to migrate legal documents table")
        sys.exit(1)
    
    # Verify migration
    if not verify_migration():
        print("❌ Migration verification failed")
        sys.exit(1)
    
    print("\n✅ Migration completed successfully!")
    print("📊 Database now uses flattened structure for better performance and querying")

if __name__ == "__main__":
    main() 