#!/usr/bin/env python3
"""
Database setup script for news scraper
Creates tables for both news articles and legal documents
"""

import os
import sys
import psycopg2
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres')

def setup_database():
    """Setup database tables and spider status"""
    try:
        # Connect to database
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Create spider_status table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS spider_status (
                    name VARCHAR PRIMARY KEY,
                    status VARCHAR DEFAULT 'enabled',
                    last_update TIMESTAMP(6)
                )
            """))
            
            # Create articles table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS articles (
                    id VARCHAR PRIMARY KEY,
                    text TEXT NOT NULL,
                    source VARCHAR NOT NULL,
                    published_at INTEGER NOT NULL,
                    published_at_iso TIMESTAMP NOT NULL,
                    url VARCHAR UNIQUE NOT NULL,
                    header VARCHAR NOT NULL,
                    parsed_at INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Create legal_documents table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS legal_documents (
                    id VARCHAR PRIMARY KEY,
                    text TEXT NOT NULL,
                    original_id VARCHAR,
                    doc_kind VARCHAR,
                    title TEXT,
                    source VARCHAR NOT NULL,
                    url VARCHAR UNIQUE NOT NULL,
                    published_at INTEGER,
                    parsed_at INTEGER,
                    jurisdiction VARCHAR,
                    language VARCHAR,
                    stage TEXT,
                    discussion_period JSONB,
                    explanatory_note JSONB,
                    summary_reports JSONB,
                    comment_stats JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.commit()
            print("✅ Database tables created successfully")
            
            # Add all spiders to spider_status if they don't exist
            all_spiders = [
                # News spiders (13)
                'tass', 'rbc', 'vedomosti', 'pnp', 'lenta', 'graininfo', 'forbes', 
                'interfax', 'izvestia', 'gazeta', 'rg', 'kommersant', 'ria', 'meduza',
                # Government and official spiders (4)
                'government', 'kremlin', 'regulation',
                # Legal document spiders (3)
                'pravo', 'sozd', 'eaeu'
            ]

            for spider in all_spiders:
                conn.execute(text("""
                    INSERT INTO spider_status (name, status) 
                    VALUES (:name, 'scheduled') 
                    ON CONFLICT (name) DO UPDATE SET status = 'scheduled'
                """), {"name": spider})
            
            conn.commit()
            print("✅ Added all 20 spiders to spider_status with 'scheduled' status")
            
            # Show table information
            print("\n📊 Database Tables Created:")
            print("- spider_status: Spider management table")
            print("- articles: News articles table")
            print("- legal_documents: Legal documents table")
            
            # Show spider count
            result = conn.execute(text("SELECT COUNT(*) FROM spider_status"))
            spider_count = result.scalar()
            print(f"\n🕷️ Total spiders in database: {spider_count}")
            
            # Show sample spiders
            result = conn.execute(text("SELECT name, status FROM spider_status LIMIT 5"))
            sample_spiders = result.fetchall()
            print(f"📋 Sample spiders: {[f'{s[0]} ({s[1]})' for s in sample_spiders]}")
            
    except Exception as e:
        print(f"❌ Error setting up database: {str(e)}")
        return False
    
    return True

def verify_legal_documents_structure():
    """Verify that legal documents table has correct structure"""
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Check if legal_documents table exists
            result = conn.execute(text("""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = 'legal_documents' 
                ORDER BY ordinal_position
            """))
            
            columns = result.fetchall()
            
            if not columns:
                print("❌ legal_documents table not found")
                return False
            
            print("\n📋 Legal Documents Table Structure:")
            print("-" * 50)
            for col in columns:
                nullable = "NULL" if col[2] == "YES" else "NOT NULL"
                print(f"{col[0]:<20} {col[1]:<15} {nullable}")
            
            return True
            
    except Exception as e:
        print(f"❌ Error verifying table structure: {str(e)}")
        return False

if __name__ == "__main__":
    print("🔧 Setting up News Scraper Database...")
    
    success = setup_database()
    if success:
        print("\n✅ Database setup completed successfully!")
        verify_legal_documents_structure()
    else:
        print("\n❌ Database setup failed!")
        sys.exit(1) 