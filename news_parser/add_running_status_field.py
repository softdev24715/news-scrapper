#!/usr/bin/env python3
"""
Script to add running_status field to spider_status table
This separates manual control (scheduled/disabled) from operational status (running/error)
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

def add_running_status_field():
    """Add running_status field to spider_status table"""
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            print("🔄 Adding running_status field to spider_status table...")
            
            # Add the new running_status field
            conn.execute(text("""
                ALTER TABLE spider_status 
                ADD COLUMN IF NOT EXISTS running_status VARCHAR DEFAULT 'idle'
            """))
            
            # Update existing records to set proper initial values
            # If status is 'running', set running_status to 'running' and status to 'scheduled'
            conn.execute(text("""
                UPDATE spider_status 
                SET running_status = 'running', status = 'scheduled' 
                WHERE status = 'running'
            """))
            
            # If status is 'error', set running_status to 'error' and status to 'scheduled'
            conn.execute(text("""
                UPDATE spider_status 
                SET running_status = 'error', status = 'scheduled' 
                WHERE status = 'error'
            """))
            
            # If status is 'scheduled' or 'disabled', set running_status to 'idle'
            conn.execute(text("""
                UPDATE spider_status 
                SET running_status = 'idle' 
                WHERE status IN ('scheduled', 'disabled')
            """))
            
            conn.commit()
            print("✅ Added running_status field and updated existing data")
            
            # Show the new table structure
            result = conn.execute(text("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = 'spider_status' 
                ORDER BY ordinal_position
            """))
            
            print("\n📋 Updated spider_status table structure:")
            print("-" * 60)
            for col in result.fetchall():
                nullable = "NULL" if col[2] == "YES" else "NOT NULL"
                default = f"DEFAULT {col[3]}" if col[3] else ""
                print(f"{col[0]:<20} {col[1]:<15} {nullable:<10} {default}")
            
            # Show sample data
            result = conn.execute(text("""
                SELECT name, status, running_status, last_update 
                FROM spider_status 
                ORDER BY name 
                LIMIT 5
            """))
            
            print("\n📊 Sample data after migration:")
            print("-" * 60)
            for row in result.fetchall():
                print(f"{row[0]:<15} {row[1]:<10} {row[2]:<10} {row[3]}")
            
            return True
            
    except Exception as e:
        logging.error(f"Error adding running_status field: {str(e)}")
        return False

def verify_migration():
    """Verify that the migration was successful"""
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Check if running_status field exists
            result = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.columns 
                WHERE table_name = 'spider_status' AND column_name = 'running_status'
            """))
            
            if result.scalar() == 0:
                print("❌ running_status field not found")
                return False
            
            # Check data integrity
            result = conn.execute(text("""
                SELECT COUNT(*) FROM spider_status 
                WHERE running_status IS NULL
            """))
            
            if result.scalar() > 0:
                print("❌ Found NULL values in running_status field")
                return False
            
            print("✅ Migration verification successful")
            return True
            
    except Exception as e:
        logging.error(f"Error verifying migration: {str(e)}")
        return False

def main():
    print("🔧 Adding running_status field to spider_status table...")
    
    # Add the new field
    if not add_running_status_field():
        print("❌ Failed to add running_status field")
        sys.exit(1)
    
    # Verify migration
    if not verify_migration():
        print("❌ Migration verification failed")
        sys.exit(1)
    
    print("\n✅ Migration completed successfully!")
    print("\n📋 New status system:")
    print("- status field: 'scheduled' or 'disabled' (manual control)")
    print("- running_status field: 'idle', 'running', or 'error' (operational status)")
    print("\n💡 Benefits:")
    print("- No more status conflicts between manual control and operational status")
    print("- Clear separation of concerns")
    print("- Better tracking of spider states")

if __name__ == "__main__":
    main() 