#!/usr/bin/env python3
"""
Retry Failed CNTD Documents

This script reads the failed documents log and retries saving them to the database.
Usage: python retry_failed_cntd.py
"""

import json
import logging
import os
import sys
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from news_parser.models import CNTDDocument, init_db

def load_failed_documents(log_file_path):
    """Load failed documents from log file"""
    failed_docs = []
    
    if not os.path.exists(log_file_path):
        logging.warning(f"Log file not found: {log_file_path}")
        return failed_docs
    
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    doc_data = json.loads(line)
                    failed_docs.append(doc_data)
                except json.JSONDecodeError as e:
                    logging.error(f"Error parsing line {line_num}: {e}")
                    continue
                    
        logging.info(f"Loaded {len(failed_docs)} failed documents from {log_file_path}")
        return failed_docs
        
    except Exception as e:
        logging.error(f"Error reading log file {log_file_path}: {e}")
        return failed_docs

def retry_save_document(session, item_data):
    """Retry saving a single document to the database"""
    try:
        # Create CNTDDocument object
        cntd_doc = CNTDDocument(
            id=item_data['id'],
            doc_id=item_data['doc_id'],
            title=item_data['title'],
            requisites=item_data['requisites'],
            text=item_data['text'],
            url=item_data['url'],
            parsed_at=item_data['parsed_at'],
            published_at_iso=item_data.get('published_at_iso')
        )
        
        session.add(cntd_doc)
        session.commit()
        
        logging.info(f"✅ Successfully retried document: {item_data['doc_id']}")
        return True, None
        
    except IntegrityError as e:
        session.rollback()
        logging.warning(f"🔄 Duplicate document (already exists): {item_data['doc_id']}")
        return True, "duplicate"  # Consider duplicate as success
        
    except SQLAlchemyError as e:
        session.rollback()
        error_msg = f"Database error: {str(e)}"
        logging.error(f"❌ Database error retrying document {item_data['doc_id']}: {error_msg}")
        return False, error_msg
        
    except Exception as e:
        session.rollback()
        error_msg = f"Unexpected error: {str(e)}"
        logging.error(f"❌ Error retrying document {item_data['doc_id']}: {error_msg}")
        return False, error_msg

def main():
    """Main function"""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('cntd_retry.log'),
            logging.StreamHandler()
        ]
    )
    
    logging.info("🔄 Starting CNTD failed documents retry process")
    
    # Get database URL from environment
    db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres')
    
    # Load failed documents
    logs_dir = os.path.join(os.path.dirname(__file__), 'logs')
    failed_log_file = os.path.join(logs_dir, 'cntd_failed_documents.log')
    
    failed_docs = load_failed_documents(failed_log_file)
    
    if not failed_docs:
        logging.info("No failed documents to retry")
        return
    
    # Connect to database
    try:
        session = init_db(db_url)
        logging.info("Connected to database")
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        return
    
    # Retry each failed document
    successful_retries = 0
    failed_retries = 0
    duplicates_found = 0
    
    logging.info(f"Retrying {len(failed_docs)} failed documents...")
    
    for i, doc_data in enumerate(failed_docs, 1):
        doc_id = doc_data.get('doc_id', 'unknown')
        logging.info(f"Retrying document {i}/{len(failed_docs)}: {doc_id}")
        
        # Get the item data from the log entry
        item_data = doc_data.get('item_data', {})
        if not item_data:
            logging.error(f"No item data found for document {doc_id}")
            failed_retries += 1
            continue
        
        # Retry saving the document
        success, result = retry_save_document(session, item_data)
        
        if success:
            successful_retries += 1
            if result == "duplicate":
                duplicates_found += 1
        else:
            failed_retries += 1
    
    # Close database session
    session.close()
    
    # Final summary
    logging.info("=" * 60)
    logging.info("📈 RETRY SUMMARY")
    logging.info("=" * 60)
    logging.info(f"📊 Total failed documents: {len(failed_docs)}")
    logging.info(f"✅ Successful retries: {successful_retries}")
    logging.info(f"🔄 Duplicates found: {duplicates_found}")
    logging.info(f"❌ Failed retries: {failed_retries}")
    logging.info(f"📊 Success rate: {(successful_retries/len(failed_docs)*100):.1f}%")
    
    if failed_retries == 0:
        logging.info("🎉 All failed documents retried successfully!")
        
        # Optionally, backup the original log file
        backup_file = f"{failed_log_file}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            os.rename(failed_log_file, backup_file)
            logging.info(f"📁 Backed up original log file to: {backup_file}")
        except Exception as e:
            logging.warning(f"Could not backup log file: {e}")
    else:
        logging.warning(f"⚠️  {failed_retries} documents still failed. Check logs for details.")
    
    logging.info("=" * 60)

if __name__ == "__main__":
    main() 