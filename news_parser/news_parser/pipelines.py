# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
from datetime import datetime
import os
import logging
from .models import Article, LegalDocument, CNTDDocument, init_db
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy import create_engine
from sqlalchemy.sql import text


class NewsParserPipeline:
    def __init__(self):
        self.output_dir = 'output'
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        self.items = []
        self.processed_urls = set()  # Track processed URLs to avoid duplicates
        self.filename = None  # Store filename for the current run
        logging.info("Initializing NewsParserPipeline")

    def process_item(self, item, spider):
        # Convert item to dict if it isn't already
        item_dict = dict(item)
        
        # Handle CNTD spider - it yields raw JSON objects without metadata/lawMetadata
        if spider.name == 'cntd':
            # For CNTD, use document ID as unique key
            unique_key = f"cntd:{item_dict.get('id', 'unknown')}"
            
            # Check if we've already processed this document ID
            if unique_key in self.processed_urls:
                logging.warning(f"Duplicate document ID found: {unique_key}")
                return item
                
            # Add to processed set and items list
            self.processed_urls.add(unique_key)
            self.items.append(item_dict)
            logging.info(f"Processing CNTD document: {unique_key} (Total items: {len(self.items)})")
            return item
        
        # Handle other spiders with metadata/lawMetadata structure
        # Create unique key based on source + url (not the generated UUID)
        if 'lawMetadata' in item_dict:
            # Legal document
            source = item_dict['lawMetadata'].get('source', '')
            url = item_dict['lawMetadata'].get('url', '')
        else:
            # News article
            source = item_dict['metadata'].get('source', '')
            url = item_dict['metadata'].get('url', '')
        
        unique_key = f"{source}:{url}"
        
        # Check if we've already processed this URL
        if unique_key in self.processed_urls:
            logging.warning(f"Duplicate URL found: {unique_key}")
            return item
            
        # Add to processed set and items list
        self.processed_urls.add(unique_key)
        self.items.append(item_dict)
        logging.info(f"Processing item: {unique_key} from {spider.name} (Total items: {len(self.items)})")
        
        return item

    def open_spider(self, spider):
        # Create filename when spider starts with consistent timezone format
        timestamp = datetime.now().strftime('%Y-%m-%dT%H-%M-%S%z')
        # Ensure timezone format is consistent (e.g., +0000 instead of +00:00)
        timestamp = timestamp.replace(':', '')
        self.filename = f"{self.output_dir}/{spider.name}_{timestamp}.json"
        logging.info(f"Opening spider {spider.name}, will save to {self.filename}")
        logging.info(f"Current items count: {len(self.items)}")

    def close_spider(self, spider):
        if not self.items:
            logging.warning(f"No items collected for {spider.name}")
            return
            
        # Write all items to a single JSON file
        with open(self.filename, 'w', encoding='utf-8') as f:
            json.dump(self.items, f, ensure_ascii=False, indent=2)
        
        logging.info(f"Saved {len(self.items)} articles to {self.filename}")
        logging.info(f"Total processed URLs: {len(self.processed_urls)}")
        
        # Reset everything for next run
        self.items = []
        self.processed_urls = set()
        self.filename = None  # Clear the filename

class PostgreSQLPipeline:
    def __init__(self, db_url):
        self.db_url = db_url
        self.session = None
        self.items_processed = 0
        self.items_saved = 0
        self.items_failed = 0
        self.duplicates_found = 0
        logging.info("Initializing PostgreSQLPipeline")

    @classmethod
    def from_crawler(cls, crawler):
        db_url = crawler.settings.get('DATABASE_URL')
        if not db_url:
            raise ValueError("DATABASE_URL setting is required")
        return cls(db_url)

    def open_spider(self, spider):
        self.session = init_db(self.db_url)
        logging.info(f"Connected to database for spider {spider.name}")
        
        # Update spider running_status to 'running' when spider starts
        try:
            self.session.execute(
                text("""
                    UPDATE spider_status 
                    SET running_status = 'running', last_update = NOW() 
                    WHERE name = :name
                """),
                {"name": spider.name}
            )
            self.session.commit()
            logging.info(f"Updated spider {spider.name} running_status to 'running'")
        except Exception as e:
            logging.error(f"Error updating spider {spider.name} running_status to 'running': {str(e)}")

    def process_item(self, item, spider):
        self.items_processed += 1
        
        try:
            # Convert item to dict
            item_dict = dict(item)
            
            # Check if this is a CNTD document, legal document, or news article
            if 'doc_id' in item_dict:
                # This is a CNTD document
                success = self._save_cntd_document(item_dict)
            elif 'lawMetadata' in item_dict:
                # This is a legal document
                success = self._save_legal_document(item_dict)
            else:
                # This is a news article
                success = self._save_news_article(item_dict)
            
            if success:
                self.items_saved += 1
                # Log progress every 10 items
                if self.items_saved % 10 == 0:
                    logging.info(f"Spider {spider.name}: Saved {self.items_saved} items, processed {self.items_processed}, failed {self.items_failed}, duplicates {self.duplicates_found}")
            
            # Update spider running_status to show it's still running (less frequently)
            if self.items_saved % 5 == 0:
                try:
                    # Check current status first - don't update if manually stopped
                    result = self.session.execute(
                        text("SELECT status FROM spider_status WHERE name = :name"),
                        {"name": spider.name}
                    )
                    current_status = result.scalar()
                    
                    # Only update to 'running' if not manually disabled
                    if current_status != 'disabled':
                        self.session.execute(
                            text("""
                                UPDATE spider_status 
                                SET running_status = 'running', last_update = NOW() 
                                WHERE name = :name
                            """),
                            {"name": spider.name}
                        )
                        self.session.commit()
                except Exception as e:
                    logging.error(f"Error updating spider {spider.name} running_status: {str(e)}")
            
        except Exception as e:
            self.items_failed += 1
            logging.error(f"Error processing item: {str(e)}")
            # Don't raise the exception to continue processing other items
        
        return item

    def _save_news_article(self, item_dict):
        """Save a news article to the articles table - using flattened structure"""
        try:
            metadata = item_dict['metadata']
            
            article = Article(
                id=item_dict['id'],
                text=item_dict['text'],
                source=metadata.get('source', ''),
                published_at=metadata.get('published_at'),
                published_at_iso=metadata.get('published_at_iso'),
                url=metadata.get('url', ''),
                header=metadata.get('header', ''),
                parsed_at=metadata.get('parsed_at')
            )
            
            self.session.add(article)
            self.session.commit()
            
            # Extract source and URL for logging
            source = metadata.get('source', '')
            url = metadata.get('url', '')
            logging.info(f"✅ Saved news article: {source} - {url[:50]}...")
            return True
            
        except IntegrityError as e:
            self.session.rollback()
            self.duplicates_found += 1
            source = metadata.get('source', '')
            url = metadata.get('url', '')
            logging.warning(f"🔄 Duplicate news article: {source} - {url[:50]}...")
            return False
        except SQLAlchemyError as e:
            self.session.rollback()
            logging.error(f"❌ Database error saving news article: {str(e)}")
            return False
        except Exception as e:
            self.session.rollback()
            logging.error(f"❌ Error saving news article: {str(e)}")
            return False

    def _save_cntd_document(self, item_dict):
        """Save a CNTD document to the docs_cntd table"""
        try:
            # Debug logging to check field lengths
            logging.info(f"CNTD document field lengths:")
            logging.info(f"  doc_id: {len(str(item_dict.get('doc_id', '')))} chars")
            logging.info(f"  page_number: {item_dict.get('page_number')}")
            logging.info(f"  title: {len(str(item_dict.get('title', '')))} chars")
            logging.info(f"  requisites: {len(str(item_dict.get('requisites', '')))} chars")
            logging.info(f"  url: {len(str(item_dict.get('url', '')))} chars")
            logging.info(f"  published_at_iso: {item_dict.get('published_at_iso')}")
            
            cntd_doc = CNTDDocument(
                id=item_dict['id'],
                doc_id=item_dict['doc_id'],
                page_number=item_dict.get('page_number'),
                title=item_dict['title'],
                requisites=item_dict['requisites'],
                text=item_dict['text'],
                url=item_dict['url'],
                parsed_at=item_dict['parsed_at'],
                published_at_iso=item_dict.get('published_at_iso')
            )
            
            self.session.add(cntd_doc)
            self.session.commit()
            
            # Extract doc_id and URL for logging
            doc_id = item_dict['doc_id']
            url = item_dict['url']
            logging.info(f"✅ Saved CNTD document: {doc_id} - {url[:50]}...")
            return True
            
        except IntegrityError as e:
            self.session.rollback()
            self.duplicates_found += 1
            doc_id = item_dict['doc_id']
            url = item_dict['url']
            logging.warning(f"🔄 Duplicate CNTD document: {doc_id} - {url[:50]}...")
            return False
        except SQLAlchemyError as e:
            self.session.rollback()
            doc_id = item_dict['doc_id']
            error_msg = f"Database error saving CNTD document: {str(e)}"
            logging.error(f"❌ {error_msg}")
            
            # Log failed document for later retry
            self.log_failed_cntd_document(doc_id, error_msg, item_dict)
            return False
        except Exception as e:
            self.session.rollback()
            doc_id = item_dict['doc_id']
            error_msg = f"Error saving CNTD document: {str(e)}"
            logging.error(f"❌ {error_msg}")
            
            # Log failed document for later retry
            self.log_failed_cntd_document(doc_id, error_msg, item_dict)
            return False

    def _save_legal_document(self, item_dict):
        """Save a legal document to the legal_documents table - using flattened structure"""
        try:
            law_metadata = item_dict['lawMetadata']
            url = law_metadata.get('url', '')
            source = law_metadata.get('source', '')
            new_stage = law_metadata.get('stage')
            
            # Check if document already exists
            existing_doc = self.session.query(LegalDocument).filter(
                LegalDocument.url == url,
                LegalDocument.source == source
            ).first()
            
            if existing_doc:
                # Update existing document if stage has changed
                if existing_doc.stage != new_stage:
                    old_stage = existing_doc.stage
                    existing_doc.stage = new_stage
                    existing_doc.parsed_at = law_metadata.get('parsedAt')
                    existing_doc.updated_at = datetime.now()
                    
                    # Update other fields if they've changed
                    if law_metadata.get('title') and existing_doc.title != law_metadata.get('title'):
                        existing_doc.title = law_metadata.get('title')
                    if law_metadata.get('files') and existing_doc.files != law_metadata.get('files'):
                        existing_doc.files = law_metadata.get('files')
                    
                    self.session.commit()
                    logging.info(f"🔄 Updated legal document stage: '{old_stage}' -> '{new_stage}' for {source} - {url[:50]}...")
                    return True
                else:
                    logging.info(f"⏭️  No stage change for {source} - {url[:50]}...")
                    return True
            else:
                # Insert new document
                legal_doc = LegalDocument(
                    id=item_dict['id'],
                    text=item_dict['text'],
                    original_id=law_metadata.get('originalId'),
                    doc_kind=law_metadata.get('docKind'),
                    title=law_metadata.get('title'),
                    source=source,
                    url=url,
                    published_at=law_metadata.get('publishedAt'),
                    parsed_at=law_metadata.get('parsedAt'),
                    jurisdiction=law_metadata.get('jurisdiction'),
                    language=law_metadata.get('language'),
                    stage=new_stage,
                    discussion_period=law_metadata.get('discussionPeriod'),
                    explanatory_note=law_metadata.get('explanatoryNote'),
                    summary_reports=law_metadata.get('summaryReports'),
                    comment_stats=law_metadata.get('commentStats'),
                    files=law_metadata.get('files')
                )
                
                self.session.add(legal_doc)
                self.session.commit()
                logging.info(f"✅ Saved new legal document: {source} - {url[:50]}...")
                return True
            
        except IntegrityError as e:
            self.session.rollback()
            self.duplicates_found += 1
            logging.warning(f"🔄 Duplicate legal document: {source} - {url[:50]}...")
            return False
        except SQLAlchemyError as e:
            self.session.rollback()
            logging.error(f"❌ Database error saving legal document: {str(e)}")
            return False
        except Exception as e:
            self.session.rollback()
            logging.error(f"❌ Error saving legal document: {str(e)}")
            return False

    def close_spider(self, spider):
        if self.session:
            try:
                # Log final statistics
                logging.info(f"📊 Spider {spider.name} final stats:")
                logging.info(f"   - Items processed: {self.items_processed}")
                logging.info(f"   - Items saved: {self.items_saved}")
                logging.info(f"   - Items failed: {self.items_failed}")
                logging.info(f"   - Duplicates found: {self.duplicates_found}")
                
                # Always update running_status to 'idle' when spider completes successfully
                # (running_status is operational state, independent of manual control status)
                self.session.execute(
                    text("""
                        UPDATE spider_status 
                        SET running_status = 'idle', last_update = NOW() 
                        WHERE name = :name
                    """),
                    {"name": spider.name}
                )
                self.session.commit()
                logging.info(f"✅ Updated spider {spider.name} running_status to 'idle' - completed successfully")
            except Exception as e:
                self.session.rollback()
                logging.error(f"❌ Error updating spider running_status to 'idle': {str(e)}")
                # Try to set running_status to 'error' if we can't set it to 'idle'
                try:
                    self.session.execute(
                        text("""
                            UPDATE spider_status 
                            SET running_status = 'error', last_update = NOW() 
                            WHERE name = :name
                        """),
                        {"name": spider.name}
                    )
                    self.session.commit()
                    logging.info(f"⚠️ Set spider {spider.name} running_status to 'error' due to failure")
                except Exception as e2:
                    logging.error(f"❌ Failed to set spider {spider.name} running_status to 'error': {str(e2)}")
            finally:
                self.session.close()
                logging.info(f"🔌 Closed database connection for spider {spider.name}")

    def log_failed_cntd_document(self, doc_id, error_message, item_dict):
        """Log failed CNTD document with details for later retry"""
        try:
            # Create logs directory if it doesn't exist
            logs_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'logs')
            os.makedirs(logs_dir, exist_ok=True)
            
            # Create failed documents log file
            failed_log_file = os.path.join(logs_dir, 'cntd_failed_documents.log')
            
            # Prepare error entry
            error_entry = {
                'doc_id': doc_id,
                'error': error_message,
                'timestamp': datetime.now().isoformat(),
                'url': item_dict.get('url', ''),
                'title': item_dict.get('title', ''),
                'item_data': item_dict
            }
            
            # Append to log file
            with open(failed_log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(error_entry, ensure_ascii=False) + '\n')
                
            logging.info(f"📝 Logged failed document {doc_id} to {failed_log_file}")
            
        except Exception as log_error:
            logging.error(f"Failed to log failed document {doc_id}: {log_error}")

class LegalDocumentsPipeline:
    """Pipeline specifically for legal documents"""
    def __init__(self, db_url):
        self.db_url = db_url
        self.session = None
        logging.info("Initializing LegalDocumentsPipeline")

    @classmethod
    def from_crawler(cls, crawler):
        db_url = crawler.settings.get('DATABASE_URL')
        if not db_url:
            raise ValueError("DATABASE_URL setting is required")
        return cls(db_url)

    def open_spider(self, spider):
        self.session = init_db(self.db_url)
        logging.info(f"Connected to database for legal documents spider {spider.name}")
        
        # Update spider running_status to 'running' when spider starts
        try:
            self.session.execute(
                text("""
                    UPDATE spider_status 
                    SET running_status = 'running', last_update = NOW() 
                    WHERE name = :name
                """),
                {"name": spider.name}
            )
            self.session.commit()
            logging.info(f"Updated legal documents spider {spider.name} running_status to 'running'")
        except Exception as e:
            logging.error(f"Error updating legal documents spider {spider.name} running_status to 'running': {str(e)}")

    def process_item(self, item, spider):
        try:
            # Convert item to dict
            item_dict = dict(item)
            
            # Only process items with lawMetadata (legal documents)
            if 'lawMetadata' not in item_dict:
                return item
            
            law_metadata = item_dict['lawMetadata']
            
            legal_doc = LegalDocument(
                id=item_dict['id'],
                text=item_dict['text'],
                original_id=law_metadata.get('originalId'),
                doc_kind=law_metadata.get('docKind'),
                title=law_metadata.get('title'),
                source=law_metadata.get('source', ''),
                url=law_metadata.get('url', ''),
                published_at=law_metadata.get('publishedAt'),
                parsed_at=law_metadata.get('parsedAt'),
                jurisdiction=law_metadata.get('jurisdiction'),
                language=law_metadata.get('language'),
                stage=law_metadata.get('stage'),
                discussion_period=law_metadata.get('discussionPeriod'),
                explanatory_note=law_metadata.get('explanatoryNote'),
                summary_reports=law_metadata.get('summaryReports'),
                comment_stats=law_metadata.get('commentStats'),
                files=law_metadata.get('files')
            )
            
            self.session.add(legal_doc)
            self.session.commit()
            logging.info(f"Saved legal document {legal_doc.id} to database")
            
        except IntegrityError as e:
            self.session.rollback()
            # Extract source and URL for better error logging
            source = law_metadata.get('source', '')
            url = law_metadata.get('url', '')
            logging.warning(f"Duplicate legal document found: {source}:{url}")
        except Exception as e:
            self.session.rollback()
            logging.error(f"Error saving legal document to database: {str(e)}")
        
        return item

    def close_spider(self, spider):
        if self.session:
            try:
                # Always update running_status to 'idle' when spider completes successfully
                # (running_status is operational state, independent of manual control status)
                self.session.execute(
                    text("""
                        UPDATE spider_status 
                        SET running_status = 'idle', last_update = NOW() 
                        WHERE name = :name
                    """),
                    {"name": spider.name}
                )
                self.session.commit()
                logging.info(f"Updated legal documents spider {spider.name} running_status to 'idle' - completed successfully")
            except Exception as e:
                self.session.rollback()
                logging.error(f"Error updating legal documents spider running_status to 'idle': {str(e)}")
                # Try to set running_status to 'error' if we can't set it to 'idle'
                try:
                    self.session.execute(
                        text("""
                            UPDATE spider_status 
                            SET running_status = 'error', last_update = NOW() 
                            WHERE name = :name
                        """),
                        {"name": spider.name}
                    )
                    self.session.commit()
                    logging.info(f"Set legal documents spider {spider.name} running_status to 'error' due to failure")
                except Exception as e2:
                    logging.error(f"Failed to set legal documents spider {spider.name} running_status to 'error': {str(e2)}")
            finally:
                self.session.close()
                logging.info(f"Closed database connection for legal documents spider {spider.name}")
