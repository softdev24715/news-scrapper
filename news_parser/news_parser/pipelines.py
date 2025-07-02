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
from .models import Article, LegalDocument, init_db
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
        
        # Update spider status to 'running' when spider starts
        try:
            self.session.execute(
                text("""
                    UPDATE spider_status 
                    SET status = 'running', last_update = NOW() 
                    WHERE name = :name
                """),
                {"name": spider.name}
            )
            self.session.commit()
            logging.info(f"Updated spider {spider.name} status to 'running'")
        except Exception as e:
            logging.error(f"Error updating spider {spider.name} status to 'running': {str(e)}")

    def process_item(self, item, spider):
        self.items_processed += 1
        
        try:
            # Convert item to dict
            item_dict = dict(item)
            
            # Check if this is a legal document or news article
            if 'lawMetadata' in item_dict:
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
            
            # Update spider status to show it's still running (less frequently)
            if self.items_saved % 5 == 0:
                try:
                    self.session.execute(
                        text("""
                            UPDATE spider_status 
                            SET status = 'running', last_update = NOW() 
                            WHERE name = :name
                        """),
                        {"name": spider.name}
                    )
                    self.session.commit()
                except Exception as e:
                    logging.error(f"Error updating spider {spider.name} status: {str(e)}")
            
        except Exception as e:
            self.items_failed += 1
            logging.error(f"Error processing item: {str(e)}")
            # Don't raise the exception to continue processing other items
        
        return item

    def _save_news_article(self, item_dict):
        """Save a news article to the articles table - matches exact spider structure"""
        try:
            article = Article(
                id=item_dict['id'],
                text=item_dict['text'],
                article_metadata=item_dict['metadata']
            )
            
            self.session.add(article)
            self.session.commit()
            
            # Extract source and URL for logging
            source = item_dict['metadata'].get('source', '')
            url = item_dict['metadata'].get('url', '')
            logging.info(f"✅ Saved news article: {source} - {url[:50]}...")
            return True
            
        except IntegrityError as e:
            self.session.rollback()
            self.duplicates_found += 1
            source = item_dict['metadata'].get('source', '')
            url = item_dict['metadata'].get('url', '')
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

    def _save_legal_document(self, item_dict):
        """Save a legal document to the legal_documents table - matches exact spider structure"""
        try:
            legal_doc = LegalDocument(
                id=item_dict['id'],
                text=item_dict['text'],
                law_metadata=item_dict['lawMetadata']
            )
            
            self.session.add(legal_doc)
            self.session.commit()
            
            # Extract source and URL for logging
            source = item_dict['lawMetadata'].get('source', '')
            url = item_dict['lawMetadata'].get('url', '')
            logging.info(f"✅ Saved legal document: {source} - {url[:50]}...")
            return True
            
        except IntegrityError as e:
            self.session.rollback()
            self.duplicates_found += 1
            source = item_dict['lawMetadata'].get('source', '')
            url = item_dict['lawMetadata'].get('url', '')
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
                
                # Update spider status to "Scheduled" (completed successfully)
                self.session.execute(
                    text("""
                        UPDATE spider_status 
                        SET status = 'scheduled', last_update = NOW() 
                        WHERE name = :name
                    """),
                    {"name": spider.name}
                )
                self.session.commit()
                logging.info(f"✅ Updated spider {spider.name} status to 'Scheduled' - completed successfully")
            except Exception as e:
                self.session.rollback()
                logging.error(f"❌ Error updating spider status to 'Scheduled': {str(e)}")
                # Try to set status to 'error' if we can't set it to 'scheduled'
                try:
                    self.session.execute(
                        text("""
                            UPDATE spider_status 
                            SET status = 'error', last_update = NOW() 
                            WHERE name = :name
                        """),
                        {"name": spider.name}
                    )
                    self.session.commit()
                    logging.info(f"⚠️ Set spider {spider.name} status to 'error' due to failure")
                except Exception as e2:
                    logging.error(f"❌ Failed to set spider {spider.name} status to 'error': {str(e2)}")
            finally:
                self.session.close()
                logging.info(f"🔌 Closed database connection for spider {spider.name}")

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
        
        # Update spider status to 'running' when spider starts
        try:
            self.session.execute(
                text("""
                    UPDATE spider_status 
                    SET status = 'running', last_update = NOW() 
                    WHERE name = :name
                """),
                {"name": spider.name}
            )
            self.session.commit()
            logging.info(f"Updated legal documents spider {spider.name} status to 'running'")
        except Exception as e:
            logging.error(f"Error updating legal documents spider {spider.name} status to 'running': {str(e)}")

    def process_item(self, item, spider):
        try:
            # Convert item to dict
            item_dict = dict(item)
            
            # Only process items with lawMetadata (legal documents)
            if 'lawMetadata' not in item_dict:
                return item
            
            legal_doc = LegalDocument(
                id=item_dict['id'],
                text=item_dict['text'],
                law_metadata=item_dict['lawMetadata']
            )
            
            self.session.add(legal_doc)
            self.session.commit()
            logging.info(f"Saved legal document {legal_doc.id} to database")
            
        except IntegrityError as e:
            self.session.rollback()
            # Extract source and URL for better error logging
            source = item_dict['lawMetadata'].get('source', '')
            url = item_dict['lawMetadata'].get('url', '')
            logging.warning(f"Duplicate legal document found: {source}:{url}")
        except Exception as e:
            self.session.rollback()
            logging.error(f"Error saving legal document to database: {str(e)}")
        
        return item

    def close_spider(self, spider):
        if self.session:
            try:
                # Update spider status to "Scheduled" (completed successfully)
                self.session.execute(
                    text("""
                        UPDATE spider_status 
                        SET status = 'scheduled', last_update = NOW() 
                        WHERE name = :name
                    """),
                    {"name": spider.name}
                )
                self.session.commit()
                logging.info(f"Updated legal documents spider {spider.name} status to 'Scheduled' - completed successfully")
            except Exception as e:
                self.session.rollback()
                logging.error(f"Error updating legal documents spider status to 'scheduled': {str(e)}")
                # Try to set status to 'error' if we can't set it to 'scheduled'
                try:
                    self.session.execute(
                        text("""
                            UPDATE spider_status 
                            SET status = 'error', last_update = NOW() 
                            WHERE name = :name
                        """),
                        {"name": spider.name}
                    )
                    self.session.commit()
                    logging.info(f"Set legal documents spider {spider.name} status to 'error' due to failure")
                except Exception as e2:
                    logging.error(f"Failed to set legal documents spider {spider.name} status to 'error': {str(e2)}")
            finally:
                self.session.close()
                logging.info(f"Closed database connection for legal documents spider {spider.name}")
