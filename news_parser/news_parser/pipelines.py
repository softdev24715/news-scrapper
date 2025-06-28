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
from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine
from sqlalchemy.sql import text


class NewsParserPipeline:
    def __init__(self):
        self.output_dir = 'output'
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        self.items = []
        self.processed_ids = set()  # Track processed article IDs
        self.filename = None  # Store filename for the current run
        logging.info("Initializing NewsParserPipeline")

    def process_item(self, item, spider):
        # Convert item to dict if it isn't already
        item_dict = dict(item)
        
        # Check if we've already processed this article
        article_id = item_dict.get('id')
        if article_id in self.processed_ids:
            logging.warning(f"Duplicate article ID found: {article_id}")
            return item
            
        # Add to processed set and items list
        self.processed_ids.add(article_id)
        self.items.append(item_dict)
        logging.info(f"Processing article: {article_id} from {spider.name} (Total items: {len(self.items)})")
        
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
        logging.info(f"Total processed IDs: {len(self.processed_ids)}")
        
        # Reset everything for next run
        self.items = []
        self.processed_ids = set()
        self.filename = None  # Clear the filename

class PostgreSQLPipeline:
    def __init__(self, db_url):
        self.db_url = db_url
        self.session = None
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

    def process_item(self, item, spider):
        try:
            # Convert item to dict
            item_dict = dict(item)
            
            # Check if this is a legal document or news article
            if 'lawMetadata' in item_dict:
                # This is a legal document
                self._save_legal_document(item_dict)
            else:
                # This is a news article
                self._save_news_article(item_dict)
            
            # Update spider status
            self.session.execute(
                text("""
                    UPDATE spider_status 
                    SET status = 'running', last_update = NOW() 
                    WHERE name = :name
                """),
                {"name": spider.name}
            )
            self.session.commit()
            
        except IntegrityError as e:
            self.session.rollback()
            logging.warning(f"Duplicate item found: {item_dict['id']}")
        except Exception as e:
            self.session.rollback()
            logging.error(f"Error saving item to database: {str(e)}")
        
        return item

    def _save_news_article(self, item_dict):
        """Save a news article to the articles table"""
        try:
            article = Article(
                id=item_dict['id'],
                text=item_dict['text'],
                source=item_dict['source'],
                url=item_dict['url'],
                header=item_dict['header'],
                published_at=item_dict['published_at'],
                published_at_iso=datetime.fromisoformat(item_dict['published_at_iso'].replace('Z', '+00:00')),
                parsed_at=item_dict['parsed_at'],
                author=item_dict.get('author'),
                categories=item_dict.get('categories'),
                images=item_dict.get('images')
            )
            
            self.session.add(article)
            self.session.commit()
            logging.info(f"Saved news article {article.id} to database")
        except Exception as e:
            logging.error(f"Error saving news article: {str(e)}")
            logging.error(f"Item dict: {item_dict}")
            raise

    def _save_legal_document(self, item_dict):
        """Save a legal document to the legal_documents table"""
        law_metadata = item_dict['lawMetadata']
        
        legal_doc = LegalDocument(
            id=item_dict['id'],
            text=item_dict['text'],
            original_id=law_metadata.get('originalId'),
            doc_kind=law_metadata.get('docKind'),
            title=law_metadata.get('title'),
            source=law_metadata.get('source'),
            url=law_metadata.get('url'),
            published_at=law_metadata.get('publishedAt'),
            parsed_at=law_met1adata.get('parsedAt'),
            jurisdiction=law_metadata.get('jurisdiction'),
            language=law_metadata.get('language'),
            stage=law_metadata.get('stage'),
            discussion_period=law_metadata.get('discussionPeriod'),
            explanatory_note=law_metadata.get('explanatoryNote'),
            summary_reports=law_metadata.get('summaryReports'),
            comment_stats=law_metadata.get('commentStats')
        )
        
        self.session.add(legal_doc)
        self.session.commit()
        logging.info(f"Saved legal document {legal_doc.id} to database")

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
                logging.info(f"Updated spider {spider.name} status to 'Scheduled' - completed successfully")
            except Exception as e:
                self.session.rollback()
                logging.error(f"Error updating spider status to 'Scheduled': {str(e)}")
            finally:
                self.session.close()
                logging.info(f"Closed database connection for spider {spider.name}")

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
                source=law_metadata.get('source'),
                url=law_metadata.get('url'),
                published_at=law_metadata.get('publishedAt'),
                parsed_at=law_metadata.get('parsedAt'),
                jurisdiction=law_metadata.get('jurisdiction'),
                language=law_metadata.get('language'),
                stage=law_metadata.get('stage'),
                discussion_period=law_metadata.get('discussionPeriod'),
                explanatory_note=law_metadata.get('explanatoryNote'),
                summary_reports=law_metadata.get('summaryReports'),
                comment_stats=law_metadata.get('commentStats')
            )
            
            self.session.add(legal_doc)
            self.session.commit()
            logging.info(f"Saved legal document {legal_doc.id} to database")
            
        except IntegrityError as e:
            self.session.rollback()
            logging.warning(f"Duplicate legal document found: {item_dict['id']}")
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
            finally:
                self.session.close()
                logging.info(f"Closed database connection for legal documents spider {spider.name}")
