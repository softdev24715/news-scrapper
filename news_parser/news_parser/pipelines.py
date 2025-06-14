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
from .models import Article, init_db
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
            
            # Create Article instance
            article = Article(
                id=item_dict['id'],
                text=item_dict['text'],
                source=item_dict['metadata']['source'],
                url=item_dict['metadata']['url'],
                header=item_dict['metadata']['header'],
                published_at=item_dict['metadata']['published_at'],
                published_at_iso=datetime.fromisoformat(item_dict['metadata']['published_at_iso'].replace('Z', '+00:00')),
                parsed_at=item_dict['metadata']['parsed_at'],
                author=item_dict['metadata'].get('author'),
                categories=item_dict['metadata'].get('categories'),
                images=item_dict['metadata'].get('images')
            )
            
            # Add to session
            self.session.add(article)
            self.session.commit()
            logging.info(f"Saved article {article.id} to database")

            # Update spider status
            self.session.execute(
                text("""
                    UPDATE spider_status 
                    SET status = 'ok', last_update = NOW() 
                    WHERE name = :name
                """),
                {"name": spider.name}
            )
            self.session.commit()
            
        except IntegrityError as e:
            self.session.rollback()
            logging.warning(f"Duplicate article found: {item_dict['id']}")
        except Exception as e:
            self.session.rollback()
            logging.error(f"Error saving article to database: {str(e)}")
        
        return item

    def close_spider(self, spider):
        if self.session:
            self.session.close()
            logging.info(f"Closed database connection for spider {spider.name}")
