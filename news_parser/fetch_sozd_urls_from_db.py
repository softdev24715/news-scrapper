import os
import sys
import logging
from datetime import datetime
import configparser
from dotenv import load_dotenv

# Add the news_parser directory to Python path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'news_parser'))

from news_parser.models import LegalDocument, init_db

# Load environment variables and configuration
load_dotenv()

def load_config():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    
    if os.path.exists(config_path):
        config.read(config_path)
    else:
        # Default configuration if config.ini doesn't exist
        config['Database'] = {
            'DATABASE_URL': 'postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres'
        }
    
    return config

# Load configuration
config = load_config()
DATABASE_URL = os.getenv('DATABASE_URL', config.get('Database', 'DATABASE_URL', fallback='postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres'))

# Initialize database
db = init_db(DATABASE_URL)

def setup_logging():
    """Setup logging configuration"""
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    
    # File handler
    log_file = os.path.join(log_dir, 'sozd_url_fetcher.log')
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

# Setup logging
logger = setup_logging()

def fetch_sozd_urls_from_db():
    """Fetch all SOZD URLs from the database"""
    try:
        # Query for all SOZD documents
        sozd_documents = db.query(LegalDocument).filter(
            LegalDocument.source == 'sozd.duma.gov.ru'
        ).all()
        
        urls = []
        for doc in sozd_documents:
            if doc.url:
                urls.append(doc.url)
        
        logger.info(f"Found {len(urls)} SOZD URLs in database")
        return urls
        
    except Exception as e:
        logger.error(f"Error fetching SOZD URLs from database: {e}")
        return []

def save_urls_to_file(urls, filename):
    """Save URLs to a file"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            for url in urls:
                f.write(f'"{url}"\n')
        logger.info(f"Saved {len(urls)} URLs to {filename}")
    except Exception as e:
        logger.error(f"Error saving URLs to file: {e}")

def main():
    """Main function to fetch SOZD URLs from database"""
    try:
        logger.info("Starting SOZD URL fetching from database...")
        
        # Fetch URLs from database
        urls = fetch_sozd_urls_from_db()
        
        if not urls:
            logger.warning("No SOZD URLs found in database")
            return
        
        # Save URLs to file
        timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        output_filename = f"sozd_urls_from_db_{timestamp}.txt"
        output_path = os.path.join(os.path.dirname(__file__), '..', 'extra', output_filename)
        
        # Ensure the extra directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        save_urls_to_file(urls, output_path)
        
        logger.info("SOZD URL fetching from database completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)
        raise
    finally:
        # Close database connection
        try:
            db.close()
            logger.info("Database connection closed")
        except:
            pass

if __name__ == "__main__":
    try:
        main()
        logger.info("SOZD URL fetcher finished with exit code 0")
    except Exception as e:
        logger.error(f"SOZD URL fetcher failed with error: {e}", exc_info=True)
        sys.exit(1) 