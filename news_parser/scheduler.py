import os
import time
import configparser
from datetime import datetime
import subprocess
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import threading

# Load environment variables from .env file
load_dotenv()

# Load configuration from config.ini
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
        config['Scrapy'] = {
            'SCRAPY_PROJECT_PATH': 'news_parser',
            'PYTHON_PATH': 'python'
        }
        config['Scheduler'] = {
            'HOUR': '22',
            'MINUTE': '0',
            'LOG_LEVEL': 'INFO'
        }
        config['Spider'] = {
            'MAX_CONCURRENT': '0',
            'TIMEOUT': '3600',
            'RETRY_FAILED': 'True',
            'MAX_RETRIES': '3'
        }
    
    return config

# Load configuration
config = load_config()

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL', config.get('Database', 'DATABASE_URL', fallback='postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres'))
SCRAPY_PROJECT_PATH = os.getenv('SCRAPY_PROJECT_PATH', os.path.abspath(os.path.join(os.path.dirname(__file__), config.get('Scrapy', 'SCRAPY_PROJECT_PATH', fallback='news_parser'))))
PYTHON_PATH = os.getenv('PYTHON_PATH', config.get('Scrapy', 'PYTHON_PATH', fallback='python'))

# Scheduler configuration
SCHEDULE_HOUR = int(config.get('Scheduler', 'HOUR', fallback='22'))
SCHEDULE_MINUTE = int(config.get('Scheduler', 'MINUTE', fallback='0'))
LOG_LEVEL = config.get('Scheduler', 'LOG_LEVEL', fallback='INFO')

# Spider configuration
MAX_CONCURRENT = int(config.get('Spider', 'MAX_CONCURRENT', fallback='0'))
SPIDER_TIMEOUT = int(config.get('Spider', 'TIMEOUT', fallback='3600'))
RETRY_FAILED = config.getboolean('Spider', 'RETRY_FAILED', fallback=True)
MAX_RETRIES = int(config.get('Spider', 'MAX_RETRIES', fallback='3'))

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format='%(asctime)s %(levelname)s %(message)s'
)

logger = logging.getLogger(__name__)

def get_scheduled_spiders():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT name FROM spider_status WHERE status = 'scheduled'"))
        spiders = [row[0] for row in result]
    return spiders

def update_spider_status(name, status, last_update=None):
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(
            text('UPDATE spider_status SET status=:status, last_update=:last_update WHERE name=:name'),
            {'status': status, 'last_update': last_update, 'name': name}
        )
        conn.commit()

def run_spider_with_monitoring(spider_name):
    """Run a spider and monitor its completion"""
    logger.info(f"Starting spider: {spider_name}")
    update_spider_status(spider_name, 'running', datetime.utcnow())
    
    try:
        # Start spider process
        process = subprocess.Popen([
        PYTHON_PATH, '-m', 'scrapy', 'crawl', spider_name
    ], cwd=SCRAPY_PROJECT_PATH)
        
        # Wait for completion with timeout
        try:
            process.wait(timeout=SPIDER_TIMEOUT)
            now = datetime.utcnow()
            
            if process.returncode == 0:
                update_spider_status(spider_name, 'scheduled', now)
                logger.info(f"Spider {spider_name} finished successfully.")
            else:
                update_spider_status(spider_name, 'error', now)
                logger.error(f"Spider {spider_name} failed with return code {process.returncode}.")
                
        except subprocess.TimeoutExpired:
            logger.error(f"Spider {spider_name} timed out after {SPIDER_TIMEOUT} seconds")
            process.terminate()
            update_spider_status(spider_name, 'error', datetime.utcnow())
            
    except Exception as e:
        logger.error(f"Error running spider {spider_name}: {str(e)}")
        update_spider_status(spider_name, 'error', datetime.utcnow())

def run_all_scheduled_spiders():
    """Run all spiders that have 'scheduled' status concurrently"""
    logger.info(f"Starting scheduled spider run at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d}")
    scheduled_spiders = get_scheduled_spiders()
    
    if not scheduled_spiders:
        logger.info("No spiders with 'scheduled' status found")
        return
    
    logger.info(f"Starting {len(scheduled_spiders)} scheduled spiders concurrently: {scheduled_spiders}")
    
    # Create threads for each spider
    spider_threads = []
    for spider in scheduled_spiders:
        thread = threading.Thread(target=run_spider_with_monitoring, args=(spider,))
        thread.daemon = True
        spider_threads.append(thread)
    
    # Start all spiders at the same time
    for thread in spider_threads:
        thread.start()
    
    # Wait for all spiders to complete
    for thread in spider_threads:
        thread.join()
    
    logger.info("All scheduled spiders completed")

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    
    # Schedule all spiders to run at configured time every day
    scheduler.add_job(run_all_scheduled_spiders, 'cron', hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE)
    
    # Get initially scheduled spiders for logging
    scheduled_spiders = get_scheduled_spiders()
    logger.info(f"Scheduler started. Spiders will run at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} daily.")
    logger.info(f"Currently scheduled spiders: {scheduled_spiders}")
    logger.info(f"Configuration: Max concurrent={MAX_CONCURRENT}, Timeout={SPIDER_TIMEOUT}s, Retry failed={RETRY_FAILED}")
    
    scheduler.start() 