import os
import time
import configparser
from datetime import datetime, timedelta
import subprocess
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue

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
            'START_HOUR': '6',
            'START_MINUTE': '0',
            'LOG_LEVEL': 'INFO',
            'BATCH_SIZE': '10',
            'CYCLE_INTERVAL_HOURS': '4',
            'CYCLE_COUNT': '0'
        }
        config['Spider'] = {
            'MAX_CONCURRENT': '5',
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
START_HOUR = int(config.get('Scheduler', 'START_HOUR', fallback='6'))
START_MINUTE = int(config.get('Scheduler', 'START_MINUTE', fallback='0'))
LOG_LEVEL = config.get('Scheduler', 'LOG_LEVEL', fallback='INFO')
BATCH_SIZE = int(config.get('Scheduler', 'BATCH_SIZE', fallback='10'))
CYCLE_INTERVAL_HOURS = int(config.get('Scheduler', 'CYCLE_INTERVAL_HOURS', fallback='4'))
CYCLE_COUNT = int(config.get('Scheduler', 'CYCLE_COUNT', fallback='0'))

# Spider configuration
MAX_CONCURRENT = int(config.get('Spider', 'MAX_CONCURRENT', fallback='5'))
SPIDER_TIMEOUT = int(config.get('Spider', 'TIMEOUT', fallback='3600'))
RETRY_FAILED = config.getboolean('Spider', 'RETRY_FAILED', fallback=True)
MAX_RETRIES = int(config.get('Spider', 'MAX_RETRIES', fallback='3'))

# Configure logging
def setup_logging():
    """Setup logging configuration with file and console handlers"""
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, LOG_LEVEL.upper()))
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create formatters
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    
    # File handler - save all logs to spider.log
    log_file = os.path.join(log_dir, 'spider.log')
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(getattr(logging, LOG_LEVEL.upper()))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler - for immediate feedback
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, LOG_LEVEL.upper()))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

# Setup logging
logger = setup_logging()

# Global concurrency control
spider_semaphore = threading.Semaphore(MAX_CONCURRENT)
spider_queue = queue.Queue()

def validate_config():
    """Validate configuration settings"""
    if MAX_CONCURRENT < 1:
        logger.error(f"MAX_CONCURRENT must be at least 1, got {MAX_CONCURRENT}")
        return False
    
    if BATCH_SIZE > MAX_CONCURRENT:
        logger.warning(f"BATCH_SIZE ({BATCH_SIZE}) is larger than MAX_CONCURRENT ({MAX_CONCURRENT}). This may cause delays.")
    
    if CYCLE_INTERVAL_HOURS < 2:
        logger.error(f"CYCLE_INTERVAL_HOURS must be at least 2, got {CYCLE_INTERVAL_HOURS}")
        return False
    
    logger.info(f"Configuration validated: MAX_CONCURRENT={MAX_CONCURRENT}, BATCH_SIZE={BATCH_SIZE}, CYCLE_INTERVAL_HOURS={CYCLE_INTERVAL_HOURS}")
    return True

def get_all_spiders():
    """Get all available spiders from the database"""
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT name FROM spider_status WHERE status != 'disabled'"))
        spiders = [row[0] for row in result]
    return spiders

def get_scheduled_spiders():
    """Get spiders that are ready to run (scheduled status)"""
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT name FROM spider_status WHERE status = 'scheduled'"))
        spiders = [row[0] for row in result]
    return spiders

def update_spider_status(name, status, last_update=None):
    """Update manual control status for a spider"""
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(
            text('UPDATE spider_status SET status=:status, last_update=:last_update WHERE name=:name'),
            {'status': status, 'last_update': last_update, 'name': name}
        )
        conn.commit()

def update_spider_running_status(name, running_status, last_update=None):
    """Update operational status for a spider"""
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(
            text('UPDATE spider_status SET running_status=:running_status, last_update=:last_update WHERE name=:name'),
            {'running_status': running_status, 'last_update': last_update, 'name': name}
        )
        conn.commit()

def reset_all_spiders_to_scheduled():
    """Reset all enabled spiders to scheduled status"""
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(
            text("UPDATE spider_status SET status='scheduled' WHERE status != 'disabled'")
        )
        conn.commit()
    logger.info("Reset all enabled spiders to scheduled status")

def run_spider_with_monitoring(spider_name):
    """Run a spider and monitor its completion"""
    logger.info(f"Starting spider: {spider_name}")
    update_spider_running_status(spider_name, 'running', datetime.utcnow())
    
    try:
        # Special handling for regulation spider (Playwright-based)
        if spider_name == 'regulation':
            logger.info(f"Running Playwright-based regulation scraper")
            process = subprocess.Popen([
                PYTHON_PATH, 'regulation.py'
            ], cwd=SCRAPY_PROJECT_PATH, 
               stdout=subprocess.PIPE,
               stderr=subprocess.PIPE,
               text=True,
               bufsize=1,
               universal_newlines=True,
               env={**os.environ, 'PYTHONUNBUFFERED': '1'})
        else:
            # Start regular Scrapy spider process
            process = subprocess.Popen([
                PYTHON_PATH, '-m', 'scrapy', 'crawl', spider_name
            ], cwd=SCRAPY_PROJECT_PATH)
        
        # Wait for completion with timeout
        try:
            # Longer timeout for regulation spider due to async operations
            timeout = SPIDER_TIMEOUT * 2 if spider_name == 'regulation' else SPIDER_TIMEOUT
            stdout, stderr = process.communicate(timeout=timeout)
            now = datetime.utcnow()
            
            # Log any output for regulation spider
            if spider_name == 'regulation':
                if stdout:
                    for line in stdout.splitlines():
                        if line.strip():
                            logger.info(f"[{spider_name}] {line.strip()}")
                if stderr:
                    logger.error(f"[{spider_name}] Error output: {stderr}")
            
            if process.returncode == 0:
                update_spider_running_status(spider_name, 'idle', now)
                logger.info(f"Spider {spider_name} finished successfully.")
            else:
                update_spider_running_status(spider_name, 'error', now)
                logger.error(f"Spider {spider_name} failed with return code {process.returncode}.")
                
        except subprocess.TimeoutExpired:
            timeout_seconds = SPIDER_TIMEOUT * 2 if spider_name == 'regulation' else SPIDER_TIMEOUT
            logger.error(f"Spider {spider_name} timed out after {timeout_seconds} seconds")
            process.terminate()
            update_spider_running_status(spider_name, 'error', datetime.utcnow())
            
    except Exception as e:
        logger.error(f"Error running spider {spider_name}: {str(e)}")
        update_spider_running_status(spider_name, 'error', datetime.utcnow())

def run_spider_batch(spider_batch):
    """Run a batch of spiders with global concurrency control"""
    logger.info(f"Starting batch of {len(spider_batch)} spiders: {spider_batch}")
    
    # Use ThreadPoolExecutor with global semaphore to limit concurrent execution
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
        # Submit all spiders in the batch
        future_to_spider = {}
        for spider in spider_batch:
            # Acquire semaphore before submitting spider
            spider_semaphore.acquire()
            future = executor.submit(run_spider_with_monitoring, spider)
            future_to_spider[future] = spider
        
        # Wait for all spiders in the batch to complete
        for future in as_completed(future_to_spider):
            spider = future_to_spider[future]
            try:
                future.result()  # This will raise any exception that occurred
            except Exception as e:
                logger.error(f"Spider {spider} generated an exception: {e}")
            finally:
                # Release semaphore when spider completes
                spider_semaphore.release()
    
    logger.info(f"Batch completed: {spider_batch}")

def run_daily_spider_cycle():
    """Run the complete daily cycle of all spiders in batches"""
    logger.info("Starting daily spider cycle")
    
    # Reset all spiders to scheduled status
    reset_all_spiders_to_scheduled()
    
    # Get all available spiders
    all_spiders = get_all_spiders()
    if not all_spiders:
        logger.warning("No enabled spiders found")
        return
    
    logger.info(f"Total spiders to process: {len(all_spiders)}")
    
    # Split spiders into batches
    spider_batches = [all_spiders[i:i + BATCH_SIZE] for i in range(0, len(all_spiders), BATCH_SIZE)]
    
    logger.info(f"Created {len(spider_batches)} batches of up to {BATCH_SIZE} spiders each")
    
    # Run each batch
    for i, batch in enumerate(spider_batches):
        logger.info(f"Processing batch {i+1}/{len(spider_batches)}")
        run_spider_batch(batch)
        
        # Wait between batches (except for the last batch)
        if i < len(spider_batches) - 1:
            wait_time = CYCLE_INTERVAL_HOURS * 3600  # Convert hours to seconds
            logger.info(f"Waiting {CYCLE_INTERVAL_HOURS} hours before next batch...")
            time.sleep(wait_time)
    
    logger.info("Daily spider cycle completed")

def schedule_staggered_groups():
    """Schedule spider groups with staggered start times"""
    logger.info(f"Setting up staggered schedule: start at {START_HOUR:02d}:{START_MINUTE:02d}, interval {CYCLE_INTERVAL_HOURS}h, count {CYCLE_COUNT if CYCLE_COUNT > 0 else 'unlimited'}")
    
    # Reset all spiders to scheduled status
    reset_all_spiders_to_scheduled()
    
    # Define fixed groups of 10 spiders each
    spider_groups = [
        # Group 1: Major news and business sources
        ['tass', 'rbc', 'vedomosti', 'pnp', 'lenta', 'kommersant', 'gazeta', 'graininfo', 'forbes', 'interfax'],
        
        # Group 2: Government, legal and remaining sources
        ['government', 'kremlin', 'regulation', 'rg', 'ria', 'pravo', 'sozd', 'eaeu', 'izvestia', 'meduza', 'cntd']
    ]
    
    logger.info(f"Created 2 fixed groups of 10 spiders each")
    
    # Schedule Group 1 at start time
    logger.info(f"Scheduling Group 1 at {START_HOUR:02d}:{START_MINUTE:02d} every {CYCLE_INTERVAL_HOURS} hours")
    scheduler.add_job(
        run_spider_batch, 
        'cron', 
        hour=START_HOUR,
        minute=START_MINUTE,
        args=[spider_groups[0]],
        id='group_1_staggered'
    )
    
    # Schedule Group 2 at start time + 2 hours
    group2_hour = (START_HOUR + 2) % 24
    logger.info(f"Scheduling Group 2 at {group2_hour:02d}:{START_MINUTE:02d} every {CYCLE_INTERVAL_HOURS} hours")
    scheduler.add_job(
        run_spider_batch, 
        'cron', 
        hour=group2_hour,
        minute=START_MINUTE,
        args=[spider_groups[1]],
        id='group_2_staggered'
    )

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    
    # Validate configuration
    if not validate_config():
        logger.error("Configuration validation failed. Exiting.")
        exit(1)
    
    # Get all spiders for logging
    all_spiders = get_all_spiders()
    logger.info(f"Scheduler started with {len(all_spiders)} enabled spiders")
    logger.info(f"Configuration: Max concurrent={MAX_CONCURRENT}, Timeout={SPIDER_TIMEOUT}s")
    
    # Schedule staggered groups
    schedule_staggered_groups()
    
    # Schedule daily reset at 5:30 AM (before first group)
    scheduler.add_job(reset_all_spiders_to_scheduled, 'cron', hour=5, minute=30, id='daily_reset')
    
    logger.info(f"Spider groups scheduled with staggered timing:")
    logger.info("  Group 1 (News & Business): tass, rbc, vedomosti, pnp, lenta, kommersant, gazeta, graininfo, forbes, interfax")
    logger.info("  Group 2 (Government & Legal): government, kremlin, regulation, rg, ria, pravo, sozd, eaeu, izvestia, meduza")
    logger.info(f"Group 1 starts at {START_HOUR:02d}:{START_MINUTE:02d}, Group 2 starts 2 hours later")
    logger.info(f"Cycle repeats every {CYCLE_INTERVAL_HOURS} hours")
    logger.info(f"Global concurrency limit: {MAX_CONCURRENT} spiders maximum")
    logger.info("Daily reset scheduled at 5:30 AM")
    
    scheduler.start() 