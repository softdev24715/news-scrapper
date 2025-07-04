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
            'LOG_LEVEL': 'INFO',
            'BATCH_SIZE': '5',
            'BATCH_INTERVAL_HOURS': '4'
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
SCHEDULE_HOUR = int(config.get('Scheduler', 'HOUR', fallback='22'))
SCHEDULE_MINUTE = int(config.get('Scheduler', 'MINUTE', fallback='0'))
LOG_LEVEL = config.get('Scheduler', 'LOG_LEVEL', fallback='INFO')
BATCH_SIZE = int(config.get('Scheduler', 'BATCH_SIZE', fallback='5'))
BATCH_INTERVAL_HOURS = int(config.get('Scheduler', 'BATCH_INTERVAL_HOURS', fallback='4'))

# Spider configuration
MAX_CONCURRENT = int(config.get('Spider', 'MAX_CONCURRENT', fallback='5'))
SPIDER_TIMEOUT = int(config.get('Spider', 'TIMEOUT', fallback='3600'))
RETRY_FAILED = config.getboolean('Spider', 'RETRY_FAILED', fallback=True)
MAX_RETRIES = int(config.get('Spider', 'MAX_RETRIES', fallback='3'))

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format='%(asctime)s %(levelname)s %(message)s'
)

logger = logging.getLogger(__name__)

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
        # Start spider process with date range parameters
        today = datetime.now().strftime('%Y-%m-%d')
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        process = subprocess.Popen([
            PYTHON_PATH, '-m', 'scrapy', 'crawl', spider_name,
            '-a', f'start_date={yesterday}',
            '-a', f'end_date={today}'
        ], cwd=SCRAPY_PROJECT_PATH)
        
        # Wait for completion with timeout
        try:
            process.wait(timeout=SPIDER_TIMEOUT)
            now = datetime.utcnow()
            
            if process.returncode == 0:
                update_spider_running_status(spider_name, 'idle', now)
                logger.info(f"Spider {spider_name} finished successfully.")
            else:
                update_spider_running_status(spider_name, 'error', now)
                logger.error(f"Spider {spider_name} failed with return code {process.returncode}.")
                
        except subprocess.TimeoutExpired:
            logger.error(f"Spider {spider_name} timed out after {SPIDER_TIMEOUT} seconds")
            process.terminate()
            update_spider_running_status(spider_name, 'error', datetime.utcnow())
            
    except Exception as e:
        logger.error(f"Error running spider {spider_name}: {str(e)}")
        update_spider_running_status(spider_name, 'error', datetime.utcnow())

def run_spider_batch(spider_batch):
    """Run a batch of spiders concurrently"""
    logger.info(f"Starting batch of {len(spider_batch)} spiders: {spider_batch}")
    
    # Use ThreadPoolExecutor to limit concurrent execution
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
        # Submit all spiders in the batch
        future_to_spider = {executor.submit(run_spider_with_monitoring, spider): spider for spider in spider_batch}
        
        # Wait for all spiders in the batch to complete
        for future in as_completed(future_to_spider):
            spider = future_to_spider[future]
            try:
                future.result()  # This will raise any exception that occurred
            except Exception as e:
                logger.error(f"Spider {spider} generated an exception: {e}")
    
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
            wait_time = BATCH_INTERVAL_HOURS * 3600  # Convert hours to seconds
            logger.info(f"Waiting {BATCH_INTERVAL_HOURS} hours before next batch...")
            time.sleep(wait_time)
    
    logger.info("Daily spider cycle completed")

def schedule_daily_batches():
    """Schedule spider batches throughout the day"""
    logger.info("Setting up daily batch schedule")
    
    # Reset all spiders to scheduled status
    reset_all_spiders_to_scheduled()
    
    # Define fixed groups of 5 spiders each
    spider_groups = [
        # Group 1: Major news sources (6:00 AM)
        ['tass', 'rbc', 'vedomosti', 'pnp', 'lenta'],
        
        # Group 2: Business and economics (12:00 PM)
        ['kommersant', 'gazeta', 'graininfo', 'forbes', 'interfax'],
        
        # Group 3: Government and official sources (6:00 PM)
        ['government', 'kremlin', 'regulation', 'rg', 'ria'],
        
        # Group 4: Legal documents and remaining sources (12:00 AM)
        ['pravo', 'sozd', 'eaeu', 'izvestia', 'meduza']
    ]
    
    # Define schedule times for each group (6-hour intervals)
    schedule_times = [
        (6, 0),   # 6:00 AM - Group 1
        (12, 0),  # 12:00 PM - Group 2  
        (18, 0),  # 6:00 PM - Group 3
        (0, 0)    # 12:00 AM - Group 4
    ]
    
    logger.info(f"Created 4 fixed groups of 5 spiders each")
    
    # Schedule each group
    for i, (group, (hour, minute)) in enumerate(zip(spider_groups, schedule_times)):
        logger.info(f"Scheduling Group {i+1} ({group}) at {hour:02d}:{minute:02d}")
        
        # Schedule the group
        scheduler.add_job(
            run_spider_batch, 
            'cron', 
            hour=hour, 
            minute=minute,
            args=[group],
            id=f'group_{i+1}'
        )

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    
    # Get all spiders for logging
    all_spiders = get_all_spiders()
    logger.info(f"Scheduler started with {len(all_spiders)} enabled spiders")
    logger.info(f"Configuration: Max concurrent={MAX_CONCURRENT}, Timeout={SPIDER_TIMEOUT}s")
    
    # Schedule daily batches
    schedule_daily_batches()
    
    # Schedule daily reset at 5:30 AM (before first group)
    scheduler.add_job(reset_all_spiders_to_scheduled, 'cron', hour=5, minute=30, id='daily_reset')
    
    logger.info("Daily spider groups scheduled:")
    logger.info("  Group 1 (Major news): 6:00 AM - tass, rbc, vedomosti, pnp, lenta")
    logger.info("  Group 2 (Business): 12:00 PM - kommersant, gazeta, graininfo, forbes, interfax")
    logger.info("  Group 3 (Government): 6:00 PM - government, kremlin, regulation, rg, ria")
    logger.info("  Group 4 (Legal): 12:00 AM - pravo, sozd, eaeu, izvestia, meduza")
    logger.info("Daily reset scheduled at 5:30 AM")
    
    scheduler.start() 