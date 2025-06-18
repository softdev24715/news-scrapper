import os
import time
from datetime import datetime
import subprocess
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
import logging

# Load environment variables from .env file
load_dotenv()

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres')
SCRAPY_PROJECT_PATH = os.getenv('SCRAPY_PROJECT_PATH', os.path.abspath(os.path.join(os.path.dirname(__file__), 'news_parser')))
PYTHON_PATH = os.getenv('PYTHON_PATH', 'python')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

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

def run_spider(spider_name):
    logging.info(f"Running spider: {spider_name}")
    update_spider_status(spider_name, 'running', datetime.utcnow())
    result = subprocess.run([
        PYTHON_PATH, '-m', 'scrapy', 'crawl', spider_name
    ], cwd=SCRAPY_PROJECT_PATH)
    now = datetime.utcnow()
    if result.returncode == 0:
        update_spider_status(spider_name, 'scheduled', now)
        logging.info(f"Spider {spider_name} finished successfully.")
    else:
        update_spider_status(spider_name, 'error', now)
        logging.error(f"Spider {spider_name} failed with return code {result.returncode}.")

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduled_spiders = get_scheduled_spiders()
    for spider in scheduled_spiders:
        scheduler.add_job(run_spider, 'interval', args=[spider], hours=24, next_run_time=None)
    logging.info(f"Scheduler started. Scheduled spiders: {scheduled_spiders}")
    scheduler.start() 