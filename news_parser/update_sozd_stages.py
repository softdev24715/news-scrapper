#!/usr/bin/env python3
"""
SOZD Stage Update Script
Fetches SOZD documents from the last 30 days from database and updates their stages
"""

import asyncio
import aiohttp
import logging
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import configparser

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sozd_stage_update.log'),
        logging.StreamHandler()
    ]
)

class SOZDStageUpdater:
    def __init__(self, days_back=30, max_concurrent=10):
        self.days_back = days_back
        self.max_concurrent = max_concurrent
        
        # Load database configuration from config.ini
        self.db_config = self.load_database_config()
        
        # Calculate date range
        self.end_date = datetime.now().date()
        self.start_date = self.end_date - timedelta(days=self.days_back)
        
        logging.info(f"Initializing SOZD Stage Updater")
        logging.info(f"Date range: {self.start_date} to {self.end_date}")
        logging.info(f"Max concurrent requests: {self.max_concurrent}")

    def load_database_config(self):
        """Load database configuration from config.ini"""
        try:
            config = configparser.ConfigParser()
            config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
            config.read(config_path)
            
            # Parse DATABASE_URL from config.ini
            database_url = config.get('Database', 'DATABASE_URL')
            
            # Parse the PostgreSQL URL
            # Format: postgresql://username:password@host:port/database
            if database_url.startswith('postgresql://'):
                # Remove postgresql:// prefix
                url_part = database_url[13:]
                
                # Split by @ to separate credentials from host
                credentials_part, host_part = url_part.split('@')
                
                # Split credentials
                username, password = credentials_part.split(':')
                
                # Split host part
                host_port, database = host_part.split('/')
                host, port = host_port.split(':')
                
                db_config = {
                    'host': host,
                    'database': database,
                    'user': username,
                    'password': password,
                    'port': port
                }
                
                logging.info(f"Loaded database config: {host}:{port}/{database}")
                return db_config
            else:
                raise ValueError(f"Invalid DATABASE_URL format: {database_url}")
                
        except Exception as e:
            logging.error(f"Error loading database config: {e}")
            # Fallback to environment variables
            return {
                'host': os.getenv('DB_HOST', 'localhost'),
                'database': os.getenv('DB_NAME', 'news_scraper'),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', ''),
                'port': os.getenv('DB_PORT', '5432')
            }

    def get_db_connection(self):
        """Get database connection"""
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            return None

    def fetch_recent_sozd_urls(self):
        """Fetch SOZD URLs from the database that were parsed within the date range"""
        conn = self.get_db_connection()
        if not conn:
            return []
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Convert dates to timestamps for comparison
                start_timestamp = int(datetime.combine(self.start_date, datetime.min.time()).timestamp())
                end_timestamp = int(datetime.combine(self.end_date, datetime.max.time()).timestamp())
                
                query = """
                SELECT id, url, title, parsed_at, stage, doc_kind
                FROM legal_documents 
                WHERE source = 'sozd.duma.gov.ru' 
                AND parsed_at >= %s 
                AND parsed_at <= %s
                ORDER BY parsed_at DESC
                """
                
                cursor.execute(query, (start_timestamp, end_timestamp))
                results = cursor.fetchall()
                
                logging.info(f"Found {len(results)} SOZD documents from {self.start_date} to {self.end_date}")
                return results
                
        except Exception as e:
            logging.error(f"Error fetching recent SOZD URLs: {e}")
            return []
        finally:
            conn.close()

    def extract_stage_from_progress(self, soup):
        """Extract current stage from bill progress visualization (copied from sozd.py)"""
        stage = None
        
        try:
            # Find the bill_progress_wrap div
            progress_wrap = soup.find('div', class_='bill_progress_wrap')
            if not progress_wrap:
                logging.warning("bill_progress_wrap div not found")
                return stage
            
            # Find bill_gorizontal_progress inside it
            horizontal_progress = progress_wrap.find('div', class_='bill_gorizontal_progress')
            if not horizontal_progress:
                logging.warning("bill_gorizontal_progress div not found")
                return stage
            
            # Find bgp_middle div
            bgp_middle = horizontal_progress.find('div', class_='bgp_middle')
            if not bgp_middle:
                logging.warning("bgp_middle div not found")
                return stage
            
            # Look for active stage divs (with 'green' class)
            active_stages = []
            stage_descriptions = []
            
            # Method 1: Look for divs with btm\d+ classes that have 'green' class
            for div in bgp_middle.find_all('div', class_=re.compile(r'btm\d+')):
                classes = div.get('class', [])
                if 'green' in classes:
                    # Extract stage number from class name (e.g., 'btm9' -> '9')
                    stage_match = re.search(r'btm(\d+)', ' '.join(classes))
                    if stage_match:
                        stage_num = stage_match.group(1)
                        active_stages.append(stage_num)
                        
                        # Get the stage description from data-original-title attribute
                        anchor = div.find('a', attrs={'data-original-title': True})
                        if anchor:
                            stage_desc = anchor.get('data-original-title')
                            if stage_desc:
                                stage_descriptions.append((stage_num, stage_desc))
                                logging.info(f"Found stage {stage_num}: {stage_desc}")
            
            # Method 2: If no green stages found, look for any stages with data-original-title
            if not active_stages:
                logging.info("No green stages found, looking for any stages with descriptions")
                for div in bgp_middle.find_all('div', class_=re.compile(r'btm\d+')):
                    stage_match = re.search(r'btm(\d+)', ' '.join(div.get('class', [])))
                    if stage_match:
                        stage_num = stage_match.group(1)
                        anchor = div.find('a', attrs={'data-original-title': True})
                        if anchor:
                            stage_desc = anchor.get('data-original-title')
                            if stage_desc:
                                active_stages.append(stage_num)
                                stage_descriptions.append((stage_num, stage_desc))
                                logging.info(f"Found stage {stage_num}: {stage_desc}")
            
            # Method 3: Look for any anchor with data-original-title in the progress area
            if not active_stages:
                logging.info("No stages found in divs, looking for any anchors with descriptions")
                for anchor in bgp_middle.find_all('a', attrs={'data-original-title': True}):
                    stage_desc = anchor.get('data-original-title')
                    if stage_desc:
                        # Try to find the parent div with btm class
                        parent_div = anchor.find_parent('div', class_=re.compile(r'btm\d+'))
                        if parent_div:
                            stage_match = re.search(r'btm(\d+)', ' '.join(parent_div.get('class', [])))
                            if stage_match:
                                stage_num = stage_match.group(1)
                                active_stages.append(stage_num)
                                stage_descriptions.append((stage_num, stage_desc))
                                logging.info(f"Found stage {stage_num}: {stage_desc}")
            
            # Get the highest stage number (most recent/current stage)
            if active_stages:
                current_stage_num = max(active_stages, key=int)
                
                # Try to get the description for the current stage
                current_stage_desc = None
                for stage_num, desc in stage_descriptions:
                    if stage_num == current_stage_num:
                        current_stage_desc = desc
                        break
                
                if current_stage_desc:
                    stage = current_stage_desc
                    logging.info(f"Extracted stage: {stage} (stage number: {current_stage_num})")
                else:
                    # Fallback to mapping if no description found
                    stage_mapping = {
                        '1': 'Внесение законопроекта в Государственную Думу',
                        '2': 'Предварительное рассмотрение законопроекта, внесенного в Государственную Думу',
                        '3': 'Рассмотрение законопроекта в первом чтении',
                        '4': 'Рассмотрение законопроекта во втором чтении',
                        '5': 'Рассмотрение законопроекта в третьем чтении',
                        '6': 'Прохождение закона в Совете Федерации',
                        '8': 'Прохождение закона у Президента Российской Федерации',
                        '9': 'Повторное рассмотрение закона, отклоненного Президентом Российской Федерации',
                        '11': 'Опубликование закона'
                    }
                    stage = stage_mapping.get(current_stage_num, f'Этап {current_stage_num}')
                    logging.info(f"Extracted stage (fallback): {stage} (stage number: {current_stage_num})")
            else:
                logging.warning("No active stages found in progress visualization")
                
        except Exception as e:
            logging.warning(f"Error extracting stage from progress: {e}")
        
        return stage

    def extract_stage_fallback(self, soup):
        """Fallback method to extract stage from other parts of the page"""
        stage = None
        
        try:
            # Method 1: Look for stage information in bill_data_wrap
            bill_data_wrap = soup.find('div', class_='bill_data_wrap')
            if bill_data_wrap:
                # Look for status or stage information
                status_elem = bill_data_wrap.find('span', id='current_oz_status')
                if status_elem:
                    status_text = status_elem.get_text(strip=True)
                    if status_text:
                        stage = status_text
                        logging.info(f"Found stage from status element: {stage}")
                        return stage
                
                # Look for any text that might indicate stage
                bill_text = bill_data_wrap.get_text()
                stage_patterns = [
                    r'Статус[:\s]*([^,\n]+)',
                    r'Этап[:\s]*([^,\n]+)',
                    r'Состояние[:\s]*([^,\n]+)'
                ]
                
                for pattern in stage_patterns:
                    stage_match = re.search(pattern, bill_text)
                    if stage_match:
                        stage = stage_match.group(1).strip()
                        logging.info(f"Found stage from pattern '{pattern}': {stage}")
                        return stage
            
            # Method 2: Look for stage information in the entire page
            page_text = soup.get_text()
            stage_patterns = [
                r'Статус[:\s]*([^,\n]+)',
                r'Этап[:\s]*([^,\n]+)',
                r'Состояние[:\s]*([^,\n]+)',
                r'Текущий этап[:\s]*([^,\n]+)',
                r'Стадия[:\s]*([^,\n]+)'
            ]
            
            for pattern in stage_patterns:
                stage_match = re.search(pattern, page_text)
                if stage_match:
                    stage = stage_match.group(1).strip()
                    logging.info(f"Found stage from page text pattern '{pattern}': {stage}")
                    return stage
            
            # Method 3: Look for specific stage keywords
            stage_keywords = {
                'внесен': 'Внесение законопроекта в Государственную Думу',
                'первое чтение': 'Рассмотрение законопроекта в первом чтении',
                'второе чтение': 'Рассмотрение законопроекта во втором чтении',
                'третье чтение': 'Рассмотрение законопроекта в третьем чтении',
                'совет федерации': 'Прохождение закона в Совете Федерации',
                'президент': 'Прохождение закона у Президента Российской Федерации',
                'опубликован': 'Опубликование закона',
                'принят': 'Принятие закона'
            }
            
            page_text_lower = page_text.lower()
            for keyword, stage_desc in stage_keywords.items():
                if keyword in page_text_lower:
                    stage = stage_desc
                    logging.info(f"Found stage from keyword '{keyword}': {stage}")
                    return stage
                    
        except Exception as e:
            logging.warning(f"Error in stage fallback extraction: {e}")
        
        return stage

    async def fetch_and_update_stage(self, session, doc_record):
        """Fetch a single document and update its stage in the database"""
        url = doc_record['url']
        doc_id = doc_record['id']
        old_stage = doc_record['stage']
        doc_kind = doc_record.get('doc_kind', 'bill')
        
        try:
            logging.info(f"Fetching document: {url}")
            
            async with session.get(url, timeout=30) as response:
                if response.status == 200:
                    html_content = await response.text()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    # Extract stage based on document kind
                    if doc_kind == 'bill':
                        new_stage = self.extract_stage_from_progress(soup)
                        if not new_stage:
                            # Fallback for bills: try to extract stage from other sources
                            new_stage = self.extract_stage_fallback(soup)
                            if new_stage:
                                logging.info(f"Extracted stage using fallback method: {new_stage}")
                            else:
                                # Default stage for bills if no stage found
                                new_stage = "Внесение законопроекта в Государственную Думу"
                                logging.info(f"No stage found for bill, using default: {new_stage}")
                    else:
                        # For non-bill documents, set default stage
                        new_stage = "Внесение в Государственную Думу"
                        logging.info(f"Setting default stage for {doc_kind}: {new_stage}")
                    
                    # Update database if stage changed
                    if new_stage and new_stage != old_stage:
                        success = self.update_stage_in_db(doc_id, new_stage)
                        if success:
                            logging.info(f"✅ Updated stage for {doc_id}: '{old_stage}' -> '{new_stage}'")
                            return {'status': 'updated', 'doc_id': doc_id, 'old_stage': old_stage, 'new_stage': new_stage}
                        else:
                            logging.error(f"❌ Failed to update stage in database for {doc_id}")
                            return {'status': 'db_error', 'doc_id': doc_id}
                    else:
                        logging.info(f"⏭️  No stage change for {doc_id}: '{old_stage}'")
                        return {'status': 'no_change', 'doc_id': doc_id}
                else:
                    logging.error(f"❌ HTTP {response.status} for {url}")
                    return {'status': 'http_error', 'doc_id': doc_id, 'status_code': response.status}
                    
        except asyncio.TimeoutError:
            logging.error(f"⏰ Timeout fetching {url}")
            return {'status': 'timeout', 'doc_id': doc_id}
        except Exception as e:
            logging.error(f"❌ Error fetching {url}: {e}")
            return {'status': 'error', 'doc_id': doc_id, 'error': str(e)}

    def update_stage_in_db(self, doc_id, new_stage):
        """Update stage in the database"""
        conn = self.get_db_connection()
        if not conn:
            return False
        
        try:
            with conn.cursor() as cursor:
                query = """
                UPDATE legal_documents 
                SET stage = %s, updated_at = NOW()
                WHERE id = %s
                """
                cursor.execute(query, (new_stage, doc_id))
                conn.commit()
                return True
                
        except Exception as e:
            logging.error(f"Error updating stage in database: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    async def process_documents(self, documents):
        """Process documents with concurrency control"""
        logging.info(f"Processing {len(documents)} documents with max {self.max_concurrent} concurrent requests")
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def fetch_with_semaphore(session, doc_record):
            async with semaphore:
                return await self.fetch_and_update_stage(session, doc_record)
        
        # Create aiohttp session
        timeout = aiohttp.ClientTimeout(total=60)
        connector = aiohttp.TCPConnector(limit=self.max_concurrent, limit_per_host=self.max_concurrent)
        
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            tasks = [fetch_with_semaphore(session, doc_record) for doc_record in documents]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            return results

    def print_summary(self, results):
        """Print summary of the update process"""
        total = len(results)
        updated = sum(1 for r in results if isinstance(r, dict) and r.get('status') == 'updated')
        no_change = sum(1 for r in results if isinstance(r, dict) and r.get('status') == 'no_change')
        errors = sum(1 for r in results if isinstance(r, dict) and r.get('status') in ['error', 'http_error', 'timeout', 'db_error'])
        exceptions = sum(1 for r in results if isinstance(r, Exception))
        
        logging.info("=" * 60)
        logging.info("📋 STAGE UPDATE SUMMARY")
        logging.info("=" * 60)
        logging.info(f"📊 Total documents processed: {total}")
        logging.info(f"✅ Stages updated: {updated}")
        logging.info(f"⏭️  No changes: {no_change}")
        logging.info(f"❌ Errors: {errors + exceptions}")
        logging.info(f"📈 Update rate: {(updated/total*100):.1f}%" if total > 0 else "N/A")
        logging.info("=" * 60)

    async def run(self):
        """Main execution method"""
        logging.info("🚀 Starting SOZD stage update process")
        
        # Fetch recent documents from database
        documents = self.fetch_recent_sozd_urls()
        if not documents:
            logging.warning("No recent SOZD documents found in database")
            return
        
        # Process documents
        results = await self.process_documents(documents)
        
        # Print summary
        self.print_summary(results)

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Update SOZD document stages from the last 30 days')
    parser.add_argument('--days-back', type=int, default=30, 
                       help='Number of days back to check (default: 30)')
    parser.add_argument('--max-concurrent', type=int, default=10,
                       help='Maximum concurrent requests (default: 10)')
    
    args = parser.parse_args()
    
    # Create and run updater
    updater = SOZDStageUpdater(
        days_back=args.days_back,
        max_concurrent=args.max_concurrent
    )
    
    # Run the update process
    asyncio.run(updater.run())

if __name__ == '__main__':
    main() 