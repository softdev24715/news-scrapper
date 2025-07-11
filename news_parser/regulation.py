import asyncio
from playwright.async_api import async_playwright
import logging
from datetime import datetime
import re
import uuid
import json
import requests
import xml.etree.ElementTree as ET
import os
import configparser
from dotenv import load_dotenv

# Add the news_parser directory to Python path for imports
import sys
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_date(date_str):
    if not date_str:
        return None
    months = {
        'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
        'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
    }
    match = re.match(r'(\d{1,2})\s+(\w+)\s+(\d{4})', date_str)
    if match:
        day, month_rus, year = match.groups()
        month = months.get(month_rus.lower())
        if month:
            dt = datetime(int(year), month, int(day))
            return int(dt.timestamp())
    return None

def extract_npaid_from_url(url):
    match = re.search(r'npaID=(\d+)', url)
    return match.group(1) if match else None

def get_file_extension(filename):
    """Extract file extension from filename"""
    if not filename:
        return 'unknown'
    filename_lower = filename.lower().strip()
    
    # Common file extensions to look for
    common_extensions = ['.pdf', '.docx', '.doc', '.xlsx', '.xls', '.rtf', '.txt', '.zip', '.rar']
    
    # Look for common file extensions
    for ext in common_extensions:
        if filename_lower.endswith(ext):
            return ext[1:]  # Remove the dot
    
    # Fallback: try to get the last part after dot, but only if it looks like an extension
    if '.' in filename_lower:
        parts = filename_lower.split('.')
        last_part = parts[-1]
        # Only accept if it's 2-4 characters and looks like an extension
        if len(last_part) <= 4 and last_part.isalpha():
            return last_part
    
    return 'unknown'

async def extract_structured_data(page, npa_id: str):
    url = f"https://regulation.gov.ru/Regulation/Npa/PublicView?npaID={npa_id}"
    logger.info(f"Fetching URL: {url}")

    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=120000)
        try:
            await page.wait_for_selector('text=Этапы проекта', timeout=30000)
            logger.info("Found 'Этапы проекта' content")
        except Exception:
            logger.warning("Could not find 'Этапы проекта', proceeding anyway...")

        await page.wait_for_timeout(1000)
        
        # Get the actual regulation title from the page content
        try:
            title_element = await page.query_selector('.public_view_npa_title_text')
            if title_element:
                title = await title_element.inner_text()
                title = title.strip()
                logger.info(f"Regulation title: {title}")
            else:
                # Fallback to page title if element not found
                title = await page.title()
                logger.warning(f"Title element not found, using page title: {title}")
        except Exception as e:
            logger.warning(f"Error extracting title: {e}")
            title = await page.title()
            logger.info(f"Using page title as fallback: {title}")

        discussion_start = None
        discussion_end = None
        try:
            discussion_text = await page.inner_text('body')
            start_match = re.search(r'Дата начала общественного обсуждения[:\s]*(\d{1,2}\s+\w+\s+\d{4})', discussion_text)
            end_match = re.search(r'Дата окончания общественного обсуждения[:\s]*(\d{1,2}\s+\w+\s+\d{4})', discussion_text)
            if start_match:
                discussion_start = parse_date(start_match.group(1))
            if end_match:
                discussion_end = parse_date(end_match.group(1))
            logger.info(f"Discussion period: {start_match.group(1) if start_match else 'Not found'} - {end_match.group(1) if end_match else 'Not found'}")
        except Exception as e:
            logger.warning(f"Error extracting discussion period: {e}")

        comment_stats = {"total": 0}
        try:
            comment_text = await page.inner_text('body')
            comment_match = re.search(r'(\d+)\s*комментари', comment_text)
            if comment_match:
                comment_stats["total"] = int(comment_match.group(1))
            logger.info(f"Comment stats: {comment_stats}")
        except Exception as e:
            logger.warning(f"Error extracting comment stats: {e}")

        files = []
        try:
            file_links = await page.query_selector_all('a[href*="GetFile"]')
            for link in file_links:
                href = await link.get_attribute('href')
                link_text = await link.inner_text()
                if href and 'GetFile?fileid=' in href:
                    file_id = href.split('fileid=')[-1].split('&')[0]
                    file_extension = get_file_extension(link_text)
                    files.append({
                        'fileId': file_id,
                        'url': href if href.startswith('http') else f'https://regulation.gov.ru{href}',
                        'mimeType': file_extension
                    })
            logger.info(f"Found {len(files)} files on main page")
        except Exception as e:
            logger.warning(f"Error extracting files from main page: {e}")

        explanatory_note = None
        summary_reports = []
        try:
            # Try multiple modal selectors for better reliability
            modal_selectors = [
                'a.btn.btn-default:has(img[src="/Areas/Regulation/content/images/info.svg"])',
                'a:has-text("Информация по этапу")',
                'a[onclick*="showModal"]',
                '.btn-info'
            ]
            
            modal_found = False
            for selector in modal_selectors:
                try:
                    modal_triggers = await page.query_selector_all(selector)
                    if modal_triggers:
                        logger.info(f"Found modal triggers with selector: {selector}")
                        modal_found = True
                        break
                except Exception:
                    continue
            
            if not modal_found:
                logger.warning("No modal triggers found with any selector")
                return None
                
            for modal_button in modal_triggers:
                try:
                    await modal_button.click()
                    await page.wait_for_selector('.k-window-content', timeout=15000)
                    modal_html = await page.inner_html('.k-window-content')
                    
                    # More robust file pattern matching
                    file_pattern = r'onclick="pbaAPI\.showDoc\(\'([^\']+)\'\)">\s*([^<\n]+?)\s*</a>'
                    modal_files = re.findall(file_pattern, modal_html)
                    
                    for file_id, file_name in modal_files:
                        file_name_clean = file_name.strip()
                        file_extension = get_file_extension(file_name_clean)
                        
                        file_info = {
                            'fileId': file_id,
                            'url': f'https://regulation.gov.ru/Files/GetFile?fileid={file_id}',
                            'mimeType': file_extension
                        }
                        files.append(file_info)
                        
                        if 'пояснительная записка' in file_name_clean.lower():
                            explanatory_note = file_info
                    
                    logger.info(f"Found {len(modal_files)} files in modal")
                    
                    # Close modal
                    close_btn = await page.query_selector('.k-window .closeBtn')
                    if close_btn:
                        await close_btn.click()
                    await page.wait_for_timeout(500)
                    
                except Exception as e:
                    logger.warning(f"Error processing modal: {e}")
                    continue
                    
        except Exception as e:
            logger.warning(f"Error extracting modal data: {e}")

        doc_kind = "act"
        title_lower = title.lower()
        if "приказ" in title_lower:
            doc_kind = "order"
        elif "постановление" in title_lower:
            doc_kind = "resolution"
        elif "закон" in title_lower:
            doc_kind = "law"
        elif "распоряжение" in title_lower:
            doc_kind = "directive"
        elif "указ" in title_lower:
            doc_kind = "decree"
        elif "федеральный закон" in title_lower:
            doc_kind = "federal_law"
        elif "ведомственный акт" in title_lower:
            doc_kind = "departmental_act"

        structured_data = {
            "id": str(uuid.uuid4()),
            "text": "",
            "lawMetadata": {
                "originalId": npa_id,
                "docKind": doc_kind,
                "title": title,
                "source": "regulation.gov.ru",
                "url": url,
                "publishedAt": discussion_start or int(datetime.now().timestamp()),
                "parsedAt": int(datetime.now().timestamp()),
                "jurisdiction": "RU",
                "language": "ru",
                "stage": "public_discussion",
                "discussionPeriod": {
                    "start": discussion_start,
                    "end": discussion_end
                } if discussion_start and discussion_end else None,
                "explanatoryNote": explanatory_note,
                "summaryReports": summary_reports,
                "commentStats": comment_stats,
                "files": files if files else None
            }
        }
        
        # Save to database
        try:
            # Check if document already exists
            existing_doc = db.query(LegalDocument).filter(LegalDocument.url == url).first()
            if existing_doc:
                logger.info(f"Document already exists in database: {npa_id}")
                return structured_data
            
            # Create new legal document
            legal_doc = LegalDocument(
                id=structured_data["id"],
                text=structured_data["text"],
                original_id=structured_data["lawMetadata"]["originalId"],
                doc_kind=structured_data["lawMetadata"]["docKind"],
                title=structured_data["lawMetadata"]["title"],
                source=structured_data["lawMetadata"]["source"],
                url=structured_data["lawMetadata"]["url"],
                published_at=structured_data["lawMetadata"]["publishedAt"],
                parsed_at=structured_data["lawMetadata"]["parsedAt"],
                jurisdiction=structured_data["lawMetadata"]["jurisdiction"],
                language=structured_data["lawMetadata"]["language"],
                stage=structured_data["lawMetadata"]["stage"],
                discussion_period=structured_data["lawMetadata"]["discussionPeriod"],
                explanatory_note=structured_data["lawMetadata"]["explanatoryNote"],
                summary_reports=structured_data["lawMetadata"]["summaryReports"],
                comment_stats=structured_data["lawMetadata"]["commentStats"],
                files=structured_data["lawMetadata"]["files"]
            )
            
            db.add(legal_doc)
            db.commit()
            logger.info(f"Saved document to database: {npa_id}")
            
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
            db.rollback()
        
        return structured_data

    except Exception as e:
        logger.error(f"Error extracting structured data for {npa_id}: {e}")
        return None

async def process_urls(urls, concurrency=3):
    results = []
    sem = asyncio.Semaphore(concurrency)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            proxy={
                'server': 'http://90.156.202.84:3128',
                'username': 'googlecompute',
                'password': 'xd23rXPEmq2+23'
            }
        )

        async def process_one(url):
            npa_id = extract_npaid_from_url(url)
            if not npa_id:
                logger.warning(f"Could not extract npaID from URL: {url}")
                return
            async with sem:
                page = await context.new_page()
                try:
                    logger.info(f"Processing npaID: {npa_id}")
                    data = await extract_structured_data(page, npa_id)
                    if data:
                        results.append(data)
                        logger.info(f"Success: {npa_id}")
                    else:
                        logger.warning(f"No data extracted for {npa_id}")
                except Exception as e:
                    logger.error(f"Error processing {npa_id}: {e}")
                finally:
                    await page.close()

        tasks = [process_one(url) for url in urls]
        await asyncio.gather(*tasks)
        await context.close()
        await browser.close()
    return results

def get_regulation_urls_from_rss():
    """Get all regulation URLs from RSS feed using proxy"""
    rss_url = 'https://regulation.gov.ru/rss'
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    # Proxy configuration
    proxies = {
        'http': 'http://googlecompute:xd23rXPEmq2+23@90.156.202.84:3128',
        'https': 'http://googlecompute:xd23rXPEmq2+23@90.156.202.84:3128'
    }
    
    try:
        response = requests.get(rss_url, headers=headers, proxies=proxies, timeout=30)
        response.raise_for_status()
        root = ET.fromstring(response.text)
        
        urls = []
        for item in root.findall('.//item'):
            guid = item.findtext('guid')
            if guid:
                url = f"https://regulation.gov.ru/Regulation/Npa/PublicView?npaID={guid}"
                urls.append(url)
        
        logger.info(f"Parsed {len(urls)} URLs from RSS feed")
        return urls
        
    except Exception as e:
        logger.error(f"Error parsing RSS: {e}")
        return []

def load_urls_from_file(filename):
    urls = []
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and 'npaID=' in line:
                urls.append(line)
    return urls

async def main():
    # Get URLs from RSS feed instead of file
    logger.info("Fetching regulation URLs from RSS feed...")
    urls = get_regulation_urls_from_rss()
    
    if not urls:
        logger.error("No URLs found from RSS feed. Exiting.")
        return
    
    logger.info(f"Processing {len(urls)} URLs from RSS feed")
    results = await process_urls(urls, concurrency=3)
    
    # Save results to JSON file as backup
    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    json_filename = f"regulation_structured_batch_{timestamp}.json"
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.info(f"Batch structured data saved to: {json_filename}")
    
    # Close database connection
    db.close()
    logger.info("Database connection closed")

if __name__ == "__main__":
    asyncio.run(main())
