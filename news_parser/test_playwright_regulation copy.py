import asyncio
from playwright.async_api import async_playwright
import logging
import os
from datetime import datetime
import re
import uuid

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_date(date_str):
    """Parse Russian date string to timestamp"""
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

async def extract_structured_data(page, npa_id: str = "158210"):
    """Extract structured data from regulation page and modal"""
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ]
        )
        
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        page = await context.new_page()
        
        url = f"https://regulation.gov.ru/Regulation/Npa/PublicView?npaID={npa_id}"
        logger.info(f"Fetching URL: {url}")
        
        try:
            # Try to navigate with longer timeout
            await page.goto(url, wait_until='domcontentloaded', timeout=120000)
            
            # Wait for content with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    await page.wait_for_selector('text=Этапы проекта', timeout=30000)
                    logger.info("Found 'Этапы проекта' content")
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Attempt {attempt + 1} failed, retrying...")
                        await page.wait_for_timeout(5000)
                    else:
                        logger.warning("Could not find 'Этапы проекта', proceeding anyway...")
            
            await page.wait_for_timeout(3000)
            
            # Extract data from main page
            # Extract data from main page
            title = await page.title()
            logger.info(f"Page title: {title}")
            
            # Extract discussion period dates
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
            
            # Extract comment stats
            comment_stats = {"total": 0}
            try:
                comment_text = await page.inner_text('body')
                comment_match = re.search(r'(\d+)\s*комментари', comment_text)
                if comment_match:
                    comment_stats["total"] = int(comment_match.group(1))
                logger.info(f"Comment stats: {comment_stats}")
            except Exception as e:
                logger.warning(f"Error extracting comment stats: {e}")
            
            # Extract files from main page
            files = []
            try:
                file_links = await page.query_selector_all('a[href*="GetFile"]')
                for link in file_links:
                    href = await link.get_attribute('href')
                    link_text = await link.inner_text()
                    if href and 'GetFile?fileid=' in href:
                        file_id = href.split('fileid=')[-1].split('&')[0]
                        files.append({
                            'fileId': file_id,
                            'url': href if href.startswith('http') else f'https://regulation.gov.ru{href}',
                            'mimeType': 'application/pdf' if '.pdf' in href else 'application/octet-stream',
                            'name': link_text.strip()
                        })
                logger.info(f"Found {len(files)} files on main page")
            except Exception as e:
                logger.warning(f"Error extracting files from main page: {e}")
            
            # Extract explanatory note and additional files from modal
            explanatory_note = None
            summary_reports = []
            try:
                # Try to click modal button
                modal_button = await page.query_selector("a:has-text('Информация по этапу')")
                if modal_button:
                    await modal_button.click()
                    await page.wait_for_selector('.k-window-content', timeout=15000)
                    
                    modal_html = await page.inner_html('.k-window-content')
                    modal_text = await page.inner_text('.k-window-content')
                    
                    # Extract files from modal using onclick patterns - more precise pattern
                    file_pattern = r'onclick="pbaAPI\.showDoc\(\'([^\']+)\'\)">\s*([^<\n]+?)\s*</a>'
                    modal_files = re.findall(file_pattern, modal_html)
                    
                    for file_id, file_name in modal_files:
                        # Clean up filename and extract file extension
                        file_name_clean = file_name.strip()  # Remove whitespace
                        logger.info(f"Raw filename: '{file_name}'")
                        logger.info(f"Clean filename: '{file_name_clean}'")
                        file_lower = file_name_clean.lower()
                        if '.' in file_lower:
                            file_type = file_lower.split('.')[-1]  # Get the extension
                        else:
                            file_type = 'unknown'  # Only if no extension found
                        logger.info(f"File type: {file_type}")
                        
                        file_info = {
                            'fileId': file_id,
                            'url': f'https://regulation.gov.ru/Files/GetFile?fileid={file_id}',
                            'mimeType': file_type
                        }
                        files.append(file_info)
                        
                        # Check if this is an explanatory note
                        if 'пояснительная записка' in file_name.lower():
                            explanatory_note = file_info
                    
                    logger.info(f"Found {len(modal_files)} files in modal")
                    logger.info(f"Total files including main page: {len(files)}")
                    
                    # Close modal
                    close_btn = await page.query_selector('.k-window .closeBtn')
                    if close_btn:
                        await close_btn.click()
                else:
                    logger.warning("Modal button not found")
                    
            except Exception as e:
                logger.warning(f"Error extracting modal data: {e}")
            
            # Determine document kind from title
            doc_kind = "act"  # Default
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
            
            # Assemble structured data
            structured_data = {
                "id": str(uuid.uuid4()),
                "text": title,
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
            
            # Save structured data
            timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
            json_filename = f"regulation_structured_{npa_id}_{timestamp}.json"
            import json
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(structured_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Structured data saved to: {json_filename}")
            
            return structured_data
            
        except Exception as e:
            logger.error(f"Error extracting structured data: {e}")
            return None
        
        finally:
            await context.close()
            await browser.close()

async def main():
    """Main function to test structured data extraction"""
    logger.info("Starting structured data extraction test")
    
    npa_id = "158210"
    data = await extract_structured_data(npa_id)
    
    if data:
        logger.info("Successfully extracted structured data")
        logger.info(f"Document kind: {data['lawMetadata']['docKind']}")
        logger.info(f"Files found: {len(data['lawMetadata']['files']) if data['lawMetadata']['files'] else 0}")
    else:
        logger.error("Failed to extract structured data")

if __name__ == "__main__":
    asyncio.run(main()) 