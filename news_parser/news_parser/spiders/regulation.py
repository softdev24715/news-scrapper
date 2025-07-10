import scrapy
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import re
import uuid
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import time
from bs4 import BeautifulSoup

class RegulationSpider(scrapy.Spider):
    name = 'regulation'
    allowed_domains = ['regulation.gov.ru']
    start_urls = ['https://regulation.gov.ru/rss']

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'LOG_LEVEL': 'DEBUG',
    }

    def __init__(self, *args, **kwargs):
        super(RegulationSpider, self).__init__(*args, **kwargs)
        self.driver = None
        self.use_selenium = kwargs.get('use_selenium', False)
        if self.use_selenium:
            self.setup_driver()

    def setup_driver(self):
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--allow-running-insecure-content")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")
            proxy_url = os.environ.get('PROXY_LIST') or os.environ.get('HTTP_PROXY') or os.environ.get('HTTPS_PROXY')
            os.environ['NO_PROXY'] = 'localhost,127.0.0.1,<local>'
            if proxy_url:
                from urllib.parse import urlparse
                parsed = urlparse(proxy_url)
                proxy_string = f"{parsed.hostname}:{parsed.port}"
                chrome_options.add_argument(f'--proxy-server={proxy_string}')
                if parsed.username and parsed.password:
                    chrome_options.add_argument(f'--proxy-auth={parsed.username}:{parsed.password}')
                chrome_options.add_argument('--proxy-bypass-list=localhost,127.0.0.1,<local>')
                chrome_options.add_argument('--no-proxy=localhost,127.0.0.1')
                self.logger.info(f"Configured Selenium with proxy: {parsed.hostname}:{parsed.port}")
            else:
                self.logger.info("No proxy configured for Selenium")
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.logger.info("Selenium WebDriver initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize WebDriver: {e}")
            self.logger.info("Continuing without Selenium - will only extract RSS data")
            self.driver = None

    def closed(self, reason):
        if self.driver:
            self.driver.quit()
            self.logger.info("Selenium WebDriver closed")

    def parse(self, response):
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        target_dates = [
            today.strftime('%Y-%m-%d'),
            yesterday.strftime('%Y-%m-%d')
        ]
        self.logger.info(f"Looking for regulations from: {target_dates}")
        root = ET.fromstring(response.text)
        processed_count = 0
        for item in root.findall('.//item'):
            guid = item.findtext('guid')
            link = item.findtext('link')
            author = item.findtext('author')
            title = item.findtext('title')
            description = item.findtext('description')
            desc_fields = self.parse_description(description)
            published_at = self.parse_date(desc_fields.get('Дата создания'))
            if published_at:
                dt = datetime.fromtimestamp(published_at)
                regulation_date = dt.strftime('%Y-%m-%d')
                if regulation_date not in target_dates:
                    self.logger.debug(f"Skipping regulation from {regulation_date} - not matching {target_dates}")
                    continue
                else:
                    self.logger.info(f"Processing regulation from {regulation_date}: {title}")
            else:
                self.logger.debug(f"No date found for regulation, assuming today: {title}")
            additional_data = {}
            if self.driver is not None and link:
                additional_data = self.extract_page_data(link)
            law_metadata = {
                'originalId': guid,
                'docKind': desc_fields.get('Вид', ''),
                'title': title,
                'source': 'regulation.gov.ru',
                'url': link,
                'publishedAt': published_at,
                'parsedAt': int(datetime.now().timestamp()),
                'jurisdiction': 'RU',
                'language': 'ru',
                'stage': 'public_discussion',
                'discussionPeriod': {
                    'start': additional_data.get('discussion_start'),
                    'end': additional_data.get('discussion_end')
                },
                'explanatoryNote': {
                    'fileId': additional_data.get('explanatory_file_id'),
                    'url': additional_data.get('explanatory_url'),
                    'mimeType': additional_data.get('explanatory_mime_type')
                },
                'summaryReports': additional_data.get('summary_reports', []),
                'commentStats': {'total': additional_data.get('comment_count', 0)}
            }
            processed_count += 1
            yield {
                'id': str(uuid.uuid4()),
                'text': title,
                'lawMetadata': law_metadata,
                'files': additional_data.get('files', [])
            }
        self.logger.info(f"Processed {processed_count} regulations from RSS feed")

    def extract_page_data(self, url):
        if self.driver is None:
            self.logger.error("WebDriver is not initialized")
            return {}
        try:
            self.logger.info(f"Visiting page: {url}")
            self.driver.get(url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            page_source = self.driver.page_source
            debug_filename = f"debug_page_{url.split('=')[-1]}.html"
            with open(debug_filename, 'w', encoding='utf-8') as f:
                f.write(page_source)
            self.logger.info(f"Saved page source to {debug_filename}")
            additional_data = {}
            # Static extraction from hidden <script type="text/template">
            modal_ids = self.extract_modal_ids(page_source)
            for modal_id in modal_ids:
                modal_html = self.extract_modal_template(page_source, modal_id)
                if modal_html:
                    modal_data = self.parse_modal_html(modal_html)
                    additional_data.update(modal_data)
            # Dynamic extraction as fallback
            try:
                info_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((
                        By.XPATH,
                        "//a[contains(@class, 'btn') and contains(@onclick, \"npaPreview.show\") and contains(text(), \"Информация по этапу\")]"
                    ))
                )
                self.logger.info("Found information button, clicking to open modal")
                info_button.click()
                time.sleep(2)
                modal = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "modal-content"))
                )
                modal_data = self.extract_modal_data(modal)
                additional_data.update(modal_data)
                try:
                    close_button = modal.find_element(By.CLASS_NAME, "close")
                    close_button.click()
                except NoSuchElementException:
                    pass
            except TimeoutException:
                self.logger.warning("Information button not found or modal didn't appear")
            page_data = self.extract_page_content()
            additional_data.update(page_data)
            return additional_data
        except Exception as e:
            self.logger.error(f"Error extracting data from {url}: {e}")
            return {}

    def extract_modal_ids(self, page_html):
        soup = BeautifulSoup(page_html, 'html.parser')
        modal_ids = []
        for btn in soup.find_all('a', onclick=True):
            onclick = btn['onclick']
            match = re.search(r"npaPreview\.show\('([^']+)'", onclick)
            if match:
                modal_ids.append(match.group(1))
        return modal_ids

    def extract_modal_template(self, page_html, modal_id):
        soup = BeautifulSoup(page_html, 'html.parser')
        script_tag = soup.find('script', {'id': modal_id, 'type': 'text/template'})
        if script_tag and script_tag.string:
            return script_tag.string
        return None

    def parse_modal_html(self, modal_html):
        soup = BeautifulSoup(modal_html, 'html.parser')
        data = {}
        discussion_text = soup.get_text()
        discussion_match = re.search(r'Период обсуждения[:\s]*(\d{2}\.\d{2}\.\d{4})\s*-\s*(\d{2}\.\d{2}\.\d{4})', discussion_text)
        if discussion_match:
            data['discussion_start'] = discussion_match.group(1)
            data['discussion_end'] = discussion_match.group(2)
        comment_match = re.search(r'Количество комментариев[:\s]*(\d+)', discussion_text)
        if comment_match:
            data['comment_count'] = int(comment_match.group(1))
        files = []
        file_links = soup.find_all('a', href=True)
        for link in file_links:
            href = link.get('href')
            if href and ('download' in href or '.pdf' in href or '.doc' in href):
                files.append({
                    'url': href,
                    'title': link.get_text().strip(),
                    'type': self.get_file_type(href)
                })
        data['files'] = files
        return data

    def extract_modal_data(self, modal):
        try:
            modal_html = modal.get_attribute('innerHTML')
            self.logger.debug(f"Modal HTML (first 500 chars): {modal_html[:500]}")
            soup = BeautifulSoup(modal_html, 'html.parser')
            data = {}
            discussion_text = soup.get_text()
            discussion_match = re.search(r'Период обсуждения[:\s]*(\d{2}\.\d{2}\.\d{4})\s*-\s*(\d{2}\.\d{2}\.\d{4})', discussion_text)
            if discussion_match:
                data['discussion_start'] = discussion_match.group(1)
                data['discussion_end'] = discussion_match.group(2)
            comment_match = re.search(r'Количество комментариев[:\s]*(\d+)', discussion_text)
            if comment_match:
                data['comment_count'] = int(comment_match.group(1))
            files = []
            file_links = soup.find_all('a', href=True)
            for link in file_links:
                href = link.get('href')
                if href and ('download' in href or '.pdf' in href or '.doc' in href):
                    files.append({
                        'url': href,
                        'title': link.get_text().strip(),
                        'type': self.get_file_type(href)
                    })
            data['files'] = files
            return data
        except Exception as e:
            self.logger.error(f"Error extracting modal data: {e}")
            return {}

    def extract_page_content(self):
        if self.driver is None:
            return {}
        try:
            page_html = self.driver.page_source
            soup = BeautifulSoup(page_html, 'html.parser')
            data = {}
            explanatory_links = soup.find_all('a', href=True)
            for link in explanatory_links:
                href = link.get('href')
                text = link.get_text().strip().lower()
                if 'пояснительная' in text or 'explanatory' in text:
                    data['explanatory_url'] = href
                    data['explanatory_mime_type'] = self.get_mime_type(href)
                    break
            summary_reports = []
            for link in explanatory_links:
                href = link.get('href')
                text = link.get_text().strip().lower()
                if 'итоговый' in text or 'summary' in text:
                    summary_reports.append({
                        'url': href,
                        'title': link.get_text().strip(),
                        'type': self.get_file_type(href)
                    })
            data['summary_reports'] = summary_reports
            return data
        except Exception as e:
            self.logger.error(f"Error extracting page content: {e}")
            return {}

    def get_file_type(self, url):
        if '.pdf' in url:
            return 'pdf'
        elif '.doc' in url or '.docx' in url:
            return 'doc'
        elif '.xls' in url or '.xlsx' in url:
            return 'excel'
        else:
            return 'unknown'

    def get_mime_type(self, url):
        if '.pdf' in url:
            return 'application/pdf'
        elif '.doc' in url:
            return 'application/msword'
        elif '.docx' in url:
            return 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        elif '.xls' in url:
            return 'application/vnd.ms-excel'
        elif '.xlsx' in url:
            return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        else:
            return 'application/octet-stream'

    def parse_description(self, description):
        fields = {}
        if not description:
            return fields
        for line in description.split('\n'):
            match = re.match(r'([^:]+):\s*"?(.+?)"?$', line.strip())
            if match:
                key, value = match.groups()
                fields[key.strip()] = value.strip()
        return fields

    def parse_date(self, date_str):
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
