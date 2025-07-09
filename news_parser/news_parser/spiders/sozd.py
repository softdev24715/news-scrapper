import scrapy
from datetime import datetime, timedelta
import uuid
from bs4 import BeautifulSoup
import logging
import re

class SozdSpider(scrapy.Spider):
    name = 'sozd'
    allowed_domains = ['sozd.duma.gov.ru']
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 1,
    }

    def start_requests(self):
        """Start with all three document types to find recent documents"""
        # Get today's and yesterday's dates for filtering
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        self.target_dates = [
            today.strftime('%Y-%m-%d'),
            yesterday.strftime('%Y-%m-%d')
        ]
        
        # Track pagination state for each document type
        self.stop_pagination = {
            'bills': False,
            'draft_resolutions': False,
            'draft_initiatives': False
        }
        
        # Track pagination info for each document type
        self.pagination_info = {
            'bills': (1, 1),
            'draft_resolutions': (1, 1),
            'draft_initiatives': (1, 1)
        }
        

        
        logging.info(f"Looking for SOZD documents from: {self.target_dates}")
        
        # Start with all three document types
        document_types = [
            {
                'name': 'bills',
                'base_url': 'https://sozd.duma.gov.ru/search?q=#data_source_tab_b',
                'pagination_pattern': 'https://sozd.duma.gov.ru/search?q=&page={}#data_source_tab_b',
                'doc_kind': 'bill'
            },
            {
                'name': 'draft_resolutions',
                'base_url': 'https://sozd.duma.gov.ru/search/pp?q=#data_source_tab_p',
                'pagination_pattern': 'https://sozd.duma.gov.ru/search/pp?q=&page={}#data_source_tab_p',
                'doc_kind': 'draft_resolution'
            },
            {
                'name': 'draft_initiatives',
                'base_url': 'https://sozd.duma.gov.ru/search/c?q=',
                'pagination_pattern': 'https://sozd.duma.gov.ru/search/c?q=&page={}',
                'doc_kind': 'draft_initiative'
            }
        ]
        
        for doc_type in document_types:
            yield scrapy.Request(
                url=doc_type['base_url'],
                callback=self.parse_search_page,
                meta={'doc_type': doc_type, 'page': 1}
            )

    def parse_search_page(self, response):
        """Parse search page to extract pagination and document links"""
        soup = BeautifulSoup(response.text, 'html.parser')
        doc_type = response.meta['doc_type']
        current_page = response.meta.get('page', 1)
        
        logging.info(f"Parsing {doc_type['name']} search page {current_page}: {response.url}")
        
        # Save the page HTML for debugging
        # with open(f'sozd_{doc_type["name"]}_page_{current_page}.html', 'w', encoding='utf-8') as f:
        #     f.write(response.text)
        logging.info(f"Saved page HTML to sozd_{doc_type['name']}_page_{current_page}.html")
        
        # Check if we should stop pagination for this document type
        if self.stop_pagination.get(doc_type['name'], False):
            logging.info(f"Skipping {doc_type['name']} page {current_page} - pagination stopped due to older documents")
            return
        
        # Extract pagination info from the first page only
        if current_page == 1:
            pagination_info = self.extract_pagination_info(soup, doc_type)
            if pagination_info:
                first_page, last_page = pagination_info
                self.pagination_info[doc_type['name']] = (first_page, last_page)
                logging.info(f"Found pagination for {doc_type['name']}: first page = {first_page}, last page = {last_page}")
        
        # Extract document links from current page
        document_links = self.extract_document_links(soup, response.url)
        logging.info(f"Found {len(document_links)} document links on {doc_type['name']} page {current_page}")
        
        # Sequential processing: process one document at a time
        if document_links and not self.stop_pagination.get(doc_type['name'], False):
            yield scrapy.Request(
                url=document_links[0],
                callback=self.parse_document_detail,
                meta={
                    'doc_type': doc_type,
                    'source_url': response.url,
                    'current_page': current_page,
                    'document_index': 0,
                    'total_documents': len(document_links),
                    'remaining_links': document_links[1:],
                    'processed_count': 0
                }
            )
        else:
            logging.info(f"No documents to process on {doc_type['name']} page {current_page}")

    def extract_pagination_info(self, soup, doc_type):
        """Extract pagination information from search page"""
        # Look for pagination elements
        pagination_elements = soup.find_all(['a', 'span'], class_=re.compile(r'page|pagination'))
        
        page_numbers = set()
        
        # Extract page numbers from pagination elements
        for elem in pagination_elements:
            # Check href for page numbers
            href = elem.get('href', '')
            page_match = re.search(r'page=(\d+)', href)
            if page_match:
                page_num = int(page_match.group(1))
                page_numbers.add(page_num)
                logging.info(f"Found page number from href: {page_num}")
            
            # Check text content for page numbers
            text = elem.get_text(strip=True)
            if text.isdigit() and len(text) <= 3:  # Reasonable page number
                page_num = int(text)
                page_numbers.add(page_num)
                logging.info(f"Found page number from text: {page_num}")
        
        # Also look for pagination text like "Страница X из Y"
        pagination_text = soup.get_text()
        page_range_match = re.search(r'Страница\s+(\d+)\s+из\s+(\d+)', pagination_text)
        if page_range_match:
            current_page = int(page_range_match.group(1))
            total_pages = int(page_range_match.group(2))
            page_numbers.add(current_page)
            page_numbers.add(total_pages)
            logging.info(f"Found page range: {current_page} of {total_pages}")
        
        if page_numbers:
            first_page = 1
            last_page = max(page_numbers)
            logging.info(f"Found page numbers for {doc_type['name']}: {sorted(page_numbers)}, last page: {last_page}")
            return first_page, last_page
        
        # If no pagination found, assume it's a single page
        logging.warning(f"No pagination found for {doc_type['name']}, assuming single page")
        return 1, 1

    def extract_document_links(self, soup, page_url):
        """Extract bill detail page links from search page using both data-clickopen and <a href>"""
        links = set()
        # Extract from data-clickopen attributes
        for div in soup.find_all(attrs={"data-clickopen": True}):
            href = div.get("data-clickopen")
            if href and href.startswith("/bill/"):
                full_url = f"https://sozd.duma.gov.ru{href}"
                links.add(full_url)
                logging.info(f"Found document link (data-clickopen): {full_url}")
        # Extract from <a href="/bill/xxxxx-x">
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.startswith('/bill/'):
                full_url = f"https://sozd.duma.gov.ru{href}"
                links.add(full_url)
                logging.info(f"Found document link (<a href>): {full_url}")
        return list(links)

    def parse_document_detail(self, response):
        """Parse individual document detail page to extract content and metadata"""
        soup = BeautifulSoup(response.text, 'html.parser')
        doc_type = response.meta['doc_type']
        current_page = response.meta.get('current_page', 1)
        document_index = response.meta.get('document_index', 0)
        total_documents = response.meta.get('total_documents', 1)
        remaining_links = response.meta.get('remaining_links', [])
        processed_count = response.meta.get('processed_count', 0)

        logging.info(f"Parsing {doc_type['name']} detail page {document_index + 1}/{total_documents}: {response.url}")

        # Check if pagination is already stopped for this document type
        # (do not return early, always process all bills on the page)

        # Extract document ID from URL
        doc_id = response.url.split('/')[-1] if '/' in response.url else None

        # Extract title
        title = ""
        title_selectors = ['h1', 'h2', '.title', '.document-title', '.bill-title', 'title']
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                title = title_elem.get_text(strip=True)
                logging.info(f"Found title using '{selector}': {title}")
                break

        # Extract content/text
        content = self.extract_content(soup)

        # Extract publication date
        publication_date = self.extract_publication_date(soup, response.text)

        # Check if document is from today or yesterday FIRST (before extracting files)
        should_stop = False
        process_this_bill = True
        if publication_date:
            try:
                date_obj = datetime.strptime(publication_date, '%d.%m.%Y')
                bill_date = date_obj.strftime('%Y-%m-%d')

                if bill_date not in self.target_dates:
                    logging.info(f"Skipping {doc_type['name']} from {bill_date} - not matching {self.target_dates}")

                    # Check if this document is older than yesterday
                    doc_date = date_obj.date()
                    yesterday = datetime.now().date() - timedelta(days=1)

                    if doc_date < yesterday:
                        # Stop pagination for this document type - we've found older documents
                        self.stop_pagination[doc_type['name']] = True
                        should_stop = True
                        logging.info(f"Found document older than yesterday ({doc_date} < {yesterday}), stopping pagination for {doc_type['name']}")
                    process_this_bill = False  # Do not yield this bill, but continue to next
                else:
                    logging.info(f"Processing {doc_type['name']} from {bill_date}: {title}")
            except ValueError:
                logging.warning(f"Could not parse publication date: {publication_date}")
                # Continue processing even if date parsing fails
        else:
            logging.info(f"No publication date found for {doc_type['name']}, processing anyway")

        files = []
        if process_this_bill:
            files = self.extract_files_info(soup)
            published_at = int(datetime.now().timestamp())
            if publication_date:
                try:
                    date_obj = datetime.strptime(publication_date, '%d.%m.%Y')
                    published_at = int(date_obj.timestamp())
                except ValueError as e:
                    logging.warning(f"Failed to parse date {publication_date}: {e}")
            doc_uuid = str(uuid.uuid4())
            logging.info(f"Extracted {doc_type['name']}: {title}")
            logging.info(f"Document ID: {doc_id}")
            logging.info(f"Publication date: {publication_date}")
            logging.info(f"Content length: {len(content)}")
            yield {
                'id': doc_uuid,
                'text': content,
                'lawMetadata': {
                    'originalId': doc_id,
                    'docKind': doc_type['doc_kind'],
                    'title': title,
                    'source': 'sozd.duma.gov.ru',
                    'url': response.url,
                    'publishedAt': published_at,
                    'parsedAt': int(datetime.now().timestamp()),
                    'jurisdiction': 'RU',
                    'language': 'ru',
                    'stage': None,
                    'discussionPeriod': {
                        'start': None,
                        'end': None
                    },
                    'explanatoryNote': {
                        'fileId': None,
                        'url': None,
                        'mimeType': None
                    },
                    'summaryReports': [],
                    'commentStats': {'total': 0},
                    'files': files
                }
            }

        # Always continue to the next document on the page, even if should_stop is True
        yield from self.continue_to_next_document(doc_type, current_page, remaining_links, processed_count + 1, total_documents, should_stop)

    def extract_content(self, soup):
        """Extract main content from document page"""
        content = ""
        
        # SOZD-specific content extraction - focus on bill_data_wrap
        bill_data_wrap = soup.find('div', class_='bill_data_wrap')
        if bill_data_wrap:
            # Extract key information from bill_data_wrap
            content_parts = []
            
            # Document number
            number_elem = bill_data_wrap.find('span', id='number_oz_id')
            if number_elem:
                content_parts.append(f"Номер: {number_elem.get_text(strip=True)}")
            
            # Document title
            title_elem = bill_data_wrap.find('span', id='oz_name')
            if title_elem:
                content_parts.append(f"Название: {title_elem.get_text(strip=True)}")
            
            # Document type
            type_elem = bill_data_wrap.find('div', class_='type_of_law')
            if type_elem:
                content_parts.append(f"Тип: {type_elem.get_text(strip=True)}")
            
            # Status
            status_elem = bill_data_wrap.find('span', id='current_oz_status')
            if status_elem:
                content_parts.append(f"Статус: {status_elem.get_text(strip=True)}")
            
            # Passport data table
            passport_table = bill_data_wrap.find('table', class_='table')
            if passport_table:
                content_parts.append("Паспортные данные:")
                rows = passport_table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        if label and value:
                            content_parts.append(f"  {label}: {value}")
            
            content = "\n".join(content_parts)
            logging.info(f"Extracted SOZD-specific content: {len(content)} chars")
        
        # If no SOZD-specific content found, try general content selectors
        if not content:
            content_selectors = [
                '.document-content',
                '.bill-content',
                '.explanatory-note',
                '.main-content',
                '.content',
                '.text-content',
                '.document-text',
                '.bill-text',
                'article',
                'main',
                '.description',
                '.explanatory',
                '.note'
            ]
            
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    # Remove navigation and UI elements
                    for elem in content_elem.find_all(['nav', 'menu', '.navigation', '.sidebar', '.menu', '.search', '.filter', 'header', 'footer', 'script', 'style']):
                        elem.decompose()
                    content = content_elem.get_text(separator=' ', strip=True)
                    logging.info(f"Found content using selector '{selector}': {len(content)} chars")
                    if len(content) > 100:
                        break
        
        # If still no content, fallback to body
        if not content or len(content) < 100:
            body = soup.find('body')
            if body:
                # Remove UI elements
                for elem in body.find_all(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                    elem.decompose()
                content = body.get_text(separator=' ', strip=True)
                logging.info(f"Using body content: {len(content)} chars")
        
        # Clean up content
        if content:
            # Remove common navigation text
            content = re.sub(r'Система обеспечениязаконодательной деятельности.*?СОЗД.*?Объекты законотворчества', '', content, flags=re.DOTALL)
            content = re.sub(r'©Государственная Дума Федерального Собрания Российской Федерации.*?СОЗД ГАС.*?©ГД РФ', '', content, flags=re.DOTALL)
            content = re.sub(r'НаверхЗакрытьПредупреждение.*?Закрыть', '', content, flags=re.DOTALL)
            content = re.sub(r'Карточка законопроекта.*?Видеоописание функционала страницы', '', content, flags=re.DOTALL)
            
            # Clean up whitespace
            content = re.sub(r'\s+', ' ', content).strip()
        
        return content

    def extract_publication_date(self, soup, page_text):
        """Extract publication date from document page"""
        # Look for date patterns in the text
        date_patterns = [
            r'(\d{2}\.\d{2}\.\d{4})',  # DD.MM.YYYY
            r'Дата[:\s]*(\d{2}\.\d{2}\.\d{4})',
            r'Опубликован[:\s]*(\d{2}\.\d{2}\.\d{4})',
            r'Регистрация[:\s]*(\d{2}\.\d{2}\.\d{4})'
        ]
        
        for pattern in date_patterns:
            date_match = re.search(pattern, page_text)
            if date_match:
                return date_match.group(1)
        
        return None

    def extract_files_info(self, soup):
        """Extract only files from the 'Passport details' section of the bill detail page."""
        files = []
        seen_fileids = set()
        seen_urls = set()

        # Find the 'Passport details' section (look for a table with bill metadata)
        # The file is usually in a row with a PDF icon or a download link
        passport_table = None
        for table in soup.find_all('table'):
            if table.find(string=lambda s: s and ('Package of documents' in s or 'Пакет документов' in s)):
                passport_table = table
                break
        if passport_table:
            for row in passport_table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) >= 2:
                    # Look for a file link in the value cell
                    value_cell = cells[1]
                    link = value_cell.find('a', href=True)
                    if link:
                        href = link['href']
                        if href.startswith('/'):
                            href = f"https://sozd.duma.gov.ru{href}"
                        file_id = href.split('/')[-1]
                        if file_id in seen_fileids or href in seen_urls:
                            continue
                        seen_fileids.add(file_id)
                        seen_urls.add(href)
                        # Guess mime type
                        mime_type = 'application/octet-stream'
                        if href.lower().endswith('.pdf'):
                            mime_type = 'application/pdf'
                        elif href.lower().endswith('.doc') or href.lower().endswith('.docx'):
                            mime_type = 'application/msword'
                        elif href.lower().endswith('.xls') or href.lower().endswith('.xlsx'):
                            mime_type = 'application/vnd.ms-excel'
                        elif href.lower().endswith('.zip'):
                            mime_type = 'application/zip'
                        files.append({
                            'fileId': file_id,
                            'url': href,
                            'mimeType': mime_type
                        })
        else:
            # Fallback: look for a PDF or download link in the main card area
            card = soup.find('div', class_='bill_data_wrap')
            if card:
                link = card.find('a', href=True)
                if link:
                    href = link['href']
                    if href.startswith('/'):
                        href = f"https://sozd.duma.gov.ru{href}"
                    file_id = href.split('/')[-1]
                    if file_id not in seen_fileids and href not in seen_urls:
                        seen_fileids.add(file_id)
                        seen_urls.add(href)
                        mime_type = 'application/octet-stream'
                        if href.lower().endswith('.pdf'):
                            mime_type = 'application/pdf'
                        elif href.lower().endswith('.doc') or href.lower().endswith('.docx'):
                            mime_type = 'application/msword'
                        elif href.lower().endswith('.xls') or href.lower().endswith('.xlsx'):
                            mime_type = 'application/vnd.ms-excel'
                        elif href.lower().endswith('.zip'):
                            mime_type = 'application/zip'
                        files.append({
                            'fileId': file_id,
                            'url': href,
                            'mimeType': mime_type
                        })
        logging.info(f"Total extracted {len(files)} files from Passport details section")
        return files

    def continue_to_next_document(self, doc_type, current_page, remaining_links, processed_count, total_documents, should_stop):
        """Continue to the next document or page"""
        if should_stop or self.stop_pagination.get(doc_type['name'], False):
            logging.info(f"Not continuing - pagination stopped for {doc_type['name']}")
            return
        if remaining_links:
            next_link = remaining_links[0]
            next_remaining = remaining_links[1:]
            logging.info(f"Continuing to next document on {doc_type['name']} page {current_page}: {processed_count + 1}/{total_documents}")
            from scrapy import Request
            yield Request(
                url=next_link,
                callback=self.parse_document_detail,
                meta={
                    'doc_type': doc_type,
                    'source_url': f"page_{current_page}",
                    'current_page': current_page,
                    'document_index': processed_count,
                    'total_documents': total_documents,
                    'remaining_links': next_remaining,
                    'processed_count': processed_count
                }
            )
        else:
            # No more documents on this page, continue to next page (construct URL directly)
            yield from self.continue_to_next_page(doc_type, current_page)

    def continue_to_next_page(self, doc_type, current_page):
        """Continue to the next page if pagination is not stopped (construct URL directly)"""
        if self.stop_pagination.get(doc_type['name'], False):
            logging.info(f"Not continuing to next page - pagination stopped for {doc_type['name']}")
            return
        pagination_info = self.pagination_info.get(doc_type['name'])
        if pagination_info and current_page < pagination_info[1]:
            next_page = current_page + 1
            page_url = doc_type['pagination_pattern'].format(next_page)
            logging.info(f"Continuing to {doc_type['name']} page {next_page}: {page_url}")
            from scrapy import Request
            yield Request(
                url=page_url,
                callback=self.parse_search_page,
                meta={'doc_type': doc_type, 'page': next_page}
            )
        else:
            logging.info(f"No more pages for {doc_type['name']} (current: {current_page}, last: {pagination_info[1] if pagination_info else 'unknown'})")

 