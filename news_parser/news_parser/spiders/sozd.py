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
        """Start with all three document types to find all documents"""
        # Track pagination info for each document type
        self.pagination_info = {
            'bills': (1, 1),
            'draft_resolutions': (1, 1),
            'draft_initiatives': (1, 1)
        }
        
        # Track pagination state for each document type
        self.stop_pagination = {
            'bills': False,
            'draft_resolutions': False,
            'draft_initiatives': False
        }
        
        logging.info("Looking for all SOZD documents (no date filtering)")
        
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
        
        # Check if pagination is already stopped for this document type
        if self.stop_pagination[doc_type['name']]:
            logging.info(f"Skipping {doc_type['name']} page {current_page} - pagination already stopped")
            return
        
        # Save the page HTML for debugging
        # with open(f'sozd_{doc_type["name"]}_page_{current_page}.html', 'w', encoding='utf-8') as f:
        #     f.write(response.text)
        logging.info(f"Saved page HTML to sozd_{doc_type['name']}_page_{current_page}.html")
        
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
        if document_links:
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
        """Extract pagination information from search page - improved like EAEU"""
        page_numbers = set()  # Use set to avoid duplicates
        
        # Method 1: Look for pagination elements with page/pagination classes
        pagination_elements = soup.find_all(['a', 'span'], class_=re.compile(r'page|pagination'))
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
            if text.isdigit() and len(text) <= 4:  # Reasonable page number length
                page_num = int(text)
                page_numbers.add(page_num)
                logging.info(f"Found page number from text: {page_num}")
        
        # Method 2: Look for pagination text like "Страница X из Y"
        pagination_text = soup.get_text()
        page_range_match = re.search(r'Страница\s+(\d+)\s+из\s+(\d+)', pagination_text)
        if page_range_match:
            current_page = int(page_range_match.group(1))
            total_pages = int(page_range_match.group(2))
            page_numbers.add(current_page)
            page_numbers.add(total_pages)
            logging.info(f"Found page range: {current_page} of {total_pages}")
        
        # Method 3: Look for all numbers that could be page numbers in the entire page
        all_numbers = re.findall(r'page=(\d+)', str(soup))
        for num_str in all_numbers:
            page_num = int(num_str)
            page_numbers.add(page_num)
            logging.info(f"Found page number from HTML pattern: {page_num}")
        
        # Method 4: Look for numbers that appear to be page numbers in pagination areas
        pagination_areas = soup.find_all(['div', 'nav'], class_=re.compile(r'pagination|page|nav'))
        for area in pagination_areas:
            text_numbers = re.findall(r'\b(\d{1,4})\b', area.get_text())
            for num_str in text_numbers:
                page_num = int(num_str)
                # Filter out obviously non-page numbers
                if page_num > 0 and page_num <= 10000:  # Reasonable page number range
                    page_numbers.add(page_num)
                    logging.info(f"Found potential page number from pagination area: {page_num}")
        
        if page_numbers:
            first_page = 1  # Always start from 1
            last_page = max(page_numbers)
            sorted_pages = sorted(page_numbers)
            logging.info(f"All found page numbers for {doc_type['name']}: {sorted_pages}")
            logging.info(f"Last page determined: {last_page}")
            logging.info(f"Total pages to visit: {last_page}")
            return first_page, last_page
        
        # If no pagination found, assume it's a single page
        logging.warning(f"No pagination found for {doc_type['name']}, assuming single page")
        return 1, 1

    def extract_document_links(self, soup, page_url):
        """Extract document detail page links from search page using multiple methods"""
        links = set()
        
        # Method 1: Extract from data-clickopen attributes
        for div in soup.find_all(attrs={"data-clickopen": True}):
            href = div.get("data-clickopen")
            if href and (href.startswith("/bill/") or href.startswith("/document/")):
                full_url = f"https://sozd.duma.gov.ru{href}"
                links.add(full_url)
                logging.info(f"Found document link (data-clickopen): {full_url}")
        
        # Method 2: Extract from <a href> with various patterns
        for a in soup.find_all('a', href=True):
            href = a['href']
            # Check for various document URL patterns
            if (href.startswith('/bill/') or 
                href.startswith('/document/') or 
                href.startswith('/search/') or
                '/bill/' in href or
                '/document/' in href):
                if href.startswith('/'):
                    full_url = f"https://sozd.duma.gov.ru{href}"
                elif href.startswith('http'):
                    full_url = href
                else:
                    full_url = f"https://sozd.duma.gov.ru/{href}"
                links.add(full_url)
                logging.info(f"Found document link (<a href>): {full_url}")
        
        # Method 3: Look for document containers and extract links from them
        document_containers = soup.find_all(['div', 'tr'], class_=re.compile(r'document|bill|item|row'))
        for container in document_containers:
            for link in container.find_all('a', href=True):
                href = link['href']
                if (href.startswith('/bill/') or 
                    href.startswith('/document/') or 
                    href.startswith('/search/') or
                    '/bill/' in href or
                    '/document/' in href):
                    if href.startswith('/'):
                        full_url = f"https://sozd.duma.gov.ru{href}"
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        full_url = f"https://sozd.duma.gov.ru/{href}"
                    links.add(full_url)
                    logging.info(f"Found document link (container): {full_url}")
        
        # Method 4: Look for any clickable elements that might be document links
        clickable_elements = soup.find_all(['div', 'span', 'td'], onclick=True)
        for elem in clickable_elements:
            onclick = elem.get('onclick', '')
            # Extract URL from onclick handlers
            url_match = re.search(r"['\"]([^'\"]*bill[^'\"]*)['\"]", onclick)
            if url_match:
                href = url_match.group(1)
                if href.startswith('/'):
                    full_url = f"https://sozd.duma.gov.ru{href}"
                elif href.startswith('http'):
                    full_url = href
                else:
                    full_url = f"https://sozd.duma.gov.ru/{href}"
                links.add(full_url)
                logging.info(f"Found document link (onclick): {full_url}")
        
        logging.info(f"Total document links found: {len(links)}")
        return list(links)

    def extract_discussion_period(self, soup):
        """Extract discussion period start and end dates from event timeline."""
        import datetime
        import re

        discussion_period = {}
        event_dates = []

        # Find all event dates in the bill history/event timeline
        bill_history = soup.find('div', class_='bill_history_wrap bill_hist')
        if bill_history:
            date_spans = bill_history.find_all(string=re.compile(r'\d{2}\.\d{2}\.\d{4}'))
            for date_str in date_spans:
                match = re.search(r'(\d{2}\.\d{2}\.\d{4})', date_str)
                if match:
                    event_dates.append(match.group(1))

        if event_dates:
            try:
                parsed_dates = [datetime.datetime.strptime(d, '%d.%m.%Y') for d in event_dates]
                parsed_dates.sort()
                discussion_period['start'] = parsed_dates[0].strftime('%d.%m.%Y')
                discussion_period['end'] = parsed_dates[-1].strftime('%d.%m.%Y')
            except Exception as e:
                logging.warning(f"Failed to parse event dates for discussion period: {e}")
                discussion_period['start'] = None
                discussion_period['end'] = None
        else:
            discussion_period['start'] = None
            discussion_period['end'] = None

        return discussion_period

    def should_stop_pagination(self, doc_type, publication_date):
        """Check if we should stop pagination based on document date"""
        if not publication_date:
            return False
        
        try:
            # Parse the publication date (format: DD.MM.YYYY)
            date_obj = datetime.strptime(publication_date, '%d.%m.%Y')
            yesterday = datetime.now().date() - timedelta(days=1)
            
            # If document is older than yesterday, stop pagination
            if date_obj.date() < yesterday:
                logging.info(f"Document from {date_obj.date()} is older than yesterday ({yesterday}), should stop pagination for {doc_type['name']}")
                return True
        except Exception as e:
            logging.warning(f"Error checking document date: {e}")
        
        return False

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

        # Extract document ID from URL
        doc_id = response.url.split('/')[-1] if '/' in response.url else None

        # Extract main title for lawMetadata['title']
        title_elem = soup.find('span', id='oz_name')
        title = title_elem.get_text(strip=True) if title_elem else ""
        text = ""

        # Extract content/text (for other uses, not for 'text' field)
        content = self.extract_content(soup)

        # Extract publication date
        publication_date = self.extract_publication_date(soup, response.text)

        # Process all documents regardless of date
        files = self.extract_files_info(soup)
        published_at = int(datetime.now().timestamp())
        if publication_date:
            try:
                date_obj = datetime.strptime(publication_date, '%d.%m.%Y')
                published_at = int(date_obj.timestamp())
                logging.info(f"Processing {doc_type['name']} from {publication_date}: {title}")
            except ValueError as e:
                logging.warning(f"Failed to parse date {publication_date}: {e}")
        else:
            logging.info(f"No publication date found for {doc_type['name']}, processing anyway")

        doc_uuid = str(uuid.uuid4())
        logging.info(f"Extracted {doc_type['name']}: {title}")
        logging.info(f"Document ID: {doc_id}")
        logging.info(f"Publication date: {publication_date}")
        logging.info(f"Content length: {len(content)}")
        yield {
            'id': doc_uuid,
            'text': text,
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
                'discussionPeriod': self.extract_discussion_period(soup),
                'explanatoryNote': None,
                'summaryReports': None,
                'commentStats': None,
                'files': files
            }
        }

        # Check if we should stop pagination based on document date
        # Only stop if we've processed multiple documents and they're all old
        if self.should_stop_pagination(doc_type, publication_date):
            # Only stop if this is not the first document on the page
            if processed_count > 0:
                self.stop_pagination[doc_type['name']] = True
                logging.info(f"Found document older than yesterday after processing {processed_count + 1} documents, stopping pagination for {doc_type['name']}")
                # Don't continue to next document, stop processing this document type
                return
            else:
                logging.info(f"First document is old, but continuing to check other documents on this page")
        
        # Continue to the next document on the page
        yield from self.continue_to_next_document(doc_type, current_page, remaining_links, processed_count + 1, total_documents)

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
        """Extract all real downloadable files from the document page, filtering out navigation, anchor, RSS, and non-file links."""
        files = []
        seen_fileids = set()
        seen_urls = set()

        def is_valid_file_link(href):
            if not href:
                return False
            # Exclude anchors, queries, and RSS
            if href.startswith('#') or href.startswith('?') or '/rss' in href:
                return False
            # Exclude external links not on sozd.duma.gov.ru
            if href.startswith('http') and not href.startswith('https://sozd.duma.gov.ru'):
                return False
            # Only allow /download/UUID or /Files/GetFile?fileid=...
            if '/download/' in href:
                file_id = href.split('/')[-1]
                # Must look like a UUID
                if re.match(r'^[a-f0-9-]{16,}$', file_id):
                    return True
            if '/Files/GetFile' in href and 'fileid=' in href:
                file_id = re.search(r'fileid=([a-f0-9-]{16,})', href)
                if file_id:
                    return True
            return False

        def add_file(link):
            href = link.get('href')
            if not is_valid_file_link(href):
                return
            if href.startswith('/'):
                href = f"https://sozd.duma.gov.ru{href}"
            file_id = href.split('/')[-1]
            if '/Files/GetFile' in href:
                m = re.search(r'fileid=([a-f0-9-]{16,})', href)
                if m:
                    file_id = m.group(1)
            if file_id in seen_fileids or href in seen_urls:
                return
            seen_fileids.add(file_id)
            seen_urls.add(href)
            mime_type = self.get_mime_type_from_html(link, href)
            files.append({
                'fileId': file_id,
                'url': href,
                'mimeType': mime_type
            })

        # 1. Passport details table (all links)
        for table in soup.find_all('table'):
            if table.find(string=lambda s: s and ('Package of documents' in s or 'Пакет документов' in s)):
                for row in table.find_all('tr'):
                    cells = row.find_all('td')
                    for cell in cells:
                        for link in cell.find_all('a', href=True):
                            add_file(link)

        # 2. bill_data_wrap area (all links)
        bill_data_wrap = soup.find('div', class_='bill_data_wrap')
        if bill_data_wrap:
            for link in bill_data_wrap.find_all('a', href=True):
                add_file(link)

        # 3. bill_history_wrap and other sections
        additional_sections = [
            'bill_history_wrap',
            'bill_discussion_wrap',
            'bill_attachments_wrap',
            'bill_related_wrap',
            'bill_events_wrap',
        ]
        for section_class in additional_sections:
            section = soup.find('div', class_=section_class)
            if section:
                for link in section.find_all('a', href=True):
                    add_file(link)

        logging.info(f"Total extracted {len(files)} valid downloadable files from all sections")
        return files

    def get_mime_type_from_html(self, link_element, url):
        """Extract MIME type from HTML structure and fallback to URL extension"""
        # First, try to get format from HTML structure
        format_classes = [
            'format-pdf', 'format-doc', 'format-docx', 'format-xls', 'format-xlsx',
            'format-zip', 'format-rar', 'format-txt', 'format-rtf'
        ]
        for format_class in format_classes:
            if link_element.find(class_=format_class):
                return format_class.replace('format-', '')
        icon_elements = link_element.find_all(class_=re.compile(r'icon-file|format-'))
        for icon in icon_elements:
            icon_classes = icon.get('class', [])
            for class_name in icon_classes:
                if class_name.startswith('format-'):
                    return class_name.replace('format-', '')
        return self.get_mime_type_from_url(url)

    def get_mime_type_from_url(self, url):
        """Determine MIME type based on file extension (fallback method)"""
        url_lower = url.lower()
        if url_lower.endswith('.pdf'):
            return 'pdf'
        elif url_lower.endswith('.doc'):
            return 'doc'
        elif url_lower.endswith('.docx'):
            return 'docx'
        elif url_lower.endswith('.xls'):
            return 'xls'
        elif url_lower.endswith('.xlsx'):
            return 'xlsx'
        elif url_lower.endswith('.zip'):
            return 'zip'
        elif url_lower.endswith('.rar'):
            return 'rar'
        elif url_lower.endswith('.txt'):
            return 'txt'
        elif url_lower.endswith('.rtf'):
            return 'rtf'
        else:
            return 'unknown'

    def continue_to_next_document(self, doc_type, current_page, remaining_links, processed_count, total_documents):
        """Continue to the next document or page"""
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
        """Continue to the next page (construct URL directly)"""
        # Check if pagination is stopped for this document type
        if self.stop_pagination[doc_type['name']]:
            logging.info(f"Pagination stopped for {doc_type['name']}, not continuing to next page")
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

 