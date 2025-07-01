import scrapy
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta
import re
import uuid

class PravoSpider(scrapy.Spider):
    name = 'pravo'
    allowed_domains = ['publication.pravo.gov.ru']
    start_urls = ['http://publication.pravo.gov.ru/api/rss?pageSize=200']

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'LOG_LEVEL': 'DEBUG',
    }

    def parse(self, response):
        # today = date.today() - timedelta(days=1)
        today = datetime.now().strftime('%Y-%m-%d')
        self.logger.info(f"Looking for documents from: {today}")

        root = ET.fromstring(response.text)
        
        for item in root.findall('.//item'):
            title = item.findtext('title')
            link = item.findtext('link')
            description = item.findtext('description')
            pub_date = item.findtext('pubDate')
            
            # Parse publication number and date from description
            pub_info = self.parse_publication_info(description)
            
            # Check if this document is from today
            if not self.is_today(pub_info.get('publicationDate'), today):
                self.logger.debug(f"Skipping document from {pub_info.get('publicationDate')} - not matching {today}")
                continue
            
            self.logger.info(f"Processing document from {pub_info.get('publicationDate')}: {title}")
            
            # Convert publication date to timestamp
            published_at = self.parse_date(pub_date) if pub_date else None
            
            # Compose text content from title and description
            text_content = title
            if description:
                text_content = f"{title}\n\n{description}"
            
            # Compose structured item with exact database schema fields
            law_metadata = {
                'originalId': pub_info.get('publicationNumber', ''),
                'docKind': self.extract_doc_kind(title),
                'title': title,
                'source': 'publication.pravo.gov.ru',
                'url': link,
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
                'commentStats': {'total': 0}
            }

            yield {
                'id': str(uuid.uuid4()),
                'text': text_content,
                'lawMetadata': law_metadata
            }

    def is_today(self, publication_date_str, today_str):
        """Check if publication date is today"""
        if not publication_date_str:
            return False
            
        try:
            # Parse date in format "DD.MM.YYYY"
            pub_date = datetime.strptime(publication_date_str, '%d.%m.%Y').date()
            today_date = datetime.strptime(today_str, '%Y-%m-%d').date()
            return pub_date == today_date
        except ValueError:
            return False

    def parse_publication_info(self, description):
        """Parse publication number and date from description"""
        info = {}
        if not description:
            return info
            
        # Extract publication number
        pub_num_match = re.search(r'Номер опубликования:\s*([^;]+)', description)
        if pub_num_match:
            info['publicationNumber'] = pub_num_match.group(1).strip()
            
        # Extract publication date
        pub_date_match = re.search(r'Дата опубликования:\s*(\d{2}\.\d{2}\.\d{4})', description)
        if pub_date_match:
            info['publicationDate'] = pub_date_match.group(1)
            
        return info

    def extract_doc_kind(self, title):
        """Extract document kind from title"""
        if not title:
            return ''
            
        # Common document types in Russian legal system
        doc_types = [
            'Постановление Правительства',
            'Распоряжение Правительства', 
            'Указ Президента',
            'Федеральный закон',
            'Приказ',
            'Постановление',
            'Распоряжение',
            'Указ Губернатора',
            'Постановление Губернатора',
            'Решение',
        ]
        
        for doc_type in doc_types:
            if doc_type in title:
                return doc_type
                
        return ''

    def parse_date(self, date_str):
        """Parse RFC date string to timestamp"""
        if not date_str:
            return None
        try:
            # Parse RFC format like "Sat, 21 Jun 2025 00:00:00 +03:00"
            dt = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')
            return int(dt.timestamp())
        except ValueError:
            return None 