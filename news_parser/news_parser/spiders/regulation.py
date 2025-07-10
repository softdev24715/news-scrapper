import scrapy
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import re
import uuid
import os

class RegulationSpider(scrapy.Spider):
    name = 'regulation'
    allowed_domains = ['regulation.gov.ru']
    start_urls = ['https://regulation.gov.ru/rss']

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'LOG_LEVEL': 'INFO',
    }

    def parse(self, response):
        # Get today's and yesterday's dates for filtering
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

            # Parse fields from description
            desc_fields = self.parse_description(description)

            # Convert date to timestamp
            published_at = self.parse_date(desc_fields.get('Дата создания'))
            
            # Check if the regulation is from today or yesterday
            if published_at:
                dt = datetime.fromtimestamp(published_at)
                regulation_date = dt.strftime('%Y-%m-%d')
                if regulation_date not in target_dates:
                    self.logger.debug(f"Skipping regulation from {regulation_date} - not matching {target_dates}")
                    continue
                else:
                    self.logger.info(f"Processing regulation from {regulation_date}: {title}")
            else:
                # If no date found, assume it's from today for now
                self.logger.debug(f"No date found for regulation, assuming today: {title}")

            # Visit the URL and save HTML
            if link:
                yield scrapy.Request(
                    url=link,
                    callback=self.save_page_html,
                    meta={'guid': guid, 'title': title},
                    dont_filter=True
                )

            # Compose structured item
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
                'stage': 'public_discussion',  # Default stage for regulation.gov.ru documents
            }

            processed_count += 1
            yield {
                'id': str(uuid.uuid4()),
                'text': title,
                'lawMetadata': law_metadata,
            }
        
        self.logger.info(f"Processed {processed_count} regulations from RSS feed")

    def save_page_html(self, response):
        """Save the HTML of the regulation page"""
        guid = response.meta['guid']
        title = response.meta['title']
        
        try:
            # Save page source to file
            filename = f"regulation_page_{guid}.html"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(response.text)
            self.logger.info(f"Saved page HTML to {filename} for: {title}")
            
        except Exception as e:
            self.logger.error(f"Error saving page HTML for {guid}: {e}")

    def parse_description(self, description):
        # Parse multi-line description into a dict
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
        # Parse date string like '21 июня 2025' to timestamp
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
