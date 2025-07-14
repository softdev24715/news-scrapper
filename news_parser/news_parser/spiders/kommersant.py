import scrapy
from datetime import datetime, timedelta
from scrapy.spiders import SitemapSpider
from news_parser.items import NewsArticle
from bs4 import BeautifulSoup
import logging
import uuid
from lxml import etree

class KommersantSpider(SitemapSpider):
    name = 'kommersant'
    allowed_domains = ['kommersant.ru']
    sitemap_urls = ['https://www.kommersant.ru/sitemaps/sitemap_daily.xml']
    sitemap_rules = [
        ('/doc/', 'parse_article'),
        ('/news/', 'parse_article')
    ]

    def __init__(self, *args, **kwargs):
        super(KommersantSpider, self).__init__(*args, **kwargs)
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        self.target_dates = [
            today.strftime('%Y-%m-%d'),
            yesterday.strftime('%Y-%m-%d')
        ]
        logging.info(f"Initializing Kommersant spider for dates: {self.target_dates}")
        logging.info(f"Today: {today.strftime('%Y-%m-%d')}, Yesterday: {yesterday.strftime('%Y-%m-%d')}")

    def sitemap_parse(self, response):
        ns = {
            'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9',
            'news': 'http://www.google.com/schemas/sitemap-news/0.9',
            'image': 'http://www.google.com/schemas/sitemap-image/1.1'
        }
        root = etree.fromstring(response.body)
        for url in root.xpath('//sm:url', namespaces=ns):
            loc = url.xpath('sm:loc/text()', namespaces=ns)
            pub_date = url.xpath('news:news/news:publication_date/text()', namespaces=ns)
            if loc and pub_date:
                pub_date_val = pub_date[0]
                entry_date = pub_date_val.split('T')[0]
                if entry_date in self.target_dates:
                    logging.info(f"Scheduling article {loc[0]} with publication_date {pub_date_val}")
                    yield scrapy.Request(
                        loc[0],
                        callback=self.parse_article,
                        meta={'publication_date': pub_date_val}
                    )
                else:
                    logging.debug(f"Skipping article from {entry_date} (not today or yesterday): {loc[0]}")
            else:
                logging.debug(f"Skipping url entry with missing loc or publication_date")

    def parse_article(self, response):
        import uuid
        from bs4 import BeautifulSoup
        article_id = str(uuid.uuid4())
        soup = BeautifulSoup(response.text, 'html.parser')
        title_elem = soup.find('h1', class_='doc_header__name')
        if title_elem:
            title_text = title_elem.get_text(strip=True)
        else:
            title_elem = soup.find('h1')
            title_text = title_elem.get_text(strip=True) if title_elem else None
        text_parts = []
        content_div = soup.find('div', class_='doc__body')
        if content_div:
            paragraphs = content_div.find_all('p', class_=['doc__text', 'doc__thought'])
            for p in paragraphs:
                if 'document_authors' not in p.get('class', []):
                    text = p.get_text(strip=True)
                    if text:
                        text_parts.append(text)
        # Extract publication date from meta tag in HTML
        published_at = None
        published_at_iso = None
        article_date = None
        meta_pub_time = response.css('meta[property="article:published_time"]::attr(content)').get()
        if meta_pub_time:
            try:
                if '+' in meta_pub_time:
                    date_part = meta_pub_time.split('+')[0]
                    dt = datetime.fromisoformat(date_part)
                else:
                    dt = datetime.fromisoformat(meta_pub_time)
                published_at = int(dt.timestamp())
                published_at_iso = dt.isoformat()
                article_date = dt.strftime('%Y-%m-%d')
                logging.info(f"Parsed article date from meta tag: {article_date} (timestamp: {published_at})")
            except Exception as e:
                logging.warning(f"Could not parse date '{meta_pub_time}' from meta tag: {e}")
        if not published_at:
            # Fallback to current time
            current_time = datetime.now()
            published_at = int(current_time.timestamp())
            published_at_iso = current_time.isoformat()
            article_date = current_time.strftime('%Y-%m-%d')
            logging.warning("No date found in article meta, using current time")
        article = NewsArticle()
        article['id'] = article_id
        article['text'] = '\n'.join(text_parts)
        article['metadata'] = {
            'source': 'kommersant',
            'published_at': published_at,
            'published_at_iso': published_at_iso,
            'url': response.url,
            'header': title_text,
            'parsed_at': int(datetime.now().timestamp())
        }
        logging.info(f"Yielding article from {article_date}: {response.url}")
        logging.info(f"Published timestamp: {published_at}")
        logging.info(f"Published ISO: {published_at_iso}")
        logging.info(f"Title found: {title_text}")
        logging.info(f"Text length: {len(article['text'])}")
        yield article 