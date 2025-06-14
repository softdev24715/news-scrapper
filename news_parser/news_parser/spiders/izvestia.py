import scrapy
from datetime import datetime
from scrapy.spiders import XMLFeedSpider
from news_parser.items import NewsArticle
import re
from bs4 import BeautifulSoup
import uuid

class IzvestiaSpider(XMLFeedSpider):
    name = 'izvestia'
    allowed_domains = ['iz.ru']
    start_urls = ['https://iz.ru/xml/rss/all.xml']
    iterator = 'iternodes'
    itertag = 'item'
    
    # Define namespaces
    namespaces = [
        ('dc', 'http://purl.org/dc/elements/1.1/'),
        ('media', 'http://search.yahoo.com/mrss/'),  # Media RSS namespace
    ]

    def parse_node(self, response, node):
        article = NewsArticle()
        
        # Generate unique ID
        article_id = str(uuid.uuid4())
        
        # Basic article info from RSS
        source = 'iz.ru'
        url = node.xpath('link/text()').get()
        title = node.xpath('title/text()').get()
        
        # Get publication date
        pub_date = node.xpath('pubDate/text()').get()
        if pub_date:
            # Convert pubDate to timestamp
            dt = datetime.strptime(pub_date, '%a, %d %b %Y %H:%M:%S %z')
            published_at = int(dt.timestamp())
            published_at_iso = dt.isoformat()
        else:
            current_time = datetime.now()
            published_at = int(current_time.timestamp())
            published_at_iso = current_time.isoformat()
            
        # Get author using the correct namespace
        author = node.xpath('.//dc:creator/text()').get()
        if author:
            author = author.strip()
            
        # Get description/summary
        description = node.xpath('description/text()').get()
        if description:
            # Clean HTML tags from description
            clean_description = re.sub(r'<[^>]+>', '', description)
            summary = clean_description.strip()
            
        # Get video content if available
        video_content = []
        
        # Check for media:content with type video
        video_nodes = node.xpath('.//media:content[@type="video"]')
        for video in video_nodes:
            video_title = video.xpath('.//media:title/text()').get()
            video_description = video.xpath('.//media:description/text()').get()
            if video_title:
                video_content.append(f"[VIDEO] {video_title}")
            if video_description:
                video_content.append(video_description)
        
        # Check for media:group with video content
        video_groups = node.xpath('.//media:group')
        for group in video_groups:
            video_title = group.xpath('.//media:title/text()').get()
            video_description = group.xpath('.//media:description/text()').get()
            if video_title:
                video_content.append(f"[VIDEO] {video_title}")
            if video_description:
                video_content.append(video_description)
        
        # Store video content
        if video_content:
            video_content = '\n'.join(video_content)
            
        # Get article content
        article_url = url
        yield scrapy.Request(
            url=article_url,
            callback=self.parse_article,
            meta={
                'article_id': article_id,
                'source': source,
                'url': url,
                'title': title,
                'published_at': published_at,
                'published_at_iso': published_at_iso,
                'author': author,
                'summary': summary if 'summary' in locals() else None,
                'video_content': video_content if video_content else None
            }
        )

    def parse_article(self, response):
        # Get metadata from request
        meta = response.meta
        
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Get article text from the main content area
        article_text = []
        
        # Try different possible content containers
        content_containers = [
            'div.article-page__text',
            'div.text-article',
            'div[itemprop="articleBody"]'
        ]
        
        for container in content_containers:
            content = soup.select_one(container)
            if content:
                # Get all text paragraphs
                paragraphs = content.find_all('p')
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    if text:
                        article_text.append(text)
                break
        
        # Create article with required structure
        article = NewsArticle()
        article['id'] = meta['article_id']
        article['text'] = '\n'.join(article_text)
        article['metadata'] = {
            'source': meta['source'],
            'published_at': meta['published_at'],
            'published_at_iso': meta['published_at_iso'],
            'url': meta['url'],
            'header': meta['title'],
            'parsed_at': int(datetime.now().timestamp())
        }
        
        yield article 