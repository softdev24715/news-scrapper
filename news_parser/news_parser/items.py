# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NewsArticle(scrapy.Item):
    # Main content
    id = scrapy.Field()  # Unique identifier
    text = scrapy.Field()
    
    # Metadata structure
    metadata = scrapy.Field()  # Will contain all metadata fields
    
    # Individual fields for internal use
    source = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    published_at = scrapy.Field()  # Unix timestamp
    published_at_iso = scrapy.Field()  # ISO format date
    parsed_at = scrapy.Field()  # When we parsed the article
    author = scrapy.Field()   # Article author
    summary = scrapy.Field()  # Article summary from RSS
    categories = scrapy.Field()  # List of categories/tags
    images = scrapy.Field()     # List of image URLs
    
    # RIA.ru specific fields
    article_id = scrapy.Field()  # Unique article ID
    article_section = scrapy.Field()  # Article section/category
    article_keywords = scrapy.Field()  # Article keywords
    article_description = scrapy.Field()  # Article description
    article_modified = scrapy.Field()  # Last modified date
    article_created = scrapy.Field()  # Creation date
    article_published = scrapy.Field()  # Publication date
    article_alternative_headline = scrapy.Field()  # Alternative headline
    article_name = scrapy.Field()  # Article name
    article_genre = scrapy.Field()  # Article genre
    article_in_language = scrapy.Field()  # Article language
    article_license = scrapy.Field()  # Article license
    article_publishing_principles = scrapy.Field()  # Publishing principles
    article_about = scrapy.Field()  # Article about
    article_content_location = scrapy.Field()  # Content location
    article_citation = scrapy.Field()  # Article citations
    article_associated_media = scrapy.Field()  # Associated media
    article_main_entity_of_page = scrapy.Field()  # Main entity of page
