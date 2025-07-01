# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NewsArticle(scrapy.Item):
    # Main content - matches exactly what spiders produce
    id = scrapy.Field()  # Unique identifier
    text = scrapy.Field()  # Article content
    metadata = scrapy.Field()  # All metadata as JSON object


class LegalDocument(scrapy.Item):
    # Legal document structure - matches exactly what legal spiders produce
    id = scrapy.Field()  # Unique identifier
    text = scrapy.Field()  # Document content
    lawMetadata = scrapy.Field()  # All law metadata as JSON object
