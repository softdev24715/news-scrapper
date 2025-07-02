# Scrapy settings for news_parser project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import os

BOT_NAME = "news_parser"

SPIDER_MODULES = ["news_parser.spiders"]
NEWSPIDER_MODULE = "news_parser.spiders"

ADDONS = {}

# Database settings
DATABASE_URL="postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres"

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests
CONCURRENT_REQUESTS = 16

# Configure a delay for requests for the same website
DOWNLOAD_DELAY = 3

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
   "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
   "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
   "Accept-Encoding": "gzip, deflate, br",
   "Connection": "keep-alive",
   "Upgrade-Insecure-Requests": "1",
}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "news_parser.middlewares.NewsParserSpiderMiddleware": 543,
#}

# Disable rotating proxy
ROTATING_PROXY_LIST = []
ROTATING_PROXY_PAGE_RETRY_TIMES = 0

# Disable proxy middleware
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': None,
    'news_parser.middlewares.NewsParserDownloaderMiddleware': 543,
    'news_parser.middlewares.RotateUserAgentMiddleware': None,
    'rotating_proxies.middlewares.RotatingProxyMiddleware': None,
    'rotating_proxies.middlewares.BanDetectionMiddleware': None,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
EXTENSIONS = {
    "scrapy.extensions.telnet.TelnetConsole": None,
    "scrapy.extensions.feedexport.FeedExporter": None,  # Disable feed export
}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
   "news_parser.pipelines.PostgreSQLPipeline": 400,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

# Disable duplicate pipeline settings
FEEDS = None

# Retry settings
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 429]

# Logging settings
LOG_LEVEL = 'DEBUG'

# Feed export settings
FEED_FORMAT = 'json'
FEED_URI = 'output/%(name)s_%(time)s.json'

# Disable HTML saving
FEEDS = {
    'output/%(name)s_%(time)s.json': {
        'format': 'json',
        'encoding': 'utf-8',
        'store_empty': False,
        'overwrite': True,
    },
}

# Proxy settings
PROXY_URL = 'http://googlecompute:xd23rXPEmq2+23@90.156.202.84:3128'

# Set proxy environment variables
os.environ['HTTP_PROXY'] = PROXY_URL
os.environ['HTTPS_PROXY'] = PROXY_URL

# Enable debug logging
LOG_LEVEL = 'DEBUG'

# Optional: Tune ban detection and retry
ROTATING_PROXY_PAGE_RETRY_TIMES = 5
ROTATING_PROXY_BACKOFF_BASE = 300  # seconds
ROTATING_PROXY_BACKOFF_CAP = 600   # seconds
