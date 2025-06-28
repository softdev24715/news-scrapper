#!/bin/bash

# Add cron jobs for different spiders
# Run every 6 hours

# Add to crontab
(crontab -l 2>/dev/null; echo "0 */6 * * * cd /path/to/your/rus_scraper/news_parser && /path/to/your/rus_scraper/venv/bin/python -m scrapy crawl meduza_simple -s LOG_LEVEL=INFO") | crontab -

# Add more spiders if needed
# (crontab -l 2>/dev/null; echo "30 */6 * * * cd /path/to/your/rus_scraper/news_parser && /path/to/your/rus_scraper/venv/bin/python -m scrapy crawl railinsider -s LOG_LEVEL=INFO") | crontab -

echo "Cron jobs added successfully!"
echo "Current crontab:"
crontab -l 