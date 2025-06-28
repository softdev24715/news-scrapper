#!/bin/bash

# Script to run all available spiders
cd /opt/news-scraper/app/news_parser

# Activate virtual environment
source /opt/news-scraper/bin/activate

# List of spiders to run
SPIDERS=("meduza_simple" "railinsider" "railwaygazette" "railjournal")

echo "🕷️ Starting spider run at $(date)"

for spider in "${SPIDERS[@]}"; do
    echo "Running spider: $spider"
    python -m scrapy crawl "$spider" -s LOG_LEVEL=INFO
    echo "Completed spider: $spider"
    sleep 10  # Wait between spiders
done

echo "✅ All spiders completed at $(date)" 