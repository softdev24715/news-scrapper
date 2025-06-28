#!/bin/bash

# Update system and install dependencies
apt-get update
apt-get install -y python3-pip python3-venv git

# Create and activate virtual environment
python3 -m venv /opt/news-scraper
source /opt/news-scraper/bin/activate

# Clone repository (replace with your actual repository URL)
git clone https://github.com/your-username/rus_scraper.git /opt/news-scraper/app
cd /opt/news-scraper/app/news_parser

# Install Scrapy and dependencies
pip install scrapy
pip install -r requirements.txt

# Create systemd service for Scrapy spiders
cat > /etc/systemd/system/news-scraper.service << EOL
[Unit]
Description=News Scraper Service (Scrapy Spiders)
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/news-scraper/app/news_parser
Environment="PATH=/opt/news-scraper/bin"
Environment="PROXY_LIST=http://UvPRiZ3u:AixOXwU4nI@46.3.133.104:50100"
# Run all spiders in sequence
ExecStart=/opt/news-scraper/bin/python -m scrapy crawl meduza_simple -s LOG_LEVEL=INFO
Restart=always
RestartSec=300

[Install]
WantedBy=multi-user.target
EOL

# Create a cron job for regular spider runs
cat > /etc/cron.d/news-scraper << EOL
# Run spiders every 6 hours
0 */6 * * * root cd /opt/news-scraper/app/news_parser && /opt/news-scraper/bin/python -m scrapy crawl meduza_simple -s LOG_LEVEL=INFO
30 */6 * * * root cd /opt/news-scraper/app/news_parser && /opt/news-scraper/bin/python -m scrapy crawl railinsider -s LOG_LEVEL=INFO
EOL

# Enable and start service
systemctl enable news-scraper
systemctl start news-scraper

# Test the setup
echo "Testing Scrapy installation..."
/opt/news-scraper/bin/python -m scrapy list

echo "✅ News scraper service deployed successfully!"
echo "📊 Check service status: systemctl status news-scraper"
echo "📋 View logs: journalctl -u news-scraper -f" 