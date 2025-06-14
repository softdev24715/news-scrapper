#!/bin/bash

# Update system
apt-get update
apt-get install -y python3-pip python3-venv git

# Create and activate virtual environment
python3 -m venv /opt/news-scraper
source /opt/news-scraper/bin/activate

# Clone repository (replace with your actual repository URL)
git clone https://github.com/your-username/news-scraper.git /opt/news-scraper/app
cd /opt/news-scraper/app

# Install dependencies
pip install -r requirements.txt

# Create systemd service
cat > /etc/systemd/system/news-scraper.service << EOL
[Unit]
Description=News Scraper Web Application
After=network.target

[Service]
User=root
WorkingDirectory=/opt/news-scraper/app
Environment="PATH=/opt/news-scraper/bin"
Environment="PROXY_LIST=http://UvPRiZ3u:AixOXwU4nI@46.3.133.104:50100"
ExecStart=/opt/news-scraper/bin/gunicorn --bind 0.0.0.0:8080 --workers 1 --threads 8 --timeout 0 web.app:app
Restart=always

[Install]
WantedBy=multi-user.target
EOL

# Enable and start service
systemctl enable news-scraper
systemctl start news-scraper 