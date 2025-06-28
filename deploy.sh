#!/bin/bash

# Deployment script for Google Cloud Compute Engine
set -e

echo "🚀 Starting deployment..."

# Update system
sudo apt-get update
sudo apt-get install -y python3-venv python3-pip

# Navigate to project directory
cd /path/to/your/rus_scraper

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "📥 Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Navigate to news_parser directory
cd news_parser

# Install Scrapy dependencies
pip install scrapy

# Test the setup
echo "🧪 Testing setup..."
scrapy list

echo "✅ Deployment completed successfully!"

# Optional: Set up service
echo "🤔 Do you want to set up as a systemd service? (y/n)"
read -r response
if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
    echo "📋 Setting up systemd service..."
    # Copy service file and enable it
    sudo cp ../news-scraper.service /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable news-scraper.service
    sudo systemctl start news-scraper.service
    echo "🎉 Service started! Check status with: sudo systemctl status news-scraper.service"
fi

echo "🎯 Setup complete! Your scraper is ready to run." 