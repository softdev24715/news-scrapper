# News Parser

A Scrapy-based news parser with PostgreSQL storage and Flask API backend.

## Features

- Scrapes multiple Russian news sources
- Stores articles in PostgreSQL database
- RESTful API for accessing the data
- Support for multiple news sources
- Automatic duplicate detection
- Rate limiting and user agent rotation

## Setup

1. Create a PostgreSQL database:
```sql
CREATE DATABASE news_db;
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure database connection:
Edit `news_parser/news_parser/settings.py` and update the `DATABASE_URL`:
```python
DATABASE_URL = "postgresql://username:password@localhost:5432/news_db"
```

## Usage

### Running the Scraper

To run a specific spider:
```bash
scrapy crawl <spider_name>
```

Available spiders:
- forbes
- gazeta
- graininfo
- interfax
- izvestia
- kommersant
- lenta
- pnp
- ria
- rbc
- rg
- tass
- vedomosti

### Running the API Server

```bash
cd news_parser/web
python app.py
```

The API will be available at `http://localhost:5000`

## API Endpoints

### GET /api/articles
Get paginated list of articles

Query parameters:
- page (default: 1)
- per_page (default: 10)
- source (optional)
- days (default: 7)

### GET /api/articles/<article_id>
Get a specific article by ID

### GET /api/sources
Get list of all news sources

### GET /api/stats
Get statistics about articles:
- Total articles count
- Articles count by source
- Daily article counts for the last 7 days

## Data Structure

Each article contains:
- id: Unique identifier
- text: Article content
- source: News source
- url: Original article URL
- header: Article title
- published_at: Unix timestamp
- published_at_iso: ISO format date
- parsed_at: When the article was scraped
- author: Article author (if available)
- categories: List of categories/tags
- images: List of image URLs 