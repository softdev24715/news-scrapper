# News Scraper Project

A robust, cloud-ready news scraping and API platform using Scrapy, Flask, PostgreSQL, and rotating proxies.

---

## Features
- Scrapy spiders for multiple news sources
- PostgreSQL storage with deduplication
- Flask API and dashboard for management
- Rotating proxies and user-agent rotation for stealth
- Scheduler for periodic scraping
- Cloud-ready: all paths and secrets via environment variables

---

## Quick Start

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd <your-repo-directory>
```

### 2. Set up Python environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure Environment Variables
Copy `.env.example` to `.env` and fill in your values:
```bash
cp .env.example .env
```
Edit `.env` as needed:
```
DATABASE_URL=postgresql://news_user:your_password@localhost:5432/news_db
SCRAPY_PROJECT_PATH=/absolute/path/to/news_parser
PYTHON_PATH=python
ROTATING_PROXY_LIST_PATH=/absolute/path/to/proxy_list.txt
FLASK_ENV=development
FLASK_DEBUG=1
```

### 4. Prepare the Database
- Create the PostgreSQL database and user.
- Create the `spider_status` table and insert your spiders (see below).

### 5. Add Your Proxy List
Create a file at the path specified by `ROTATING_PROXY_LIST_PATH`, one proxy per line.

---

## Running Locally

### Start the Flask API and Dashboard
```bash
cd news_parser
python run.py
```
- Visit [http://localhost:5000/dashboard](http://localhost:5000/dashboard)

### Run the Scheduler (for periodic scraping)
```bash
python news_parser/scheduler.py
```

### Run a Spider Manually
```bash
cd news_parser
scrapy crawl <spider_name>
```

---

## Database Table: `spider_status`
```sql
CREATE TABLE spider_status (
    name VARCHAR PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    status VARCHAR DEFAULT 'ok',
    last_update TIMESTAMP
);
```
Insert your spiders:
```sql
INSERT INTO spider_status (name, enabled, status) VALUES
('tass', TRUE, 'ok'),
('rbc', TRUE, 'ok'),
('kommersant', TRUE, 'ok'),
('lenta', TRUE, 'ok'),
('gazeta', TRUE, 'ok'),
('graininfo', TRUE, 'ok'),
('vedomosti', TRUE, 'ok'),
('izvestia', TRUE, 'ok'),
('interfax', TRUE, 'ok'),
('forbes', TRUE, 'ok'),
('rg', TRUE, 'ok'),
('pnp', TRUE, 'ok'),
('ria', TRUE, 'ok');
```

---

## Deployment (Cloud/Production)
- Set all environment variables in your cloud environment.
- Use Gunicorn or uWSGI for Flask in production.
- Use Supervisor, systemd, or a process manager for the scheduler.
- For Docker/Cloud Run, set environment variables in your Dockerfile or service config.

---

## Environment Variables
See `.env.example` for all required variables.

---

## Proxy & User-Agent Rotation
- Proxies are loaded from the file specified by `ROTATING_PROXY_LIST_PATH`.
- User-agents are rotated automatically via middleware.

---

## API Endpoints
- `/api/scraper/status` — Get status of all spiders
- `/api/scraper/start` — Start all or selected spiders
- `/api/scraper/stop` — Stop all or selected spiders
- `/api/scraper/immediate` — Immediately run all or selected spiders

---

## License
MIT 