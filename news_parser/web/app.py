import os
import json
import subprocess
import logging
import configparser
from datetime import datetime
from flask import Flask, jsonify, request, render_template, redirect, url_for, g
from flask_cors import CORS
from sqlalchemy import desc, create_engine, text
import sys
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Add the parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_parser.models import Article, init_db

# Load environment variables from .env file
load_dotenv()

# Load configuration from config.ini
def load_config():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.ini')
    
    if os.path.exists(config_path):
        config.read(config_path)
    else:
        # Default configuration if config.ini doesn't exist
        config['Database'] = {
            'DATABASE_URL': 'postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres'
        }
        config['Scrapy'] = {
            'SCRAPY_PROJECT_PATH': 'news_parser',
            'PYTHON_PATH': 'python'
        }
        config['Web'] = {
            'HOST': '0.0.0.0',
            'PORT': '5001',
            'DEBUG': 'True'
        }
    
    return config

# Load configuration
config = load_config()

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Initialize database connection
try:
    db_url = os.getenv('DATABASE_URL', config.get('Database', 'DATABASE_URL', fallback='postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres'))
    db = init_db(db_url)
    print("Successfully connected to the database!")
except Exception as e:
    print("Error connecting to the database:")
    print(f"Please make sure PostgreSQL is running and the database 'postgres' exists.")
    print(f"Current database URL: {db_url}")
    print(f"Error details: {str(e)}")
    print("\nTo fix this:")
    print("1. Create the database: CREATE DATABASE postgres;")
    print("2. Set the correct DATABASE_URL in .env file or environment variable")
    print("3. Make sure PostgreSQL is running: sudo service postgresql status")
    raise

# For spider status management
engine = create_engine(db_url)

# Mapping of spider names to site domains
SPIDER_SITE_MAP = {
    # News spiders
    'tass': 'tass.ru',
    'rbc': 'rbc.ru',
    'kommersant': 'kommersant.ru',
    'lenta': 'lenta.ru',
    'gazeta': 'gazeta.ru',
    'graininfo': 'graininfo.ru',
    'vedomosti': 'vedomosti.ru',
    'izvestia': 'iz.ru',
    'interfax': 'interfax.ru',
    'forbes': 'forbes.ru',
    'rg': 'rg.ru',
    'pnp': 'pnp.ru',
    'ria': 'ria.ru',
    'meduza': 'meduza.io',
    
    # Government and official spiders
    'government': 'government.ru',
    'kremlin': 'kremlin.ru',
    'regulation': 'regulation.gov.ru',
    
    # Legal document spiders
    'pravo': 'publication.pravo.gov.ru',
    'sozd': 'sozd.duma.gov.ru',
    'eaeu': 'docs.eaeunion.org',
}

# Helper to get spiders by site
def get_spider_names_by_sites(sites):
    reverse_map = {v: k for k, v in SPIDER_SITE_MAP.items()}
    return [reverse_map[site] for site in sites if site in reverse_map]

# Get project paths from configuration
SCRAPY_PROJECT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PYTHON_PATH = os.getenv('PYTHON_PATH', config.get('Scrapy', 'PYTHON_PATH', fallback='python'))

def get_spider_status():
    """Get status of all spiders from database"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT name, status, last_update FROM spider_status"))
            spiders = []
            for row in result:
                spider = {
                    'name': row[0],
                    'status': row[1],
                    'last_update': row[2].isoformat() if row[2] else None
                }
                spiders.append(spider)
            return spiders
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        return []

def update_spider_status(spiders, status):
    """Update status for specified spiders"""
    try:
        with engine.connect() as conn:
            if not spiders:  # Update all spiders
                conn.execute(text("UPDATE spider_status SET status = :status"), {"status": status})
            else:  # Update specific spiders
                conn.execute(
                    text("UPDATE spider_status SET status = :status WHERE name = ANY(:spiders)"),
                    {"status": status, "spiders": spiders}
                )
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        return False

def set_spider_status(name, status):
    with engine.connect() as conn:
        conn.execute(text('UPDATE spider_status SET status=:status, last_update=:last_update WHERE name=:name'), {
            'status': status,
            'last_update': datetime.utcnow(),
            'name': name
        })

def run_spider(spider_name):
    """Run a specific spider"""
    try:
        # Get the absolute path to the news_parser directory
        project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        
        # Run the spider
        result = subprocess.run(
            [PYTHON_PATH, '-m', 'scrapy', 'crawl', spider_name],
            cwd=project_path,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            return jsonify({'status': 'success', 'message': f'Spider {spider_name} started successfully'})
        else:
            logger.error(f"Error running spider {spider_name}: {result.stderr}")
            return jsonify({'status': 'error', 'message': result.stderr}), 500
            
    except Exception as e:
        logger.error(f"Error running spider {spider_name}: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/spiders', methods=['GET'])
def get_spiders():
    with engine.connect() as conn:
        result = conn.execute(text('SELECT name, status, last_update FROM spider_status ORDER BY name'))
        spiders = [
            {
                'name': row[0],
                'status': row[1],
                'last_update': row[2].isoformat() if row[2] else None
            }
            for row in result
        ]
    return jsonify(spiders)

@app.route('/api/spiders/<name>/start', methods=['POST'])
def start_spider(name):
    set_spider_status(name, 'scheduled')
    return jsonify({'message': f'Spider {name} scheduled.'})

@app.route('/api/spiders/<name>/stop', methods=['POST'])
def stop_spider(name):
    set_spider_status(name, 'disabled')
    return jsonify({'message': f'Spider {name} stopped.'})

@app.route('/api/spiders/<name>/status', methods=['POST'])
def update_spider_status_api(name):
    data = request.get_json()
    status = data.get('status')
    last_update = data.get('last_update')
    set_spider_status(name, status)
    return jsonify({'message': f'Status for {name} updated.'})

@app.route('/api/spiders/<name>/run', methods=['POST'])
def run_spider_api(name):
    try:
        set_spider_status(name, 'running')
        subprocess.Popen([
            PYTHON_PATH, '-m', 'scrapy', 'crawl', name
        ], cwd=SCRAPY_PROJECT_PATH)
        return jsonify({'success': True, 'message': f'{name} spider running successful'})
    except Exception as e:
        logger.error(f"Error running spider {name}: {str(e)}", exc_info=True)
        set_spider_status(name, 'error')
        return jsonify({'success': False, 'message': f'Error running spider {name}: {str(e)}'}), 500

@app.route('/api/articles', methods=['GET'])
def get_articles():
    # Get query parameters
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    source = request.args.get('source')
    days = request.args.get('days', 7, type=int)
    
    # Calculate date range
    date_from = datetime.utcnow() - timedelta(days=days)
    
    # Build query
    query = db.query(Article).filter(Article.published_at_iso >= date_from)
    
    # Apply source filter if provided
    if source:
        query = query.filter(Article.source == source)
    
    # Get total count
    total = query.count()
    
    # Apply pagination
    articles = query.order_by(desc(Article.published_at_iso))\
        .offset((page - 1) * per_page)\
        .limit(per_page)\
        .all()
    
    return jsonify({
        'articles': [article.to_dict() for article in articles],
        'total': total,
        'page': page,
        'per_page': per_page,
        'pages': (total + per_page - 1) // per_page
    })

@app.route('/api/articles/<article_id>', methods=['GET'])
def get_article(article_id):
    article = db.query(Article).filter(Article.id == article_id).first()
    if not article:
        return jsonify({'error': 'Article not found'}), 404
    return jsonify(article.to_dict())

@app.route('/api/sources', methods=['GET'])
def get_sources():
    sources = db.query(Article.source).distinct().all()
    return jsonify([source[0] for source in sources])

@app.route('/api/stats', methods=['GET'])
def get_stats():
    # Get total articles count
    total_articles = db.query(Article).count()
    
    # Get articles count by source
    sources = db.query(Article.source, db.func.count(Article.id))\
        .group_by(Article.source)\
        .all()
    
    # Get articles count by day for the last 7 days
    date_from = datetime.utcnow() - timedelta(days=7)
    daily_stats = db.query(
        db.func.date(Article.published_at_iso),
        db.func.count(Article.id)
    ).filter(Article.published_at_iso >= date_from)\
        .group_by(db.func.date(Article.published_at_iso))\
        .all()
    
    return jsonify({
        'total_articles': total_articles,
        'sources': {source: count for source, count in sources},
        'daily_stats': {str(date): count for date, count in daily_stats}
    })

def get_status_color(status):
    if status == 'running':
        return 'green'
    elif status == 'scheduled':
        return 'blue'
    elif status == 'disabled':
        return 'grey'
    elif status == 'error':
        return 'red'
    else:
        return 'grey'

@app.route('/')
def index():
    """Render the dashboard"""
    try:
        # Get spider statuses
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT name, status, last_update 
                FROM spider_status 
                ORDER BY name
            """))
            spiders = [dict(row) for row in result]
            
            # Format last_update and add color
            for spider in spiders:
                if spider['last_update']:
                    spider['last_update'] = spider['last_update'].strftime('%Y-%m-%d %H:%M:%S')
                else:
                    spider['last_update'] = 'Never'
                spider['color'] = get_status_color(spider['status'])
            
            return render_template('index.html', spiders=spiders)
    except Exception as e:
        logger.error(f"Error rendering dashboard: {str(e)}")
        return str(e), 500

@app.route('/dashboard', methods=['GET'])
def dashboard():
    with engine.connect() as conn:
        result = conn.execute(text('SELECT name, status, last_update FROM spider_status ORDER BY name'))
        spiders = [
            {
                'name': row[0],
                'status': row[1],
                'last_update': row[2].strftime('%d/%m/%Y %H:%M') if row[2] else None
            }
            for row in result
        ]
    return render_template('dashboard.html', spiders=spiders)

@app.route('/dashboard/run/<name>', methods=['POST'])
def dashboard_run_spider(name):
    # Trigger the spider run (sync call)
    try:
        # Immediately set status to 'running' for instant feedback
        set_spider_status(name, 'running')
        subprocess.Popen([
            PYTHON_PATH, '-m', 'scrapy', 'crawl', name
        ], cwd=SCRAPY_PROJECT_PATH)
        return redirect(url_for('dashboard'))
    except Exception as e:
        set_spider_status(name, 'error')
        return redirect(url_for('dashboard'))

@app.route('/dashboard/stop/<name>', methods=['POST'])
def dashboard_stop_spider(name):
    set_spider_status(name, 'disabled')
    return redirect(url_for('dashboard'))

@app.route('/dashboard/run_all', methods=['POST'])
def dashboard_run_all():
    with engine.connect() as conn:
        result = conn.execute(text('SELECT name FROM spider_status WHERE status=\'scheduled\''))
        names = [row[0] for row in result]
    for name in names:
        try:
            # Immediately set status to 'running' for instant feedback
            set_spider_status(name, 'running')
            subprocess.Popen([
                PYTHON_PATH, '-m', 'scrapy', 'crawl', name
            ], cwd=SCRAPY_PROJECT_PATH)
        except Exception as e:
            set_spider_status(name, 'error')
    return redirect(url_for('dashboard'))

@app.route('/dashboard/stop_all', methods=['POST'])
def dashboard_stop_all():
    set_spider_status(None, 'disabled')
    return redirect(url_for('dashboard'))

@app.route('/api/scraper/status', methods=['GET'])
def get_status():
    """Get status of all spiders"""
    spiders = get_spider_status()
    # Sort spiders by name
    spiders.sort(key=lambda x: x['name'])
    return jsonify({"success": True, "spiders": spiders})

@app.route('/api/scraper/start', methods=['POST'])
def start_spiders():
    data = request.get_json() or {}
    spiders = data.get('spiders', [])
    if update_spider_status(spiders, 'scheduled'):
        return jsonify({"success": True, "message": "Spiders scheduled successfully"})
    return jsonify({"success": False, "message": "Failed to schedule spiders"}), 500

@app.route('/api/scraper/stop', methods=['POST'])
def stop_spiders():
    data = request.get_json() or {}
    spiders = data.get('spiders', [])
    if update_spider_status(spiders, 'disabled'):
        return jsonify({"success": True, "message": "Spiders disabled successfully"})
    return jsonify({"success": False, "message": "Failed to disable spiders"}), 500

@app.route('/api/scraper/immediate', methods=['POST'])
def run_spiders_immediate():
    data = request.get_json() or {}
    spiders = data.get('spiders', [])
    if not spiders:
        spiders = [spider['name'] for spider in get_spider_status()]
    try:
        # Immediately set all spiders to 'running' status for instant feedback
        for name in spiders:
            set_spider_status(name, 'running')
        
        # Then start the spider processes
        for name in spiders:
            subprocess.Popen([
                PYTHON_PATH, '-m', 'scrapy', 'crawl', name
            ], cwd=SCRAPY_PROJECT_PATH)
        return jsonify({'success': True, 'message': 'All selected spiders running successfully'})
    except Exception as e:
        logger.error(f"Error running spiders: {str(e)}", exc_info=True)
        # If there's an error, set status back to 'error'
        for name in spiders:
            set_spider_status(name, 'error')
        return jsonify({'success': False, 'message': f'Error running spiders: {str(e)}'}), 500

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.before_request
def log_request_info():
    logger.info(f"Incoming request: {request.method} {request.path} - args: {dict(request.args)} - json: {request.get_json(silent=True)}")

@app.after_request
def log_response_info(response):
    logger.info(f"Outgoing response: {request.method} {request.path} - status: {response.status_code}")
    return response

if __name__ == '__main__':
    # Get web server configuration
    host = config.get('Web', 'HOST', fallback='0.0.0.0')
    port = int(config.get('Web', 'PORT', fallback='5001'))
    debug = config.getboolean('Web', 'DEBUG', fallback=True)
    
    # Start the Flask app
    app.run(host=host, port=port, debug=debug)

