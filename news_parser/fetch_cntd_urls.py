import requests
import xml.etree.ElementTree as ET
import logging
import os
import time
from datetime import datetime
from urllib.parse import urljoin
import configparser
from dotenv import load_dotenv

# Load environment variables and configuration
load_dotenv()

def load_config():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    
    if os.path.exists(config_path):
        config.read(config_path)
    else:
        # Default configuration if config.ini doesn't exist
        config['Database'] = {
            'DATABASE_URL': 'postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres'
        }
    
    return config

# Load configuration
config = load_config()

def setup_logging():
    """Setup logging configuration"""
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    
    # File handler
    log_file = os.path.join(log_dir, 'cntd_url_fetcher.log')
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

# Setup logging
logger = setup_logging()

def fetch_sitemap(url, use_proxy=True):
    """Fetch sitemap XML content"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    if use_proxy:
        # Proxy configuration
        proxies = {
            'http': 'http://googlecompute:xd23rXPEmq2+23@90.156.202.84:3128',
            'https': 'http://googlecompute:xd23rXPEmq2+23@90.156.202.84:3128'
        }
    else:
        proxies = None
    
    try:
        response = requests.get(url, headers=headers, proxies=proxies, timeout=30)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logger.error(f"Error fetching {url}: {e}")
        return None

def parse_sitemap_index(xml_content):
    """Parse the main sitemap index to get sub-sitemap URLs"""
    try:
        root = ET.fromstring(xml_content)
        
        # Define the namespace
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        # Find all URL elements
        urls = []
        for url_elem in root.findall('.//ns:url', namespace):
            loc_elem = url_elem.find('ns:loc', namespace)
            if loc_elem is not None:
                urls.append(loc_elem.text)
        
        logger.info(f"Found {len(urls)} sub-sitemap URLs")
        return urls
        
    except Exception as e:
        logger.error(f"Error parsing sitemap index: {e}")
        return []

def parse_document_sitemap(xml_content):
    """Parse individual document sitemap to get document URLs"""
    try:
        root = ET.fromstring(xml_content)
        
        # Define the namespace
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        # Find all URL elements
        urls = []
        for url_elem in root.findall('.//ns:url', namespace):
            loc_elem = url_elem.find('ns:loc', namespace)
            if loc_elem is not None:
                urls.append(loc_elem.text)
        
        return urls
        
    except Exception as e:
        logger.error(f"Error parsing document sitemap: {e}")
        return []

def save_urls_to_file(urls, filename):
    """Save URLs to a file"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            for url in urls:
                f.write(f'"{url}"\n')
        logger.info(f"Saved {len(urls)} URLs to {filename}")
    except Exception as e:
        logger.error(f"Error saving URLs to file: {e}")

def extract_sitemap_name_from_url(sitemap_url):
    """Extract sitemap name from URL for filename"""
    # Extract the filename part from the URL
    # e.g., "https://docs.cntd.ru/sitemap/docs/documents_100_sitemap.xml" -> "documents_100_sitemap"
    try:
        filename = sitemap_url.split('/')[-1]  # Get the last part
        # Remove .xml extension
        name_without_ext = filename.replace('.xml', '')
        return name_without_ext
    except Exception:
        return "unknown_sitemap"

def main():
    """Main function to fetch all CNTD document URLs"""
    try:
        # Main sitemap URL
        main_sitemap_url = "https://docs.cntd.ru/sitemap/docs/documents_sitemap.xml"
        
        logger.info("Starting CNTD URL fetching process...")
        logger.info(f"Fetching main sitemap from: {main_sitemap_url}")
        
        # Fetch main sitemap
        main_sitemap_content = fetch_sitemap(main_sitemap_url)
        if not main_sitemap_content:
            logger.error("Failed to fetch main sitemap")
            return
        
        # Parse main sitemap to get sub-sitemap URLs
        sub_sitemap_urls = parse_sitemap_index(main_sitemap_content)
        if not sub_sitemap_urls:
            logger.error("No sub-sitemap URLs found")
            return
        
        # Fetch and parse each sub-sitemap
        all_document_urls = []
        total_sub_sitemaps = len(sub_sitemap_urls)
        
        # Ensure the extra directory exists
        extra_dir = os.path.join(os.path.dirname(__file__), '..', 'extra')
        os.makedirs(extra_dir, exist_ok=True)
        
        for i, sub_sitemap_url in enumerate(sub_sitemap_urls, 1):
            logger.info(f"Processing sub-sitemap {i}/{total_sub_sitemaps}: {sub_sitemap_url}")
            
            # Fetch sub-sitemap
            sub_sitemap_content = fetch_sitemap(sub_sitemap_url)
            if not sub_sitemap_content:
                logger.warning(f"Failed to fetch sub-sitemap: {sub_sitemap_url}")
                continue
            
            # Parse sub-sitemap to get document URLs
            document_urls = parse_document_sitemap(sub_sitemap_content)
            all_document_urls.extend(document_urls)
            
            logger.info(f"Found {len(document_urls)} document URLs in sub-sitemap {i}")
            
            # Save individual sitemap URLs to separate file
            sitemap_name = extract_sitemap_name_from_url(sub_sitemap_url)
            timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
            individual_filename = f"{sitemap_name}_{timestamp}.txt"
            individual_path = os.path.join(extra_dir, individual_filename)
            
            save_urls_to_file(document_urls, individual_path)
            
            # Add a small delay to be respectful to the server
            time.sleep(1)
        
        # Also save combined file with all unique URLs
        unique_urls = list(set(all_document_urls))
        logger.info(f"Total unique document URLs found: {len(unique_urls)}")
        
        # Save combined URLs to file
        timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        combined_filename = f"cntd_urls_combined_{timestamp}.txt"
        combined_path = os.path.join(extra_dir, combined_filename)
        
        save_urls_to_file(unique_urls, combined_path)
        
        logger.info("CNTD URL fetching completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        main()
        logger.info("CNTD URL fetcher finished with exit code 0")
    except Exception as e:
        logger.error(f"CNTD URL fetcher failed with error: {e}", exc_info=True)
        exit(1) 