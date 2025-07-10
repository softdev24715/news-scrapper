#!/usr/bin/env python3
"""
Test script for Selenium-based regulation.gov.ru scraping
This script demonstrates how to extract data from the modal that appears when clicking the information button.
"""

import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import re
import json

def setup_driver():
    """Setup Chrome WebDriver with headless options and proxy"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--allow-running-insecure-content")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-images")
    
    # Configure proxy from environment or settings
    proxy_url = os.environ.get('HTTP_PROXY') or os.environ.get('HTTPS_PROXY')
    
    if proxy_url:
        # Parse proxy URL to extract host, port, username, password
        from urllib.parse import urlparse
        parsed = urlparse(proxy_url)
        
        # Set proxy with authentication
        proxy_string = f"{parsed.hostname}:{parsed.port}"
        chrome_options.add_argument(f'--proxy-server={proxy_string}')
        
        # Set proxy authentication
        if parsed.username and parsed.password:
            chrome_options.add_argument(f'--proxy-auth={parsed.username}:{parsed.password}')
        
        print(f"Configured Selenium with proxy: {parsed.hostname}:{parsed.port}")
    else:
        print("No proxy configured for Selenium")
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        print("Selenium WebDriver initialized successfully")
        return driver
    except Exception as e:
        print(f"Failed to initialize WebDriver: {e}")
        return None

def extract_modal_data(modal):
    """Extract data from the modal content"""
    try:
        modal_html = modal.get_attribute('innerHTML')
        soup = BeautifulSoup(modal_html, 'html.parser')
        
        data = {}
        
        # Extract discussion period
        discussion_text = soup.get_text()
        discussion_match = re.search(r'Период обсуждения[:\s]*(\d{2}\.\d{2}\.\d{4})\s*-\s*(\d{2}\.\d{2}\.\d{4})', discussion_text)
        if discussion_match:
            data['discussion_start'] = discussion_match.group(1)
            data['discussion_end'] = discussion_match.group(2)
            print(f"Found discussion period: {data['discussion_start']} - {data['discussion_end']}")
        
        # Extract comment count
        comment_match = re.search(r'Количество комментариев[:\s]*(\d+)', discussion_text)
        if comment_match:
            data['comment_count'] = int(comment_match.group(1))
            print(f"Found comment count: {data['comment_count']}")
        
        # Extract file information
        files = []
        file_links = soup.find_all('a', href=True)
        for link in file_links:
            href = link.get('href')
            if href and ('download' in href or '.pdf' in href or '.doc' in href):
                files.append({
                    'url': href,
                    'title': link.get_text().strip(),
                    'type': get_file_type(href)
                })
        data['files'] = files
        print(f"Found {len(files)} files in modal")
        
        return data
        
    except Exception as e:
        print(f"Error extracting modal data: {e}")
        return {}

def extract_page_content(driver):
    """Extract additional data from the main page content"""
    if driver is None:
        return {}
        
    try:
        page_html = driver.page_source
        soup = BeautifulSoup(page_html, 'html.parser')
        
        data = {}
        
        # Extract explanatory note information
        explanatory_links = soup.find_all('a', href=True)
        for link in explanatory_links:
            href = link.get('href')
            text = link.get_text().strip().lower()
            if 'пояснительная' in text or 'explanatory' in text:
                data['explanatory_url'] = href
                data['explanatory_mime_type'] = get_mime_type(href)
                print(f"Found explanatory note: {href}")
                break
        
        # Extract summary reports
        summary_reports = []
        for link in explanatory_links:
            href = link.get('href')
            text = link.get_text().strip().lower()
            if 'итоговый' in text or 'summary' in text:
                summary_reports.append({
                    'url': href,
                    'title': link.get_text().strip(),
                    'type': get_file_type(href)
                })
        data['summary_reports'] = summary_reports
        print(f"Found {len(summary_reports)} summary reports")
        
        return data
        
    except Exception as e:
        print(f"Error extracting page content: {e}")
        return {}

def get_file_type(url):
    """Determine file type from URL"""
    if '.pdf' in url:
        return 'pdf'
    elif '.doc' in url or '.docx' in url:
        return 'doc'
    elif '.xls' in url or '.xlsx' in url:
        return 'excel'
    else:
        return 'unknown'

def get_mime_type(url):
    """Get MIME type from file extension"""
    if '.pdf' in url:
        return 'application/pdf'
    elif '.doc' in url:
        return 'application/msword'
    elif '.docx' in url:
        return 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    elif '.xls' in url:
        return 'application/vnd.ms-excel'
    elif '.xlsx' in url:
        return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    else:
        return 'application/octet-stream'

def extract_page_data(driver, url):
    """Extract additional data from the regulation page using Selenium"""
    if driver is None:
        print("WebDriver is not initialized")
        return {}
        
    try:
        print(f"Visiting page: {url}")
        driver.get(url)
        
        # Wait for page to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        additional_data = {}
        
        # Look for the information button and click it to open modal
        try:
            # Find the button with onclick containing 'npaPreview.show'
            info_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//a[contains(@onclick, 'npaPreview.show')]"))
            )
            
            print("Found information button, clicking to open modal")
            info_button.click()
            
            # Wait a moment for the modal to start appearing
            time.sleep(2)
            
            # Wait for modal to appear
            modal = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "modal-content"))
            )
            
            # Extract data from modal
            modal_data = extract_modal_data(modal)
            additional_data.update(modal_data)
            
            # Close modal if there's a close button
            try:
                close_button = modal.find_element(By.CLASS_NAME, "close")
                close_button.click()
            except NoSuchElementException:
                pass
                
        except TimeoutException:
            print("Information button not found or modal didn't appear")
        
        # Extract other data from the page
        page_data = extract_page_content(driver)
        additional_data.update(page_data)
        
        return additional_data
        
    except Exception as e:
        print(f"Error extracting data from {url}: {e}")
        return {}

def main():
    """Main function to test Selenium scraping"""
    # Test URL from regulation.gov.ru
    test_url = "http://regulation.gov.ru/projects#npa=158168"
    
    driver = setup_driver()
    if driver:
        try:
            print("Testing Selenium scraping for regulation.gov.ru")
            print(f"Test URL: {test_url}")
            
            # Extract data from the page
            data = extract_page_data(driver, test_url)
            
            print("\nExtracted data:")
            print(json.dumps(data, indent=2, ensure_ascii=False))
            
        finally:
            driver.quit()
            print("WebDriver closed")
    else:
        print("Could not initialize WebDriver. Check your Chrome installation and proxy settings.")

if __name__ == "__main__":
    main() 