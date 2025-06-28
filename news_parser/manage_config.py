#!/usr/bin/env python3
"""
Configuration management script for the news scraper
Allows viewing and modifying config.ini settings
"""

import os
import sys
import configparser
from datetime import datetime

def load_config():
    """Load configuration from config.ini"""
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    
    if os.path.exists(config_path):
        config.read(config_path)
    else:
        print("❌ config.ini not found. Creating default configuration...")
        create_default_config()
        config.read(config_path)
    
    return config

def create_default_config():
    """Create default config.ini file"""
    config = configparser.ConfigParser()
    
    config['Database'] = {
        'DATABASE_URL': 'postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres'
    }
    
    config['Scrapy'] = {
        'SCRAPY_PROJECT_PATH': 'news_parser',
        'PYTHON_PATH': 'python'
    }
    
    config['Scheduler'] = {
        'HOUR': '22',
        'MINUTE': '0',
        'LOG_LEVEL': 'INFO'
    }
    
    config['Web'] = {
        'HOST': '0.0.0.0',
        'PORT': '5001',
        'DEBUG': 'True'
    }
    
    config['Spider'] = {
        'MAX_CONCURRENT': '0',
        'TIMEOUT': '3600',
        'RETRY_FAILED': 'True',
        'MAX_RETRIES': '3'
    }
    
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    with open(config_path, 'w') as configfile:
        config.write(configfile)
    
    print("✅ Default config.ini created successfully!")

def show_config():
    """Display current configuration"""
    config = load_config()
    
    print("📋 Current Configuration:")
    print("=" * 50)
    
    for section in config.sections():
        print(f"\n🔧 [{section}]")
        for key, value in config[section].items():
            print(f"   {key} = {value}")

def update_scheduler_time():
    """Update scheduler time interactively"""
    config = load_config()
    
    print("🕐 Current scheduler time:", end=" ")
    hour = config.get('Scheduler', 'HOUR', fallback='22')
    minute = config.get('Scheduler', 'MINUTE', fallback='0')
    print(f"{hour}:{minute.zfill(2)}")
    
    try:
        new_hour = input("Enter new hour (0-23, or press Enter to keep current): ").strip()
        if new_hour:
            new_hour = int(new_hour)
            if 0 <= new_hour <= 23:
                config.set('Scheduler', 'HOUR', str(new_hour))
            else:
                print("❌ Hour must be between 0 and 23")
                return
        
        new_minute = input("Enter new minute (0-59, or press Enter to keep current): ").strip()
        if new_minute:
            new_minute = int(new_minute)
            if 0 <= new_minute <= 59:
                config.set('Scheduler', 'MINUTE', str(new_minute))
            else:
                print("❌ Minute must be between 0 and 59")
                return
        
        # Save configuration
        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        with open(config_path, 'w') as configfile:
            config.write(configfile)
        
        print("✅ Scheduler time updated successfully!")
        print(f"🕐 New time: {config.get('Scheduler', 'HOUR')}:{config.get('Scheduler', 'MINUTE').zfill(2)}")
        
    except ValueError:
        print("❌ Invalid input. Please enter numbers only.")
    except KeyboardInterrupt:
        print("\n❌ Operation cancelled.")

def show_help():
    """Show help information"""
    print("""
🔧 News Scraper Configuration Manager

Usage: python manage_config.py [command]

Commands:
  show     - Display current configuration
  time     - Update scheduler time interactively
  help     - Show this help message

Examples:
  python manage_config.py show
  python manage_config.py time

Configuration file: config.ini
""")

def main():
    if len(sys.argv) < 2:
        show_help()
        return
    
    command = sys.argv[1].lower()
    
    if command == 'show':
        show_config()
    elif command == 'time':
        update_scheduler_time()
    elif command == 'help':
        show_help()
    else:
        print(f"❌ Unknown command: {command}")
        show_help()

if __name__ == "__main__":
    main() 