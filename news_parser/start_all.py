import subprocess
import sys
import os
from threading import Thread
import time
import logging
import signal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the port
PORT = 5001

def kill_existing_process(port):
    """Kill any process running on the specified port"""
    try:
        # Get the PID of the process using the port
        pid = subprocess.check_output(['lsof', '-t', f':{port}']).decode().strip()
        if pid:
            logger.info(f"Killing existing process {pid} on port {port}")
            os.kill(int(pid), signal.SIGTERM)
            time.sleep(1)  # Give it time to die
    except subprocess.CalledProcessError:
        # No process found on the port
        pass
    except Exception as e:
        logger.error(f"Error killing process: {e}")

def run_web_server():
    """Run the Flask web server using Gunicorn"""
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    logger.info(f"Starting web server on port {PORT}")
    subprocess.run([
        'gunicorn',
        '--bind', f'0.0.0.0:{PORT}',
        '--workers', '4',
        '--timeout', '120',
        'web.app:app'
    ])

def run_scheduler():
    """Run the scheduler"""
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    logger.info("Starting scheduler")
    subprocess.run([sys.executable, 'scheduler.py'])

if __name__ == '__main__':
    logger.info("Starting News Parser services...")
    logger.info(f"Web server will be available at http://localhost:{PORT}")
    
    # Kill any existing process on port 5000
    kill_existing_process(5000)
    
    # Start web server in a separate thread
    web_thread = Thread(target=run_web_server)
    web_thread.daemon = True
    web_thread.start()
    
    # Give the web server a moment to start
    time.sleep(2)
    
    logger.info("Web server started. Starting scheduler...")
    
    # Run scheduler in main thread
    run_scheduler() 