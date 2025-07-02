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
        # Try multiple methods to find and kill the process
        methods = [
            # Method 1: lsof
            ['lsof', '-t', f':{port}'],
            # Method 2: netstat
            ['netstat', '-tlnp', '2>/dev/null', '|', 'grep', f':{port}', '|', 'awk', '{print $7}', '|', 'cut', '-d/', '-f1'],
            # Method 3: ss
            ['ss', '-tlnp', '|', 'grep', f':{port}', '|', 'awk', '{print $6}', '|', 'cut', '-d/', '-f1']
        ]
        
        for method in methods:
            try:
                if len(method) == 3:  # Simple command
                    pid = subprocess.check_output(method).decode().strip()
                else:  # Complex command with pipes
                    cmd = ' '.join(method)
                    pid = subprocess.check_output(cmd, shell=True).decode().strip()
                
                if pid and pid.isdigit():
                    logger.info(f"Found process {pid} on port {port}, killing it...")
                    os.kill(int(pid), signal.SIGTERM)
                    time.sleep(2)  # Give it more time to die
                    
                    # Try SIGKILL if SIGTERM didn't work
                    try:
                        os.kill(int(pid), signal.SIGKILL)
                        logger.info(f"Force killed process {pid}")
                    except ProcessLookupError:
                        pass  # Process already dead
                    
                    break
            except (subprocess.CalledProcessError, ValueError, ProcessLookupError):
                continue
                
    except Exception as e:
        logger.error(f"Error killing process: {e}")
    
    # Additional cleanup: kill any gunicorn processes
    try:
        subprocess.run(['pkill', '-f', 'gunicorn'], check=False)
        logger.info("Killed any existing gunicorn processes")
        time.sleep(1)
    except Exception as e:
        logger.error(f"Error killing gunicorn processes: {e}")

def run_web_server():
    """Run the Flask web server using Gunicorn"""
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # Double-check that port is free
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(('0.0.0.0', PORT))
        sock.close()
        logger.info(f"Port {PORT} is free, starting web server...")
    except OSError:
        logger.error(f"Port {PORT} is still in use after cleanup attempts")
        return
    
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
    
    # Kill any existing process on port 5001
    kill_existing_process(PORT)
    
    # Wait a bit more for cleanup
    time.sleep(3)
    
    # Start web server in a separate thread
    web_thread = Thread(target=run_web_server)
    web_thread.daemon = True
    web_thread.start()
    
    # Give the web server more time to start
    time.sleep(8)
    
    # Check if web server started successfully
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        result = sock.connect_ex(('localhost', PORT))
        sock.close()
        if result == 0:
            logger.info("Web server started successfully. Starting scheduler...")
        else:
            logger.error("Web server failed to start properly")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Error checking web server status: {e}")
        sys.exit(1)
    
    # Run scheduler in main thread
    run_scheduler() 