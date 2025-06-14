import os
import sys

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from web.app import app

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001) 