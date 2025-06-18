from news_parser.models import init_db
import logging
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_db_connection():
    try:
        # Try to connect to the database
        session = init_db("postgresql://postgres:1e3Xdfsdf23@90.156.204.42:5432/postgres")
        logger.info("Successfully connected to the database!")
        
        # Try to execute a simple query
        result = session.execute(text("SELECT 1"))
        logger.info("Successfully executed test query!")
        
        return True
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        return False

if __name__ == "__main__":
    test_db_connection() 