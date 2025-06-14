from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class Article(Base):
    __tablename__ = 'articles'

    id = Column(String, primary_key=True)  # UUID from scraper
    text = Column(Text, nullable=False)
    source = Column(String, nullable=False)
    url = Column(String, unique=True, nullable=False)
    header = Column(String, nullable=False)
    published_at = Column(Integer, nullable=False)  # Unix timestamp
    published_at_iso = Column(DateTime, nullable=False)
    parsed_at = Column(Integer, nullable=False)
    author = Column(String)
    categories = Column(JSON)  # Store as JSON array
    images = Column(JSON)  # Store as JSON array
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'text': self.text,
            'source': self.source,
            'url': self.url,
            'header': self.header,
            'published_at': self.published_at,
            'published_at_iso': self.published_at_iso.isoformat(),
            'parsed_at': self.parsed_at,
            'author': self.author,
            'categories': self.categories,
            'images': self.images,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }

# Database connection setup
def init_db(db_url):
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session() 