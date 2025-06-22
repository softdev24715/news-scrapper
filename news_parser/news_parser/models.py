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

class LegalDocument(Base):
    __tablename__ = 'legal_documents'

    id = Column(String, primary_key=True)  # UUID from scraper
    text = Column(Text, nullable=False)
    original_id = Column(String)  # Original document ID from source
    doc_kind = Column(String)  # Type of document (bill, act, etc.)
    title = Column(Text)  # Document title
    source = Column(String, nullable=False)  # Source website
    url = Column(String, unique=True, nullable=False)  # Document URL
    published_at = Column(Integer)  # Unix timestamp
    parsed_at = Column(Integer)  # Unix timestamp
    jurisdiction = Column(String)  # Legal jurisdiction (RU, EAEU, etc.)
    language = Column(String)  # Document language
    stage = Column(Text)  # Document stage/status
    discussion_period = Column(JSON)  # Discussion period info
    explanatory_note = Column(JSON)  # Explanatory note info
    summary_reports = Column(JSON)  # Summary reports
    comment_stats = Column(JSON)  # Comment statistics
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'text': self.text,
            'original_id': self.original_id,
            'doc_kind': self.doc_kind,
            'title': self.title,
            'source': self.source,
            'url': self.url,
            'published_at': self.published_at,
            'parsed_at': self.parsed_at,
            'jurisdiction': self.jurisdiction,
            'language': self.language,
            'stage': self.stage,
            'discussion_period': self.discussion_period,
            'explanatory_note': self.explanatory_note,
            'summary_reports': self.summary_reports,
            'comment_stats': self.comment_stats,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }

def init_db(db_url):
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session() 