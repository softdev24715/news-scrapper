from sqlalchemy import create_engine, Column, String, JSON, Text, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class Article(Base):
    __tablename__ = 'articles'

    id = Column(String, primary_key=True)  # UUID from scraper
    text = Column(Text, nullable=False)
    source = Column(String, nullable=False)
    published_at = Column(Integer, nullable=False)
    published_at_iso = Column(DateTime, nullable=False)
    url = Column(String, nullable=False, unique=True)
    header = Column(String, nullable=False)
    parsed_at = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        """Convert to dictionary format compatible with existing code"""
        return {
            'id': self.id,
            'text': self.text,
            'metadata': {
                'source': self.source,
                'published_at': self.published_at,
                'published_at_iso': self.published_at_iso,
                'url': self.url,
                'header': self.header,
                'parsed_at': self.parsed_at
            }
        }

class LegalDocument(Base):
    __tablename__ = 'legal_documents'

    id = Column(String, primary_key=True)  # UUID from scraper
    text = Column(Text, nullable=False)
    original_id = Column(String)
    doc_kind = Column(String)
    title = Column(Text)
    source = Column(String, nullable=False)
    url = Column(String, nullable=False, unique=True)
    published_at = Column(Integer)
    parsed_at = Column(Integer)
    jurisdiction = Column(String)
    language = Column(String)
    stage = Column(Text)
    discussion_period = Column(JSON)
    explanatory_note = Column(JSON)
    summary_reports = Column(JSON)
    comment_stats = Column(JSON)
    files = Column(JSON)  # Store files array as JSON
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        """Convert to dictionary format compatible with existing code"""
        return {
            'id': self.id,
            'text': self.text,
            'lawMetadata': {
                'originalId': self.original_id,
                'docKind': self.doc_kind,
                'title': self.title,
                'source': self.source,
                'url': self.url,
                'publishedAt': self.published_at,
                'parsedAt': self.parsed_at,
                'jurisdiction': self.jurisdiction,
                'language': self.language,
                'stage': self.stage,
                'discussionPeriod': self.discussion_period,
                'explanatoryNote': self.explanatory_note,
                'summaryReports': self.summary_reports,
                'commentStats': self.comment_stats,
                'files': self.files
            }
        }

def init_db(db_url):
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session() 