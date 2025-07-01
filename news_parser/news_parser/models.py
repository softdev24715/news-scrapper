from sqlalchemy import create_engine, Column, String, JSON, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Article(Base):
    __tablename__ = 'articles'

    id = Column(String, primary_key=True)  # UUID from scraper
    text = Column(Text, nullable=False)
    article_metadata = Column(JSON, nullable=False)  # All metadata as JSON (renamed from 'metadata')

    def to_dict(self):
        return {
            'id': self.id,
            'text': self.text,
            'metadata': self.article_metadata  # Keep original name in dict output
        }

class LegalDocument(Base):
    __tablename__ = 'legal_documents'

    id = Column(String, primary_key=True)  # UUID from scraper
    text = Column(Text, nullable=False)
    law_metadata = Column(JSON, nullable=False)  # All law metadata as JSON

    def to_dict(self):
        return {
            'id': self.id,
            'text': self.text,
            'lawMetadata': self.law_metadata
        }

def init_db(db_url):
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session() 