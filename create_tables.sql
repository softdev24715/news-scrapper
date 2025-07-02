-- Database setup script for news scraper
-- Creates tables for both news articles and legal documents
-- Tables match EXACTLY the data structures produced by spiders

-- Create spider_status table for managing spider states
CREATE TABLE IF NOT EXISTS spider_status (
    name VARCHAR PRIMARY KEY,
    status VARCHAR DEFAULT 'enabled',
    last_update TIMESTAMP(6)
);

-- Create articles table for news spiders (metadata structure)
-- Matches exactly: id, text, article_metadata (renamed from metadata due to SQLAlchemy reserved word)
CREATE TABLE IF NOT EXISTS articles (
    id VARCHAR PRIMARY KEY,
    text TEXT NOT NULL,
    article_metadata JSONB NOT NULL
);

-- Create legal_documents table for legal spiders (lawMetadata structure)
-- Matches exactly: id, text, lawMetadata
CREATE TABLE IF NOT EXISTS legal_documents (
    id VARCHAR PRIMARY KEY,
    text TEXT NOT NULL,
    law_metadata JSONB NOT NULL
);

-- Add unique constraints on source + url combination to prevent duplicates
-- For articles table (with proper error handling)
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'articles_source_url_unique'
    ) THEN
        ALTER TABLE articles ADD CONSTRAINT articles_source_url_unique 
            UNIQUE ((article_metadata->>'source'), (article_metadata->>'url'));
    END IF;
END $$;

-- For legal_documents table (with proper error handling)
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'legal_documents_source_url_unique'
    ) THEN
        ALTER TABLE legal_documents ADD CONSTRAINT legal_documents_source_url_unique 
            UNIQUE ((law_metadata->>'source'), (law_metadata->>'url'));
    END IF;
END $$;

-- Add all spiders to spider_status table
INSERT INTO spider_status (name, status) VALUES 
    -- News spiders (13)
    ('tass', 'scheduled'),
    ('rbc', 'enabled'),
    ('vedomosti', 'enabled'),
    ('pnp', 'enabled'),
    ('lenta', 'enabled'),
    ('graininfo', 'enabled'),
    ('forbes', 'enabled'),
    ('interfax', 'enabled'),
    ('izvestia', 'enabled'),
    ('gazeta', 'enabled'),
    ('rg', 'enabled'),
    ('kommersant', 'enabled'),
    ('ria', 'enabled'),
    ('meduza', 'enabled'),
    
    -- Government and official spiders (3)
    ('government', 'enabled'),
    ('kremlin', 'enabled'),
    ('regulation', 'enabled'),
    
    -- Legal document spiders (3)
    ('pravo', 'enabled'),
    ('sozd', 'enabled'),
    ('eaeu', 'enabled')
ON CONFLICT (name) DO UPDATE SET status = 'enabled';

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_articles_id ON articles(id);
CREATE INDEX IF NOT EXISTS idx_articles_metadata_source ON articles USING GIN ((article_metadata->>'source'));
CREATE INDEX IF NOT EXISTS idx_articles_metadata_url ON articles USING GIN ((article_metadata->>'url'));
CREATE INDEX IF NOT EXISTS idx_articles_metadata_published_at ON articles USING GIN ((article_metadata->>'published_at'));

CREATE INDEX IF NOT EXISTS idx_legal_documents_id ON legal_documents(id);
CREATE INDEX IF NOT EXISTS idx_legal_documents_law_metadata_source ON legal_documents USING GIN ((law_metadata->>'source'));
CREATE INDEX IF NOT EXISTS idx_legal_documents_law_metadata_url ON legal_documents USING GIN ((law_metadata->>'url'));
CREATE INDEX IF NOT EXISTS idx_legal_documents_law_metadata_published_at ON legal_documents USING GIN ((law_metadata->>'publishedAt'));
CREATE INDEX IF NOT EXISTS idx_legal_documents_law_metadata_jurisdiction ON legal_documents USING GIN ((law_metadata->>'jurisdiction'));

-- Show table information
SELECT 'Tables created successfully' as status;
SELECT COUNT(*) as total_spiders FROM spider_status;

-- Show table structure
SELECT 
    'articles' as table_name,
    'id (VARCHAR PRIMARY KEY)' as column_info
UNION ALL
SELECT 
    'articles',
    'text (TEXT NOT NULL)'
UNION ALL
SELECT 
    'articles',
    'article_metadata (JSONB NOT NULL)'
UNION ALL
SELECT 
    'legal_documents',
    'id (VARCHAR PRIMARY KEY)'
UNION ALL
SELECT 
    'legal_documents',
    'text (TEXT NOT NULL)'
UNION ALL
SELECT 
    'legal_documents',
    'law_metadata (JSONB NOT NULL)'; 