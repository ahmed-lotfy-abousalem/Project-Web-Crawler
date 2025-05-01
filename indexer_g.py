import os
import sqlite3
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
from whoosh.analysis import StemmingAnalyzer
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('indexer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('Indexer')

# Define schema with stored fields
schema = Schema(
    url=ID(stored=True, unique=True),
    title=TEXT(stored=True),
    content=TEXT(analyzer=StemmingAnalyzer())
)

# Ensure index directory exists and initialize or open index
INDEX_DIR = "data/indexdir"
if not os.path.exists(INDEX_DIR):
    os.makedirs(INDEX_DIR)
    ix = create_in(INDEX_DIR, schema)
else:
    ix = open_dir(INDEX_DIR)

def index_document(url: str, title: str, content: str):
    """
    Indexes or updates a document in the search index.
    """
    writer = ix.writer()
    writer.update_document(url=url, title=title, content=content)
    writer.commit()

def index_from_db():
    """
    Indexes all documents from SQLite database.
    """
    conn = sqlite3.connect('data/crawler_data.db')
    c = conn.cursor()
    c.execute("SELECT url, text FROM crawled_data")
    for url, text in c.fetchall():
        title = url  # Placeholder; extract title from text if available
        index_document(url, title, text)
        logger.info(f"Indexed {url}")
    conn.close()

def search_index(query_string: str, limit: int = 10):
    parser = QueryParser("content", ix.schema)
    query = parser.parse(query_string)
    results_list = []
    with ix.searcher() as searcher:
        results = searcher.search(query, limit=limit)
        for r in results:
            results_list.append((r['url'], r['title'], r.score))
    return results_list

if __name__ == "__main__":
    index_from_db()