from mpi4py import MPI
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
import logging
import os
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - Indexer - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('indexer.log'),
        logging.StreamHandler()
    ]
)

# Schema for the search index
schema = Schema(
    url=ID(stored=True, unique=True),
    content=TEXT(stored=True),
    title=TEXT(stored=True)
)

def initialize_index(index_dir="indexdir"):
    """Create or open the search index"""
    if not os.path.exists(index_dir):
        os.mkdir(index_dir)
        logging.info(f"Created new index at {index_dir}")
        return create_in(index_dir, schema)
    return open_dir(index_dir)

def index_page(ix, url, title, content):
    """Index a single webpage"""
    writer = ix.writer()
    writer.add_document(url=url, title=title, content=content)
    writer.commit()
    logging.info(f"Indexed: {url}")

def search_index(ix, query, limit=5):
    """Search the index and return results"""
    results = []
    with ix.searcher() as searcher:
        query_parser = QueryParser("content", ix.schema)
        parsed_query = query_parser.parse(query)
        hits = searcher.search(parsed_query, limit=limit)
        
        for hit in hits:
            results.append({
                'url': hit['url'],
                'title': hit['title'],
                'snippet': hit.highlights('content') or hit['content'][:200]
            })
    return results

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    ix = initialize_index()

    logging.info(f"Indexer {rank} ready. Waiting for tasks...")
    
    while True:
        status = MPI.Status()
        if comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            msg = comm.recv(source=status.Get_source(), tag=status.Get_tag(), status=status)
            
            # Handle incoming tasks
            if status.Get_tag() == 2:  # Crawler sending page data
                url, title, content = msg
                index_page(ix, url, title, content)
                comm.send("ACK", dest=0, tag=5)  # Notify master
                
            elif status.Get_tag() == 3:  # Search request
                results = search_index(ix, msg)
                comm.send(results, dest=status.Get_source(), tag=4)
                
            elif status.Get_tag() == 99:  # Heartbeat check
                comm.send("ALIVE", dest=0, tag=99)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.critical(f"Indexer crashed: {e}", exc_info=True)
        MPI.COMM_WORLD.Abort(1)