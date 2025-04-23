"""
Indexer Node for the distributed web crawling system.
Responsible for processing crawled content and building a searchable index.
"""
import json
import time
import boto3
import os
import sys
from collections import defaultdict
import threading

# Add the parent directory to the path so we can import common modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import INDEX_TASK_QUEUE

class SimpleIndex:
    """A simple in-memory inverted index."""
    def __init__(self):
        # Inverted index: term -> {url -> count}
        self.index = defaultdict(lambda: defaultdict(int))
        
        # Document metadata: url -> {title, text_length}
        self.documents = {}
        
        # Lock for thread safety
        self.lock = threading.Lock()
    
    def add_document(self, url, content):
        """Add a document to the index."""
        with self.lock:
            # Extract content
            title = content.get('title', '')
            text = content.get('text', '')
            
            # Store document metadata
            self.documents[url] = {
                'title': title,
                'text_length': len(text)
            }
            
            # Tokenize text (simple whitespace tokenization)
            terms = self._tokenize(text)
            
            # Update inverted index
            for term in terms:
                self.index[term][url] += 1
    
    def _tokenize(self, text):
        """Simple tokenization - split on whitespace and lowercase."""
        if not text:
            return []
        
        # Handle case where text is a list (from crawler output)
        if isinstance(text, list):
            # Join all text elements with spaces
            text = ' '.join([str(item) for item in text])
        
        # Split on whitespace, lowercase, and filter out empty strings
        return [word.lower() for word in text.split() if word.strip()]
    
    def search(self, query, max_results=10):
        """Search the index for documents matching the query."""
        with self.lock:
            query_terms = self._tokenize(query)
            
            # Calculate scores (simple term frequency)
            scores = defaultdict(int)
            for term in query_terms:
                if term in self.index:
                    for url, count in self.index[term].items():
                        scores[url] += count
            
            # Sort by score
            results = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:max_results]
            
            # Format results
            formatted_results = []
            for url, score in results:
                if url in self.documents:
                    formatted_results.append({
                        'url': url,
                        'title': self.documents[url]['title'],
                        'score': score
                    })
            
            return formatted_results
    
    def save_to_disk(self, index_file='index.json', documents_file='documents.json'):
        """Save the index to disk."""
        with self.lock:
            # Create data directory if it doesn't exist
            os.makedirs('data', exist_ok=True)
            
            # Save index
            with open(os.path.join('data', index_file), 'w') as f:
                # Convert defaultdict to regular dict for serialization
                index_dict = {k: dict(v) for k, v in self.index.items()}
                json.dump(index_dict, f)
            
            # Save documents
            with open(os.path.join('data', documents_file), 'w') as f:
                json.dump(self.documents, f)
    
    def load_from_disk(self, index_file='index.json', documents_file='documents.json'):
        """Load the index from disk."""
        try:
            # Load index
            index_path = os.path.join('data', index_file)
            if os.path.exists(index_path):
                with open(index_path, 'r') as f:
                    index_dict = json.load(f)
                    with self.lock:
                        # Convert back to defaultdict
                        for term, urls in index_dict.items():
                            for url, count in urls.items():
                                self.index[term][url] = count
            
            # Load documents
            documents_path = os.path.join('data', documents_file)
            if os.path.exists(documents_path):
                with open(documents_path, 'r') as f:
                    with self.lock:
                        self.documents = json.load(f)
            
            return True
        except Exception as e:
            print(f"Error loading index: {e}")
            return False


class IndexerNode:
    """
    Indexer Node class that builds and maintains the search index.
    """
    def __init__(self):
        self.sqs = boto3.client('sqs')
        
        # Get queue URL
        response = self.sqs.get_queue_url(QueueName=INDEX_TASK_QUEUE)
        self.index_task_queue_url = response['QueueUrl']
        
        # Initialize the index
        self.index = SimpleIndex()
        
        # Try to load existing index
        self.index.load_from_disk()
        
        # Flag to control the indexer
        self.running = False
        
        # Counter for periodic saves
        self.processed_count = 0
        self.save_interval = 10  # Save after every 10 documents
    
    def start(self):
        """Start the indexer node."""
        print("Starting indexer node")
        self.running = True
        
        # Main indexing loop
        while self.running:
            try:
                # Get a task from the queue
                task = self._get_task()
                
                if task:
                    # Process the task
                    self._process_task(task)
                    
                    # Increment counter and save periodically
                    self.processed_count += 1
                    if self.processed_count % self.save_interval == 0:
                        self.index.save_to_disk()
                        print(f"Index saved after processing {self.processed_count} documents")
                else:
                    # No tasks available, wait a bit
                    time.sleep(1)
            except Exception as e:
                print(f"Error in indexer main loop: {e}")
                time.sleep(1)
    
    def stop(self):
        """Stop the indexer node."""
        print("Stopping indexer node")
        self.running = False
        
        # Save the index before stopping
        self.index.save_to_disk()
    
    def _get_task(self):
        """Get a task from the index task queue."""
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.index_task_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=5
            )
            
            if 'Messages' in response and response['Messages']:
                message = response['Messages'][0]
                task = json.loads(message['Body'])
                
                # Delete the message from the queue
                self.sqs.delete_message(
                    QueueUrl=self.index_task_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                
                print(f"Received index task for URL: {task['url']}")
                return task
            
            return None
        except Exception as e:
            print(f"Error getting index task: {e}")
            return None
    
    def _process_task(self, task):
        """Process an index task."""
        url = task['url']
        content = task['content']
        
        print(f"Indexing URL: {url}")
        
        # Add the document to the index
        self.index.add_document(url, content)
    
    def search(self, query, max_results=10):
        """Search the index."""
        return self.index.search(query, max_results)


if __name__ == "__main__":
    # Create and start an indexer node
    indexer = IndexerNode()
    try:
        indexer.start()
    except KeyboardInterrupt:
        indexer.stop()