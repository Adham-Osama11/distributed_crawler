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
import uuid
from datetime import datetime, timezone
import shutil
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID, STORED
from whoosh.qparser import QueryParser, MultifieldParser
from whoosh.analysis import StemmingAnalyzer

# Add the parent directory to the path so we can import common modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import INDEX_TASK_QUEUE, INDEX_DATA_BUCKET

class WhooshIndex:
    """A more robust index using Whoosh library."""
    def __init__(self, index_dir='whoosh_index'):
        # Create index directory if it doesn't exist
        self.index_dir = index_dir
        os.makedirs(self.index_dir, exist_ok=True)
        
        # Define schema with stemming analyzer for better text processing
        self.schema = Schema(
            url=ID(stored=True, unique=True),
            title=TEXT(stored=True),
            description=TEXT(stored=True),
            content=TEXT(analyzer=StemmingAnalyzer(), stored=True),
            keywords=TEXT(stored=True),
            domain=STORED,
            crawl_time=STORED
        )
        
        # Create or open the index
        if not os.listdir(self.index_dir):
            self.ix = create_in(self.index_dir, self.schema)
        else:
            self.ix = open_dir(self.index_dir)
        
        # Lock for thread safety
        self.lock = threading.Lock()
    
    def add_document(self, url, content):
        """Add a document to the index."""
        with self.lock:
            # Extract content
            title = self._extract_field(content, 'title')
            description = self._extract_field(content, 'description')
            text = self._extract_field(content, 'text')
            keywords = self._extract_field(content, 'keywords')
            
            # Extract domain from URL
            domain = url.split('/')[2] if '://' in url else url.split('/')[0]
            
            # Add document to index
            writer = self.ix.writer()
            try:
                writer.update_document(
                    url=url,
                    title=title,
                    description=description,
                    content=text,
                    keywords=keywords,
                    domain=domain,
                    crawl_time=datetime.now(timezone.utc).isoformat()
                )
                writer.commit()
                print(f"Indexed document: {url}")
                return True
            except Exception as e:
                writer.cancel()
                print(f"Error indexing document {url}: {e}")
                return False
    
    def _extract_field(self, content, field_name):
        """Extract and normalize a field from content."""
        value = content.get(field_name, '')
        
        # Handle case where field is a list (from crawler output)
        if isinstance(value, list):
            value = ' '.join([str(item) for item in value if item])
        
        return value or ''
    
    def search(self, query_string, max_results=10):
        """Search the index with more advanced query capabilities."""
        with self.lock:
            with self.ix.searcher() as searcher:
                # Create a parser that searches multiple fields
                parser = MultifieldParser(["title", "content", "description", "keywords"], 
                                         schema=self.ix.schema)
                
                # Parse the query
                query = parser.parse(query_string)
                
                # Search
                results = searcher.search(query, limit=max_results)
                
                # Format results
                formatted_results = []
                for hit in results:
                    formatted_results.append({
                        'url': hit['url'],
                        'title': hit['title'],
                        'description': hit.get('description', ''),
                        'score': hit.score
                    })
                
                return formatted_results
    
    def save_to_s3(self, s3_client, bucket_name):
        """Save the index to S3."""
        with self.lock:
            try:
                # Create a temporary zip file of the index
                timestamp = datetime.now(timezone.utc).isoformat().replace(':', '-')
                zip_filename = f"index-{timestamp}.zip"
                shutil.make_archive(zip_filename.replace('.zip', ''), 'zip', self.index_dir)
                
                # Upload to S3
                s3_key = f"index/{zip_filename}"
                with open(zip_filename, 'rb') as f:
                    s3_client.upload_fileobj(f, bucket_name, s3_key)
                
                # Clean up the temporary file
                os.remove(zip_filename)
                
                print(f"Index saved to S3: {s3_key}")
                return s3_key
            except Exception as e:
                print(f"Error saving index to S3: {e}")
                return None
    
    def load_from_s3(self, s3_client, bucket_name, s3_key):
        """Load the index from S3."""
        with self.lock:
            try:
                # Download the zip file
                local_zip = os.path.basename(s3_key)
                s3_client.download_file(bucket_name, s3_key, local_zip)
                
                # Clear existing index directory
                shutil.rmtree(self.index_dir, ignore_errors=True)
                os.makedirs(self.index_dir, exist_ok=True)
                
                # Extract the zip file
                shutil.unpack_archive(local_zip, self.index_dir, 'zip')
                
                # Remove the zip file
                os.remove(local_zip)
                
                # Reopen the index
                self.ix = open_dir(self.index_dir)
                
                print(f"Index loaded from S3: {s3_key}")
                return True
            except Exception as e:
                print(f"Error loading index from S3: {e}")
                return False

# Update the IndexerNode class to use WhooshIndex
class IndexerNode:
    """
    Indexer Node class that builds and maintains the search index.
    """
    def __init__(self):
        self.indexer_id = f"indexer-{uuid.uuid4()}"
        
        # Initialize AWS clients
        self.sqs = boto3.client('sqs', region_name='us-east-1')
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        
        # Get queue URL
        response = self.sqs.get_queue_url(QueueName=INDEX_TASK_QUEUE)
        self.index_task_queue_url = response['QueueUrl']
        
        # Initialize the index
        self.index = WhooshIndex()
        
        # Try to load latest index from S3
        self.latest_index_key = self._get_latest_index_key()
        if self.latest_index_key:
            self.index.load_from_s3(self.s3, INDEX_DATA_BUCKET, self.latest_index_key)
        
        # Flag to control the indexer
        self.running = False
        
        # Counter for periodic saves
        self.processed_count = 0
        self.save_interval = 10  # Save after every 10 documents
        
        # Create or get DynamoDB table for index metadata
        self._ensure_dynamodb_table()
        
        # Heartbeat thread for fault tolerance
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeats)
        self.heartbeat_thread.daemon = True
    
    def _ensure_dynamodb_table(self):
        """Ensure the DynamoDB tables for index metadata and indexer status exist."""
        try:
            # Define tables to create if they don't exist
            tables_to_create = {
                'index-metadata': {
                    'KeySchema': [
                        {'AttributeName': 'indexer_id', 'KeyType': 'HASH'},
                        {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                    ],
                    'AttributeDefinitions': [
                        {'AttributeName': 'indexer_id', 'AttributeType': 'S'},
                        {'AttributeName': 'timestamp', 'AttributeType': 'S'}
                    ]
                },
                'indexer-status': {
                    'KeySchema': [
                        {'AttributeName': 'indexer_id', 'KeyType': 'HASH'}
                    ],
                    'AttributeDefinitions': [
                        {'AttributeName': 'indexer_id', 'AttributeType': 'S'}
                    ]
                },
                'indexed-documents': {
                    'KeySchema': [
                        {'AttributeName': 'url', 'KeyType': 'HASH'}
                    ],
                    'AttributeDefinitions': [
                        {'AttributeName': 'url', 'AttributeType': 'S'}
                    ]
                }
            }
            
            # Check if tables exist and create them if they don't
            for table_name, schema in tables_to_create.items():
                try:
                    # Try to access the table
                    table = self.dynamodb.Table(table_name)
                    table.table_status  # This will raise an exception if the table doesn't exist
                    print(f"Table {table_name} already exists")
                    
                    # Store reference to index-metadata table
                    if table_name == 'index-metadata':
                        self.index_metadata_table = table
                    
                except Exception:
                    # Create the table if it doesn't exist
                    try:
                        print(f"Creating DynamoDB table: {table_name}")
                        table = self.dynamodb.create_table(
                            TableName=table_name,
                            KeySchema=schema['KeySchema'],
                            AttributeDefinitions=schema['AttributeDefinitions'],
                            ProvisionedThroughput={
                                'ReadCapacityUnits': 5,
                                'WriteCapacityUnits': 5
                            }
                        )
                        # Wait for the table to be created
                        table.wait_until_exists()
                        print(f"Table {table_name} created successfully")
                        
                        # Store reference to index-metadata table
                        if table_name == 'index-metadata':
                            self.index_metadata_table = table
                            
                    except Exception as e:
                        print(f"Error creating DynamoDB table {table_name}: {e}")
                        
        except Exception as e:
            print(f"Error ensuring DynamoDB tables: {e}")
    
    def _get_latest_index_key(self):
        """Get the latest index key from DynamoDB."""
        try:
            response = self.dynamodb.Table('index-metadata').scan(
                FilterExpression="indexer_id = :indexer_id",
                ExpressionAttributeValues={
                    ":indexer_id": self.indexer_id
                },
                Limit=1
                # Removed ScanIndexForward parameter as it's not valid for scan operations
            )
            
            if response.get('Items'):
                # Since we can't sort with scan, we'll sort the results ourselves
                items = sorted(response['Items'], 
                              key=lambda x: x.get('timestamp', ''), 
                              reverse=True)
                if items:
                    latest_metadata = items[0]
                    return latest_metadata.get('index_key')
            
            return None
        except Exception as e:
            print(f"Error getting latest index key: {e}")
            return None
    
    def _save_index_metadata(self, index_key):
        """Save index metadata to DynamoDB."""
        try:
            timestamp = datetime.now(timezone.utc).isoformat()
            self.index_metadata_table.put_item(
                Item={
                    'indexer_id': self.indexer_id,
                    'timestamp': timestamp,
                    'index_key': index_key,
                    'status': 'active'
                }
            )
            print(f"Index metadata saved to DynamoDB: {timestamp}")
            return True
        except Exception as e:
            print(f"Error saving index metadata to DynamoDB: {e}")
            return False
    
    def _send_heartbeats(self):
        """Send periodic heartbeats to indicate indexer is alive."""
        while self.running:
            try:
                self._send_heartbeat()
                time.sleep(30)  # Send heartbeat every 30 seconds
            except Exception as e:
                print(f"Error sending heartbeat: {e}")
    
    def _send_heartbeat(self):
        """Send a heartbeat to DynamoDB."""
        try:
            self.dynamodb.Table('indexer-status').put_item(
                Item={
                    'indexer_id': self.indexer_id,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'status': 'active',
                    'processed_count': self.processed_count
                }
            )
        except Exception as e:
            print(f"Error sending heartbeat: {e}")
    
    def start(self):
        """Start the indexer node."""
        print("Starting indexer node")
        self.running = True
        
        # Start heartbeat thread
        self.heartbeat_thread.start()
        
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
                        # Save to S3
                        index_key = self.index.save_to_s3(self.s3, INDEX_DATA_BUCKET)
                        if index_key:
                            self._save_index_metadata(index_key)
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
        index_key = self.index.save_to_s3(self.s3, INDEX_DATA_BUCKET)
        if index_key:
            self._save_index_metadata(index_key)
    
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
        success = self.index.add_document(url, content)
        
        # Store indexing metadata in DynamoDB
        try:
            self.dynamodb.Table('indexed-documents').put_item(
                Item={
                    'url': url,
                    'title': content.get('title', ''),
                    'indexed_at': datetime.now(timezone.utc).isoformat(),
                    'indexer_id': self.indexer_id,
                    'success': success,
                    's3_raw_path': task.get('s3_raw_path', '')
                }
            )
        except Exception as e:
            print(f"Error storing index metadata in DynamoDB: {e}")
    
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