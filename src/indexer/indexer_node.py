"""
Indexer Node for the distributed web crawling system.
Responsible for processing crawled content and building a searchable index.
"""
import json
import time
import boto3
import os
import sys
import logging
import traceback
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Indexer] %(message)s',
    handlers=[
        logging.FileHandler("indexer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("indexer")

class WhooshIndex:
    """A more robust index using Whoosh library."""
    def __init__(self, index_dir='whoosh_index'):
        # Create index directory if it doesn't exist
        self.index_dir = index_dir
        os.makedirs(self.index_dir, exist_ok=True)
        logger.info(f"Index directory initialized: {self.index_dir}")
        
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
        logger.debug("Schema defined with fields: url, title, description, content, keywords, domain, crawl_time")
        
        # Create or open the index
        if not os.listdir(self.index_dir):
            logger.info("Creating new index in empty directory")
            self.ix = create_in(self.index_dir, self.schema)
        else:
            logger.info("Opening existing index")
            self.ix = open_dir(self.index_dir)
        
        # Lock for thread safety
        self.lock = threading.Lock()
        logger.debug("Index initialization complete")
    
    def add_document(self, url, content):
        """Add a document to the index."""
        logger.debug(f"Adding document to index: {url}")
        with self.lock:
            # Extract content
            title = self._extract_field(content, 'title')
            description = self._extract_field(content, 'description')
            text = self._extract_field(content, 'text')
            keywords = self._extract_field(content, 'keywords')
            
            # Extract domain from URL
            domain = url.split('/')[2] if '://' in url else url.split('/')[0]
            logger.debug(f"Extracted domain: {domain}")
            
            # Add document to index
            writer = self.ix.writer()
            try:
                logger.debug(f"Writing document to index: {url}")
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
                logger.info(f"Successfully indexed document: {url}")
                return True
            except Exception as e:
                writer.cancel()
                logger.error(f"Error indexing document {url}: {e}")
                logger.error(traceback.format_exc())
                return False
    
    def _extract_field(self, content, field_name):
        """Extract and normalize a field from content."""
        value = content.get(field_name, '')
        
        # Handle case where field is a list (from crawler output)
        if isinstance(value, list):
            value = ' '.join([str(item) for item in value if item])
            logger.debug(f"Converted list field '{field_name}' to string, length: {len(value)}")
        
        return value or ''
    
    def search(self, query_string, max_results=10):
        """Search the index with more advanced query capabilities."""
        logger.info(f"Searching index for: '{query_string}' (max results: {max_results})")
        with self.lock:
            with self.ix.searcher() as searcher:
                # Create a parser that searches multiple fields
                parser = MultifieldParser(["title", "content", "description", "keywords"], 
                                         schema=self.ix.schema)
                
                # Parse the query
                query = parser.parse(query_string)
                logger.debug(f"Parsed query: {query}")
                
                # Search
                results = searcher.search(query, limit=max_results)
                logger.info(f"Search returned {len(results)} results")
                
                # Format results
                formatted_results = []
                for hit in results:
                    formatted_results.append({
                        'url': hit['url'],
                        'title': hit['title'],
                        'description': hit.get('description', ''),
                        'score': hit.score
                    })
                    logger.debug(f"Result: {hit['url']} (score: {hit.score})")
                
                return formatted_results
    
    def save_to_s3(self, s3_client, bucket_name):
        """Save the index to S3."""
        logger.info(f"Saving index to S3 bucket: {bucket_name}")
        with self.lock:
            try:
                # Check if bucket exists, create if it doesn't
                try:
                    logger.debug(f"Checking if bucket exists: {bucket_name}")
                    s3_client.head_bucket(Bucket=bucket_name)
                    logger.debug(f"Bucket {bucket_name} exists")
                except s3_client.exceptions.ClientError as e:
                    # If the bucket doesn't exist, create it
                    if e.response['Error']['Code'] == '404':
                        logger.warning(f"Bucket {bucket_name} doesn't exist, creating it...")
                        s3_client.create_bucket(Bucket=bucket_name)
                        logger.info(f"Created bucket: {bucket_name}")
                    else:
                        # If it's a permissions issue or other error, raise it
                        logger.error(f"Error checking S3 bucket: {e}")
                        logger.error(traceback.format_exc())
                        return False

                # Create a temporary zip file of the index
                timestamp = datetime.now(timezone.utc).isoformat().replace(':', '-')
                zip_filename = f"index-{timestamp}.zip"
                logger.debug(f"Creating archive: {zip_filename}")
                shutil.make_archive(zip_filename.replace('.zip', ''), 'zip', self.index_dir)
                logger.debug(f"Archive created: {zip_filename}")
                
                # Upload to S3
                s3_key = f"index/{zip_filename}"
                logger.info(f"Uploading index to S3: {s3_key}")
                with open(zip_filename, 'rb') as f:
                    s3_client.upload_fileobj(f, bucket_name, s3_key)
                
                # Clean up the temporary file
                logger.debug(f"Removing temporary file: {zip_filename}")
                os.remove(zip_filename)
                
                logger.info(f"Index successfully saved to S3: {s3_key}")
                return s3_key

            except Exception as e:
                logger.error(f"Error saving index to S3: {e}")
                logger.error(traceback.format_exc())
                return None
    
    def load_from_s3(self, s3_client, bucket_name, s3_key):
        """Load the index from S3."""
        logger.info(f"Loading index from S3: {bucket_name}/{s3_key}")
        with self.lock:
            try:
                # Download the zip file
                local_zip = os.path.basename(s3_key)
                logger.debug(f"Downloading file to: {local_zip}")
                s3_client.download_file(bucket_name, s3_key, local_zip)
                logger.debug(f"Download complete: {local_zip}")
                
                # Clear existing index directory
                logger.debug(f"Clearing existing index directory: {self.index_dir}")
                shutil.rmtree(self.index_dir, ignore_errors=True)
                os.makedirs(self.index_dir, exist_ok=True)
                
                # Extract the zip file
                logger.debug(f"Extracting archive to: {self.index_dir}")
                shutil.unpack_archive(local_zip, self.index_dir, 'zip')
                
                # Remove the zip file
                logger.debug(f"Removing temporary file: {local_zip}")
                os.remove(local_zip)
                
                # Reopen the index
                logger.debug("Reopening index")
                self.ix = open_dir(self.index_dir)
                
                logger.info(f"Index successfully loaded from S3: {s3_key}")
                return True
            except Exception as e:
                logger.error(f"Error loading index from S3: {e}")
                logger.error(traceback.format_exc())
                return False

# Update the IndexerNode class to use WhooshIndex
class IndexerNode:
    """
    Indexer Node class that builds and maintains the search index.
    """
    def __init__(self):
        self.indexer_id = f"indexer-{uuid.uuid4()}"
        logger.info(f"Initializing indexer node with ID: {self.indexer_id}")
        
        # Initialize AWS clients
        logger.debug("Initializing AWS clients")
        self.sqs = boto3.client('sqs', region_name='us-east-1')
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        
        # Get queue URL
        try:
            logger.debug(f"Getting queue URL for: {INDEX_TASK_QUEUE}")
            response = self.sqs.get_queue_url(QueueName=INDEX_TASK_QUEUE)
            self.index_task_queue_url = response['QueueUrl']
            logger.info(f"Queue URL: {self.index_task_queue_url}")
        except Exception as e:
            logger.error(f"Error getting queue URL: {e}")
            logger.error(traceback.format_exc())
            raise
        
        # Initialize the index
        logger.info("Initializing Whoosh index")
        self.index = WhooshIndex()
        
        # Try to load latest index from S3
        logger.info("Attempting to load latest index from S3")
        self.latest_index_key = self._get_latest_index_key()
        if self.latest_index_key:
            logger.info(f"Found latest index key: {self.latest_index_key}")
            self.index.load_from_s3(self.s3, INDEX_DATA_BUCKET, self.latest_index_key)
        else:
            logger.info("No existing index found, starting with empty index")
        
        # Flag to control the indexer
        self.running = False
        
        # Counter for periodic saves
        self.processed_count = 0
        self.document_count = 0  # Add this line to initialize document_count
        self.save_interval = 10  # Save after every 10 documents
        logger.info(f"Save interval set to: {self.save_interval} documents")
        
        # Create or get DynamoDB table for index metadata
        logger.info("Ensuring DynamoDB tables exist")
        self._ensure_dynamodb_table()
        
        # Heartbeat thread for fault tolerance
        logger.debug("Setting up heartbeat thread")
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeats)
        self.heartbeat_thread.daemon = True
        
        logger.info("Indexer node initialization complete")
    
    def _ensure_dynamodb_table(self):
        """Ensure the DynamoDB tables for index metadata and indexer status exist."""
        logger.debug("Checking and creating DynamoDB tables if needed")
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
                    logger.debug(f"Checking if table exists: {table_name}")
                    table = self.dynamodb.Table(table_name)
                    table.table_status  # This will raise an exception if the table doesn't exist
                    logger.info(f"Table {table_name} already exists")
                    
                    # Store reference to index-metadata table
                    if table_name == 'index-metadata':
                        self.index_metadata_table = table
                    
                except Exception:
                    # Create the table if it doesn't exist
                    try:
                        logger.info(f"Creating DynamoDB table: {table_name}")
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
                        logger.debug(f"Waiting for table to be created: {table_name}")
                        table.wait_until_exists()
                        logger.info(f"Table {table_name} created successfully")
                        
                        # Store reference to index-metadata table
                        if table_name == 'index-metadata':
                            self.index_metadata_table = table
                            
                    except Exception as e:
                        logger.error(f"Error creating DynamoDB table {table_name}: {e}")
                        logger.error(traceback.format_exc())
                        
        except Exception as e:
            logger.error(f"Error ensuring DynamoDB tables: {e}")
            logger.error(traceback.format_exc())
    
    def _get_latest_index_key(self):
        """Get the latest index key from DynamoDB."""
        logger.debug("Retrieving latest index key from DynamoDB")
        try:
            logger.debug(f"Scanning index-metadata table for indexer_id: {self.indexer_id}")
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
                logger.debug(f"Found {len(response['Items'])} items, sorting by timestamp")
                items = sorted(response['Items'], 
                              key=lambda x: x.get('timestamp', ''), 
                              reverse=True)
                if items:
                    latest_metadata = items[0]
                    logger.info(f"Latest index metadata found, timestamp: {latest_metadata.get('timestamp')}")
                    return latest_metadata.get('index_key')
            
            logger.info("No index metadata found")
            return None
        except Exception as e:
            logger.error(f"Error getting latest index key: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def _save_index_metadata(self, index_key):
        """Save index metadata to DynamoDB."""
        logger.debug(f"Saving index metadata for key: {index_key}")
        try:
            timestamp = datetime.now(timezone.utc).isoformat()
            logger.debug(f"Putting item in index-metadata table with timestamp: {timestamp}")
            self.index_metadata_table.put_item(
                Item={
                    'indexer_id': self.indexer_id,
                    'timestamp': timestamp,
                    'index_key': index_key,
                    'status': 'active'
                }
            )
            logger.info(f"Index metadata saved to DynamoDB: {timestamp}")
            return True
        except Exception as e:
            logger.error(f"Error saving index metadata to DynamoDB: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def _send_heartbeats(self):
        """Send periodic heartbeats to indicate indexer is alive."""
        logger.info("Starting heartbeat thread")
        while self.running:
            try:
                self._send_heartbeat()
                time.sleep(30)  # Send heartbeat every 30 seconds
            except Exception as e:
                logger.error(f"Error sending heartbeat: {e}")
                logger.error(traceback.format_exc())
                time.sleep(5)  # Shorter retry interval on error
    
    def _send_heartbeat(self):
        """Send a heartbeat to DynamoDB."""
        logger.debug("Sending heartbeat")
        try:
            timestamp = datetime.now(timezone.utc).isoformat()
            self.dynamodb.Table('indexer-status').put_item(
                Item={
                    'indexer_id': self.indexer_id,
                    'timestamp': timestamp,
                    'status': 'active',
                    'processed_count': self.processed_count
                }
            )
            logger.debug(f"Heartbeat sent at {timestamp}, processed count: {self.processed_count}")
        except Exception as e:
            logger.error(f"Error sending heartbeat: {e}")
            logger.error(traceback.format_exc())
    
    def start(self):
        """Start the indexer node."""
        logger.info("Starting indexer node")
        self.running = True
        
        # Start heartbeat thread
        logger.debug("Starting heartbeat thread")
        self.heartbeat_thread.start()
        
        # Main indexing loop
        logger.info("Entering main indexing loop")
        while self.running:
            try:
                # Get a task from the queue
                logger.debug("Attempting to get task from queue")
                task = self._get_task()
                
                if task:
                    # Process the task
                    logger.info(f"Processing task for URL: {task.get('url', 'unknown')}")
                    self._process_task(task)
                    
                    # Increment counter and save periodically
                    self.processed_count += 1
                    logger.debug(f"Processed count: {self.processed_count}")
                    if self.processed_count % self.save_interval == 0:
                        # Save to S3
                        logger.info(f"Save interval reached ({self.save_interval}), saving index to S3")
                        index_key = self.index.save_to_s3(self.s3, INDEX_DATA_BUCKET)
                        if index_key:
                            self._save_index_metadata(index_key)
                        logger.info(f"Index saved after processing {self.processed_count} documents")
                else:
                    # No tasks available, wait a bit
                    logger.debug("No tasks available, waiting")
                    time.sleep(1)
            except Exception as e:
                logger.error(f"Error in indexer main loop: {e}")
                logger.error(traceback.format_exc())
                time.sleep(1)
    
    def stop(self):
        """Stop the indexer node."""
        logger.info("Stopping indexer node")
        self.running = False
        
        # Save the index before stopping
        logger.info("Saving index before stopping")
        index_key = self.index.save_to_s3(self.s3, INDEX_DATA_BUCKET)
        if index_key:
            self._save_index_metadata(index_key)
            logger.info("Final index saved successfully")
        else:
            logger.warning("Failed to save final index")
    
    def _get_task(self):
        """Get a task from the index task queue."""
        try:
            logger.debug(f"Receiving message from queue: {self.index_task_queue_url}")
            response = self.sqs.receive_message(
                QueueUrl=self.index_task_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=5
            )
            
            if 'Messages' in response and response['Messages']:
                message = response['Messages'][0]
                logger.debug(f"Message received, ID: {message.get('MessageId')}")
                
                try:
                    task = json.loads(message['Body'])
                    logger.debug(f"Message parsed successfully")
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing message body: {e}")
                    logger.error(f"Message body: {message['Body']}")
                    
                    # Delete the malformed message
                    logger.warning("Deleting malformed message from queue")
                    self.sqs.delete_message(
                        QueueUrl=self.index_task_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    return None
                
                # Validate task structure
                if 'url' not in task or 'content' not in task:
                    logger.error(f"Received malformed task without required fields: {task}")
                    # Delete the invalid message from the queue
                    logger.warning("Deleting malformed task from queue")
                    self.sqs.delete_message(
                        QueueUrl=self.index_task_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    return None
                
                # Delete the message from the queue
                logger.debug(f"Deleting message from queue: {message['MessageId']}")
                self.sqs.delete_message(
                    QueueUrl=self.index_task_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                
                logger.info(f"Received index task for URL: {task['url']}")
                return task
            
            logger.debug("No messages in queue")
            return None
        except Exception as e:
            logger.error(f"Error getting index task: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def _process_task(self, task):
        """Process an index task."""
        try:
            url = task['url']
            content = task['content']
            
            logger.info(f"Indexing URL: {url}")
            logger.debug(f"Content keys: {list(content.keys())}")
            
            # Add the document to the index
            start_time = time.time()
            success = self.index.add_document(url, content)
            end_time = time.time()
            
            logger.info(f"Indexing completed in {end_time - start_time:.2f} seconds, success: {success}")
            
            # Store indexing metadata in DynamoDB
            try:
                logger.debug("Storing indexing metadata in DynamoDB")
                self.dynamodb.Table('indexed-documents').put_item(
                    Item={
                        'url': url,
                        'title': content.get('title', ''),
                        'indexed_at': datetime.now(timezone.utc).isoformat(),
                        'indexer_id': self.indexer_id,
                        'success': success,
                        's3_raw_path': task.get('s3_raw_path', ''),
                        'processing_time': f"{end_time - start_time:.2f}"
                    }
                )
                logger.debug("Indexing metadata stored successfully")
            except Exception as e:
                logger.error(f"Error storing index metadata in DynamoDB: {e}")
                logger.error(traceback.format_exc())
        except KeyError as e:
            logger.error(f"Missing required field in task: {e}")
            logger.error(f"Task structure: {task.keys()}")
        except Exception as e:
            logger.error(f"Error processing task: {e}")
            logger.error(traceback.format_exc())
    
    def search(self, query, max_results=10):
        """Search the index."""
        logger.info(f"Search request received: '{query}'")
        results = self.index.search(query, max_results)
        logger.info(f"Search returned {len(results)} results")
        return results
        
    def process_document(self, document):
        """Process a document and add it to the index.
        
        Args:
            document (dict): Document to process with fields like url, title, etc.
            
        Returns:
            bool: True if document was successfully processed, False otherwise
        """
        logger.info(f"Processing document: {document.get('url', 'unknown')}")
        try:
            # Extract the URL
            url = document.get('url')
            if not url:
                logger.error("Document missing required URL field")
                return False
                
            # Add the document to the index
            success = self.index.add_document(url, document)
            
            if success:
                logger.info(f"Successfully processed document: {url}")
            else:
                logger.warning(f"Failed to add document to index: {url}")
                
            return success
        except Exception as e:
            logger.error(f"Error processing document: {e}")
            logger.error(traceback.format_exc())
            return False
            
    def _record_indexed_document(self, document):
        """Record indexed document in DynamoDB for tracking purposes."""
        logger.debug(f"Recording indexed document: {document.get('url', 'unknown')}")
        try:
            # Get the URL and timestamp
            url = document.get('url', '')
            timestamp = datetime.now(timezone.utc).isoformat()
            
            # Create or get the indexed-documents table
            table = self.dynamodb.Table('indexed-documents')
            
            # Store the document metadata
            table.put_item(
                Item={
                    'url': url,
                    'indexer_id': self.indexer_id,
                    'indexed_at': timestamp,
                    'title': document.get('title', ''),
                    'description': document.get('description', '')[:1000] if document.get('description') else '',
                    'status': 'indexed'
                }
            )
            logger.info(f"Successfully recorded indexed document: {url}")
            return True
        except Exception as e:
            logger.error(f"Error recording indexed document: {e}")
            logger.error(traceback.format_exc())
            return False
    def add_to_index(self, document):
        """
        Add a document to the search index.
        
        Args:
            document (dict): Document to be indexed with fields like url, title, description, content
            
        Returns:
            bool: True if indexing was successful, False otherwise
        """
        try:
            # Extract fields from the document
            url = document.get('url', '')
            title = document.get('title', '')
            description = document.get('description', '')
            content = document.get('content', '')
            
            # Use the index's add_document method instead of directly manipulating the writer
            success = self.index.add_document(url, {
                'title': title,
                'description': description,
                'text': content  # Note: WhooshIndex expects 'text' field, not 'content'
            })
            
            if success:
                # Update the document count
                self.document_count += 1
                
                # Check if we need to save the index to S3
                if self.document_count % self.save_interval == 0:
                    self._save_index_to_s3()
                    
                # Record the indexed document in DynamoDB
                self._record_indexed_document(document)
                
                logger.info(f"Successfully indexed document: {url}")
                return True
                
            else:
             logger.error(f"Failed to index document: {url}")
            return False
                
        except Exception as e:
            logger.error(f"Error adding document to index: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def search_index(self, query_string, max_results=10):
        """
        Search the index for documents matching the query.
        
        Args:
            query_string (str): The search query
            max_results (int): Maximum number of results to return
            
        Returns:
            list: List of matching documents
        """
        logger.info(f"Searching index for query: '{query_string}' (max results: {max_results})")
       
        try:
        # Use the index's search method instead of directly accessing the searcher
          results = self.index.search(query_string, max_results)
          logger.info(f"Found {len(results)} matching documents")
          return results
        except Exception as e:
          logger.error(f"Error searching index: {e}")
          logger.error(traceback.format_exc())
          return []

if __name__ == "__main__":
    # Create and start an indexer node
    logger.info("Creating indexer node")
    indexer = IndexerNode()
    try:
        indexer.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, stopping indexer")
        indexer.stop()
    except Exception as e:
        logger.critical(f"Unhandled exception in indexer: {e}")
        logger.critical(traceback.format_exc())
        indexer.stop()
