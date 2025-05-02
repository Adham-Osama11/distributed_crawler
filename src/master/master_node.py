"""
Master Node for the distributed web crawling system.
Responsible for task distribution and worker management.
"""
import time
import json
import uuid
import boto3
from collections import defaultdict
import threading
import sys
import os
from datetime import datetime, timezone

# Add the parent directory to the path so we can import common modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import (
    CRAWL_TASK_QUEUE, CRAWL_RESULT_QUEUE, 
    INDEX_TASK_QUEUE, HEARTBEAT_INTERVAL,
    CRAWL_DATA_BUCKET, AWS_REGION
)
from common.utils import get_domain, normalize_url

class MasterNode:
    def __init__(self):
        # Initialize AWS clients
        self.sqs = boto3.client('sqs', region_name=AWS_REGION)
        self.s3 = boto3.client('s3', region_name=AWS_REGION)
        self.dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        
        # Create queues if they don't exist
        self._create_queues()
        
        # Ensure DynamoDB tables exist
        self._ensure_dynamodb_tables()
        
        # Track active crawler nodes
        self.active_crawlers = {}  # crawler_id -> last_heartbeat_time
        
        # Track URLs being processed
        self.urls_in_progress = set()
        self.urls_completed = set()
        
        # Track domains being crawled to enforce politeness
        self.domain_url_counts = defaultdict(int)
        
        # Lock for thread safety
        self.lock = threading.Lock()
        
        # Start heartbeat monitoring thread
        self.heartbeat_thread = threading.Thread(target=self._monitor_heartbeats)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
    
    def _create_queues(self):
        """Get URLs for existing SQS queues."""
        try:
            # Get queue URLs for existing queues
            response = self.sqs.get_queue_url(QueueName=CRAWL_TASK_QUEUE)
            self.crawl_task_queue_url = response['QueueUrl']
            
            response = self.sqs.get_queue_url(QueueName=CRAWL_RESULT_QUEUE)
            self.crawl_result_queue_url = response['QueueUrl']
            
            response = self.sqs.get_queue_url(QueueName=INDEX_TASK_QUEUE)
            self.index_task_queue_url = response['QueueUrl']
            
            print("Successfully connected to existing SQS queues")
        except Exception as e:
            print(f"Error getting SQS queue URLs: {e}")
            raise
    
    def _ensure_dynamodb_tables(self):
        """Ensure required DynamoDB tables exist."""
        try:
            # Define tables to create if they don't exist
            tables_to_create = {
                'url-frontier': {
                    'KeySchema': [
                        {'AttributeName': 'url', 'KeyType': 'HASH'},
                        {'AttributeName': 'job_id', 'KeyType': 'RANGE'}  # Add job_id as a sort key
                    ],
                    'AttributeDefinitions': [
                        {'AttributeName': 'url', 'AttributeType': 'S'},
                        {'AttributeName': 'job_id', 'AttributeType': 'S'}  # Define job_id attribute
                    ]
                },
                'crawl-metadata': {
                    'KeySchema': [
                        {'AttributeName': 'url', 'KeyType': 'HASH'}
                    ],
                    'AttributeDefinitions': [
                        {'AttributeName': 'url', 'AttributeType': 'S'}
                    ]
                },
                'crawler-status': {
                    'KeySchema': [
                        {'AttributeName': 'crawler_id', 'KeyType': 'HASH'}
                    ],
                    'AttributeDefinitions': [
                        {'AttributeName': 'crawler_id', 'AttributeType': 'S'}
                    ]
                },
                'master-status': {
                    'KeySchema': [
                        {'AttributeName': 'master_id', 'KeyType': 'HASH'},
                        {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                    ],
                    'AttributeDefinitions': [
                        {'AttributeName': 'master_id', 'AttributeType': 'S'},
                        {'AttributeName': 'timestamp', 'AttributeType': 'S'}
                    ]
                }
            }
            
            # Get existing tables
            existing_tables = self.dynamodb.meta.client.list_tables()['TableNames']
            
            # Create tables that don't exist
            for table_name, schema in tables_to_create.items():
                if table_name not in existing_tables:
                    print(f"Creating DynamoDB table: {table_name}")
                    self.dynamodb.create_table(
                        TableName=table_name,
                        KeySchema=schema['KeySchema'],
                        AttributeDefinitions=schema['AttributeDefinitions'],
                        ProvisionedThroughput={
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    )
                    # Wait for table to be created
                    table = self.dynamodb.Table(table_name)
                    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
                    print(f"Table {table_name} created successfully")
            
            print("DynamoDB tables verified")
        except Exception as e:
            print(f"Error ensuring DynamoDB tables: {e}")
    
    def start_crawl(self, seed_urls, max_depth=3, max_urls_per_domain=100):
        """Start a new crawl with the given seed URLs."""
        print(f"Starting crawl with {len(seed_urls)} seed URLs")
        
        # Generate a crawl ID
        crawl_id = str(uuid.uuid4())
        
        # Record crawl start in DynamoDB
        self._record_crawl_start(crawl_id, seed_urls, max_depth, max_urls_per_domain)
        
        # Normalize seed URLs
        normalized_urls = [normalize_url(url) for url in seed_urls]
        
        # Create crawl tasks for seed URLs
        for url in normalized_urls:
            self._enqueue_url(url, depth=0, max_depth=max_depth, 
                             max_urls_per_domain=max_urls_per_domain, crawl_id=crawl_id)
    
    def _record_crawl_start(self, crawl_id, seed_urls, max_depth, max_urls_per_domain):
        """Record crawl start information in DynamoDB."""
        try:
            master_id = f"master-{uuid.uuid4()}"
            timestamp = datetime.now(timezone.utc).isoformat()
            
            # Store in master-status table
            self.dynamodb.Table('master-status').put_item(
                Item={
                    'master_id': master_id,
                    'timestamp': timestamp,
                    'crawl_id': crawl_id,
                    'seed_urls': seed_urls,
                    'max_depth': max_depth,
                    'max_urls_per_domain': max_urls_per_domain,
                    'status': 'started'
                }
            )
            
            # Also store seed URLs in url-frontier
            url_frontier_table = self.dynamodb.Table('url-frontier')
            with url_frontier_table.batch_writer() as batch:
                for url in seed_urls:
                    batch.put_item(
                        Item={
                            'url': normalize_url(url),
                            'status': 'pending',
                            'job_id': f"job-{crawl_id}",  # Use job_id instead of crawl_id
                            'depth': 0,
                            'discovered_at': timestamp
                        }
                    )
            
            print(f"Recorded crawl start: {crawl_id}")
        except Exception as e:
            print(f"Error recording crawl start: {e}")
    
    def _enqueue_url(self, url, depth, max_depth, max_urls_per_domain, crawl_id=None):
        """Enqueue a URL for crawling if it meets the criteria."""
        with self.lock:
            # Skip if URL is already being processed or completed
            if url in self.urls_in_progress or url in self.urls_completed:
                return False
            
            # Check depth limit
            if depth > max_depth:
                return False
            
            # Check domain URL limit
            domain = get_domain(url)
            if self.domain_url_counts[domain] >= max_urls_per_domain:
                return False
            
            # Mark URL as in progress
            self.urls_in_progress.add(url)
            self.domain_url_counts[domain] += 1
        
        # Create a crawl task
        task_id = str(uuid.uuid4())
        job_id = f"job-{uuid.uuid4()}" if crawl_id is None else f"job-{crawl_id}"
        
        task = {
            'task_id': task_id,
            'url': url,
            'depth': depth,
            'max_depth': max_depth,
            'max_urls_per_domain': max_urls_per_domain,
            'job_id': job_id,
            'crawl_id': crawl_id if crawl_id else str(uuid.uuid4())
        }
        
        # Send the task to the queue
        try:
            self.sqs.send_message(
                QueueUrl=self.crawl_task_queue_url,
                MessageBody=json.dumps(task)
            )
            print(f"Enqueued URL: {url} (depth: {depth})")
            return True
        except Exception as e:
            print(f"Error adding URL to frontier: {e}")
            # Roll back the in-progress status
            with self.lock:
                self.urls_in_progress.remove(url)
                self.domain_url_counts[domain] -= 1
            return False
    
    def process_crawl_results(self):
        """Process crawl results from the queue."""
        try:
            # Receive messages from the crawl result queue
            response = self.sqs.receive_message(
                QueueUrl=self.crawl_result_queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5
            )
            
            if 'Messages' not in response:
                return
            
            for message in response['Messages']:
                try:
                    # Parse the message
                    result = json.loads(message['Body'])
                    
                    # Check if this is a heartbeat message
                    if 'status' in result and result.get('status') == 'active':
                        self._handle_heartbeat(result)
                        continue
                    
                    # Process the crawl result
                    url = result.get('url')
                    discovered_urls = result.get('discovered_urls', [])
                    depth = result.get('depth', 0)
                    max_depth = result.get('max_depth', 3)
                    max_urls_per_domain = result.get('max_urls_per_domain', 100)
                    crawl_id = result.get('crawl_id')
                    
                    # Update URL status in DynamoDB
                    try:
                        job_id = result.get('job_id', f"job-{uuid.uuid4()}")
                        
                        self.dynamodb.Table('url-frontier').update_item(
                            Key={
                                'url': url,
                                'job_id': job_id
                            },
                            UpdateExpression="SET #status = :status, completed_at = :timestamp",
                            ExpressionAttributeNames={
                                '#status': 'status'
                            },
                            ExpressionAttributeValues={
                                ':status': 'completed',
                                ':timestamp': datetime.now(timezone.utc).isoformat()
                            }
                        )
                    except Exception as e:
                        print(f"Error updating URL status: {e}")
                    
                    # Mark URL as completed
                    with self.lock:
                        if url in self.urls_in_progress:
                            self.urls_in_progress.remove(url)
                        self.urls_completed.add(url)
                    
                    # Enqueue discovered URLs
                    for discovered_url in discovered_urls:
                        self._enqueue_url(
                            url=discovered_url,
                            depth=depth + 1,
                            max_depth=max_depth,
                            max_urls_per_domain=max_urls_per_domain,
                            crawl_id=crawl_id
                        )
                    
                    # Send content to indexer
                    self._send_to_indexer(result)
                    
                    # Delete the processed message
                    self.sqs.delete_message(
                        QueueUrl=self.crawl_result_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                except Exception as e:
                    print(f"Error processing crawl result: {e}")
        except Exception as e:
            print(f"Error receiving crawl results: {e}")
    
    def _update_crawler_status(self, crawler_id, result):
        """Update crawler status in DynamoDB."""
        try:
            self.dynamodb.Table('crawler-status').put_item(
                Item={
                    'crawler_id': crawler_id,
                    'last_heartbeat': datetime.now(timezone.utc).isoformat(),
                    'status': 'active',
                    'last_url': result.get('url', ''),
                    'crawl_id': result.get('crawl_id', 'unknown')
                }
            )
        except Exception as e:
            print(f"Error updating crawler status: {e}")
    
    def _update_url_status(self, url, status, result=None):
        """Update URL status in DynamoDB."""
        try:
            update_expr = "SET #status = :status, last_updated = :timestamp"
            expr_attr_names = {'#status': 'status'}
            expr_attr_values = {
                ':status': status,
                ':timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            # Get crawl_id from result or use a default value
            job_id = result.get('crawl_id', 'unknown') if result else 'unknown'
            
            # Add additional data if available
            if result:
                if 's3_raw_path' in result:
                    update_expr += ", s3_raw_path = :s3_path"
                    expr_attr_values[':s3_path'] = result['s3_raw_path']
                
                if 'task_id' in result:
                    update_expr += ", task_id = :task_id"
                    expr_attr_values[':task_id'] = result['task_id']
            
            self.dynamodb.Table('url-frontier').update_item(
                 Key={'url': url, 'job_id': job_id},  # Use job_id instead of crawl_id
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_attr_names,
                ExpressionAttributeValues=expr_attr_values
            )
        except Exception as e:
            print(f"Error updating URL status: {e}")

    def _add_to_url_frontier(self, url, depth, crawl_id):
        """Add a URL to the frontier in DynamoDB."""
        try:
            # Check if URL already exists
            response = self.dynamodb.Table('url-frontier').get_item(
                Key={'url': url, 'job_id': crawl_id or 'unknown'}  # Use job_id instead of crawl_id
            )
            
            # Only add if it doesn't exist
            if 'Item' not in response:
                self.dynamodb.Table('url-frontier').put_item(
                    Item={
                        'url': url,
                        'status': 'discovered',
                        'depth': depth,
                        'job_id': crawl_id or 'unknown',  # Use job_id instead of crawl_id
                        'discovered_at': datetime.now(timezone.utc).isoformat()
                    }
                )
        except Exception as e:
            print(f"Error adding URL to frontier: {e}")
    
    def _store_discovered_urls(self, result):
        """Store discovered URLs in S3 for analysis."""
        try:
            if 'discovered_urls' in result and result['discovered_urls']:
                # Create a record with source URL and discovered URLs
                record = {
                    'source_url': result['url'],
                    'discovered_urls': result['discovered_urls'],
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'crawler_id': result.get('crawler_id', 'unknown'),
                    'crawl_id': result.get('crawl_id', 'unknown'),
                    'depth': result.get('depth', 0)
                }
                
                # Generate a key for S3
                key = f"discovered-urls/{result.get('crawl_id', 'unknown')}/{uuid.uuid4()}.json"
                
                # Upload to S3
                self.s3.put_object(
                    Bucket=CRAWL_DATA_BUCKET,
                    Key=key,
                    Body=json.dumps(record),
                    ContentType='application/json'
                )
        except Exception as e:
            print(f"Error storing discovered URLs: {e}")
    
    def _send_to_indexer(self, result):
        """Send crawled content to the indexer."""
        index_task = {
            'url': result['url'],
            'content': result['content'],
            'timestamp': time.time(),
            'crawl_id': result.get('crawl_id', 'unknown'),
            's3_raw_path': result.get('s3_raw_path', '')
        }
        
        try:
            self.sqs.send_message(
                QueueUrl=self.index_task_queue_url,
                MessageBody=json.dumps(index_task)
            )
            print(f"Sent to indexer: {result['url']}")
        except Exception as e:
            print(f"Error sending to indexer: {e}")
    
    def register_crawler(self, crawler_id):
        """Register a new crawler node."""
        with self.lock:
            self.active_crawlers[crawler_id] = time.time()
            print(f"Registered crawler: {crawler_id}")
        
        # Add to DynamoDB
        try:
            self.dynamodb.Table('crawler-status').put_item(
                Item={
                    'crawler_id': crawler_id,
                    'registration_time': datetime.now(timezone.utc).isoformat(),
                    'last_heartbeat': datetime.now(timezone.utc).isoformat(),
                    'status': 'registered'
                }
            )
        except Exception as e:
            print(f"Error registering crawler in DynamoDB: {e}")
    
    def _monitor_heartbeats(self):
        """Monitor crawler heartbeats and handle failed nodes."""
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            current_time = time.time()
            
            with self.lock:
                # Find crawlers that haven't sent a heartbeat recently
                failed_crawlers = []
                for crawler_id, last_heartbeat in self.active_crawlers.items():
                    if current_time - last_heartbeat > HEARTBEAT_INTERVAL * 2:
                        failed_crawlers.append(crawler_id)
                
                # Remove failed crawlers
                for crawler_id in failed_crawlers:
                    print(f"Crawler {crawler_id} failed, removing from active crawlers")
                    del self.active_crawlers[crawler_id]
                    
                    # Update status in DynamoDB
                    try:
                        self.dynamodb.Table('crawler-status').update_item(
                            Key={'crawler_id': crawler_id},
                            UpdateExpression="SET #status = :status, failed_at = :timestamp",
                            ExpressionAttributeNames={
                                '#status': 'status'
                            },
                            ExpressionAttributeValues={
                                ':status': 'failed',
                                ':timestamp': datetime.now(timezone.utc).isoformat()
                            }
                        )
                    except Exception as e:
                        print(f"Error updating failed crawler status: {e}")
    
    def get_crawl_stats(self):
        """Get current crawl statistics."""
        with self.lock:
            stats = {
                'active_crawlers': len(self.active_crawlers),
                'urls_in_progress': len(self.urls_in_progress),
                'urls_completed': len(self.urls_completed),
                'domains_crawled': len(self.domain_url_counts)
            }
        
        # Store stats in S3
        try:
            stats['timestamp'] = datetime.now(timezone.utc).isoformat()
            stats['master_id'] = f"master-{uuid.uuid4()}"
            
            # Generate a key for S3
            key = f"stats/master-stats-{datetime.now(timezone.utc).isoformat().replace(':', '-')}.json"
            
            # Upload to S3
            self.s3.put_object(
                Bucket=CRAWL_DATA_BUCKET,
                Key=key,
                Body=json.dumps(stats),
                ContentType='application/json'
            )
            
            print(f"Crawl stats saved to S3: {key}")
        except Exception as e:
            print(f"Error saving crawl stats: {e}")
        
        return stats
    
    def run(self):
        """Run the master node main loop."""
        print("Master node running...")
        try:
            last_stats_time = time.time()
            stats_interval = 300  # 5 minutes
            
            while True:
                # Process crawl results
                self.process_crawl_results()
                
                # Periodically save stats
                current_time = time.time()
                if current_time - last_stats_time > stats_interval:
                    self.get_crawl_stats()
                    last_stats_time = current_time
                
                # Sleep briefly to avoid tight loop
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("Master node shutting down...")
            # Save final stats before shutting down
            self.get_crawl_stats()

def process_client_requests(self):
    """Process client requests from the task queue."""
    try:
        # Receive messages from the crawl task queue
        response = self.sqs.receive_message(
            QueueUrl=self.crawl_task_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5
        )
        
        if 'Messages' not in response:
            return
        
        for message in response['Messages']:
            try:
                # Parse the message
                task = json.loads(message['Body'])
                
                # Check if this is a URL from a client
                if task.get('action') == 'crawl_url':
                    url = task.get('url')
                    max_depth = task.get('max_depth', 3)
                    max_urls_per_domain = task.get('max_urls_per_domain', 100)
                    
                    if url:
                        print(f"Received URL from client: {url}")
                        # Start a crawl with this URL
                        self.start_crawl([url], max_depth, max_urls_per_domain)
                
                # Delete the processed message
                self.sqs.delete_message(
                    QueueUrl=self.crawl_task_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
            except Exception as e:
                print(f"Error processing client request: {e}")
    except Exception as e:
        print(f"Error receiving client requests: {e}")

def process_crawl_tasks(self):
    """Process incoming crawl task messages."""
    try:
        # Receive messages from the crawl task queue
        response = self.sqs.receive_message(
            QueueUrl=self.crawl_task_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5
        )
        
        if 'Messages' not in response:
            return
        
        for message in response['Messages']:
            try:
                # Parse the message
                task = json.loads(message['Body'])
                
                # Check if this is a URL from the search interface
                if task.get('action') == 'crawl_url' and task.get('source') == 'search_interface':
                    url = task.get('url')
                    if url:
                        print(f"Received URL from search interface: {url}")
                        # Create a crawl task for this URL
                        self._enqueue_url(
                            url=url,
                            depth=0,
                            max_depth=MAX_DEPTH,
                            max_urls_per_domain=MAX_URLS_PER_DOMAIN,
                            crawl_id=str(uuid.uuid4())
                        )
                # Handle other task types (like start_crawl from client)
                elif task.get('action') == 'start_crawl':
                    url = task.get('url')
                    max_depth = task.get('max_depth', MAX_DEPTH)
                    max_urls_per_domain = task.get('max_urls_per_domain', MAX_URLS_PER_DOMAIN)
                    
                    if url:
                        self._enqueue_url(
                            url=url,
                            depth=0,
                            max_depth=max_depth,
                            max_urls_per_domain=max_urls_per_domain,
                            crawl_id=str(uuid.uuid4())
                        )
                
                # Delete the processed message
                self.sqs.delete_message(
                    QueueUrl=self.crawl_task_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
            except Exception as e:
                print(f"Error processing crawl task: {e}")
    except Exception as e:
        print(f"Error receiving crawl tasks: {e}")

def start(self):
    """Start the master node."""
    print("Starting master node")
    
    # Main processing loop
    while True:
        try:
            # Process crawl results
            self.process_crawl_results()
            
            # Process client requests
            self.process_client_requests()
            
            # Sleep briefly to avoid tight looping
            time.sleep(1)
        except Exception as e:
            print(f"Error in master node main loop: {e}")
            time.sleep(5)  # Sleep longer on error

if __name__ == "__main__":
    master = MasterNode()
    try:
        print("Master node running. Press Ctrl+C to stop.")
        while True:
            master.process_crawl_results()
            time.sleep(1)  # Avoid CPU spinning
    except KeyboardInterrupt:
        print("Master node stopped by user")
    # Example: Start a crawl with some seed URLs
    '''   seed_urls = [
        "http://books.toscrape.com/",
        "https://quotes.toscrape.com/"
    ]
    '''
    
 #   master.start_crawl(seed_urls)
  #  master.run()

def receive_client_url(self, url, max_depth=3, max_urls_per_domain=100):
    """
    Receive a URL from a client and start crawling it.
    
    Args:
        url (str): The URL to crawl
        max_depth (int): Maximum depth to crawl
        max_urls_per_domain (int): Maximum URLs to crawl per domain
    
    Returns:
        str: The crawl ID for the new crawl
    """
    print(f"Received URL from client: {url}")
    
    # Normalize the URL
    normalized_url = normalize_url(url)
    
    # Start a crawl with this URL as the seed
    crawl_id = str(uuid.uuid4())
    self.start_crawl([normalized_url], max_depth, max_urls_per_domain)
    
    return crawl_id