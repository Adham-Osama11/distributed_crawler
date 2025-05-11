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
import logging
import traceback
from decimal import Decimal
import botocore

# Add the parent directory to the path so we can import common modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import (
    CRAWL_TASK_QUEUE, CRAWL_RESULT_QUEUE, 
    INDEX_TASK_QUEUE, HEARTBEAT_INTERVAL,
    CRAWL_DATA_BUCKET, AWS_REGION, MASTER_COMMAND_QUEUE
)
from common.utils import get_domain, normalize_url

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Master] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('master_node.log')
    ]
)
logger = logging.getLogger(__name__)

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def to_decimal(val):
    try:
        return Decimal(str(val))
    except Exception:
        return Decimal('0')

class MasterNode:
    def __init__(self):
        logger.info("Initializing master node")
        # Initialize AWS clients
        self.sqs = boto3.client('sqs', region_name=AWS_REGION)
        self.s3 = boto3.client('s3', region_name=AWS_REGION)
        self.dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        logger.info(f"AWS clients initialized with region: {AWS_REGION}")
        
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
        logger.info("Heartbeat monitoring thread started")
        
        logger.info("Master node initialization complete")
    
    def _create_queues(self):
        """Get URLs for existing SQS queues or create them if they don't exist."""
        try:
            # Try to get queue URLs for existing queues, create if they don't exist
            try:
                response = self.sqs.get_queue_url(QueueName=CRAWL_TASK_QUEUE)
                self.crawl_task_queue_url = response['QueueUrl']
            except self.sqs.exceptions.QueueDoesNotExist:
                # Create the queue if it doesn't exist
                response = self.sqs.create_queue(
                    QueueName=CRAWL_TASK_QUEUE,
                    Attributes={
                        'VisibilityTimeout': '60',  # 1 minute
                        'MessageRetentionPeriod': '86400'  # 1 day
                    }
                )
                self.crawl_task_queue_url = response['QueueUrl']
                print(f"Created crawl task queue: {self.crawl_task_queue_url}")
            
            try:
                response = self.sqs.get_queue_url(QueueName=CRAWL_RESULT_QUEUE)
                self.crawl_result_queue_url = response['QueueUrl']
            except self.sqs.exceptions.QueueDoesNotExist:
                # Create the queue if it doesn't exist
                response = self.sqs.create_queue(
                    QueueName=CRAWL_RESULT_QUEUE,
                    Attributes={
                        'VisibilityTimeout': '60',  # 1 minute
                        'MessageRetentionPeriod': '86400'  # 1 day
                    }
                )
                self.crawl_result_queue_url = response['QueueUrl']
                print(f"Created crawl result queue: {self.crawl_result_queue_url}")
            
            try:
                response = self.sqs.get_queue_url(QueueName=INDEX_TASK_QUEUE)
                self.index_task_queue_url = response['QueueUrl']
            except self.sqs.exceptions.QueueDoesNotExist:
                # Create the queue if it doesn't exist
                response = self.sqs.create_queue(
                    QueueName=INDEX_TASK_QUEUE,
                    Attributes={
                        'VisibilityTimeout': '60',  # 1 minute
                        'MessageRetentionPeriod': '86400'  # 1 day
                    }
                )
                self.index_task_queue_url = response['QueueUrl']
                print(f"Created index task queue: {self.index_task_queue_url}")
            
            # Add the master command queue
            try:
                response = self.sqs.get_queue_url(QueueName=MASTER_COMMAND_QUEUE)
                self.master_command_queue_url = response['QueueUrl']
            except self.sqs.exceptions.QueueDoesNotExist:
                # Create the queue if it doesn't exist
                response = self.sqs.create_queue(
                    QueueName=MASTER_COMMAND_QUEUE,
                    Attributes={
                        'VisibilityTimeout': '60',  # 1 minute
                        'MessageRetentionPeriod': '86400'  # 1 day
                    }
                )
                self.master_command_queue_url = response['QueueUrl']
                print(f"Created master command queue: {self.master_command_queue_url}")
            
            print("Successfully connected to SQS queues")
        except Exception as e:
            print(f"Error getting/creating SQS queue URLs: {e}")
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
                        {'AttributeName': 'url', 'KeyType': 'HASH'},
                        {'AttributeName': 'job_id', 'KeyType': 'RANGE'}
                    ],
                    'AttributeDefinitions': [
                        {'AttributeName': 'url', 'AttributeType': 'S'},
                        {'AttributeName': 'job_id', 'AttributeType': 'S'}
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
            
            # Create or update tables
            for table_name, schema in tables_to_create.items():
                if table_name in existing_tables:
                    # Check if table needs to be recreated
                    try:
                        current_schema = self.dynamodb.meta.client.describe_table(TableName=table_name)
                        current_keys = {k['AttributeName']: k['KeyType'] for k in current_schema['Table']['KeySchema']}
                        expected_keys = {k['AttributeName']: k['KeyType'] for k in schema['KeySchema']}
                        
                        if current_keys != expected_keys:
                            logger.info(f"Table {table_name} schema mismatch. Current: {current_keys}, Expected: {expected_keys}")
                            logger.info(f"Deleting and recreating table {table_name}")
                            # Delete the table
                            self.dynamodb.Table(table_name).delete()
                            # Wait for deletion
                            self.dynamodb.meta.client.get_waiter('table_not_exists').wait(TableName=table_name)
                            # Create new table
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
                            self.dynamodb.meta.client.get_waiter('table_exists').wait(TableName=table_name)
                            logger.info(f"Table {table_name} recreated with new schema")
                    except Exception as e:
                        logger.error(f"Error checking/updating table {table_name}: {e}")
                        raise
                else:
                    # Create new table
                    logger.info(f"Creating new table {table_name}")
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
                    self.dynamodb.meta.client.get_waiter('table_exists').wait(TableName=table_name)
                    logger.info(f"Table {table_name} created successfully")
            
            logger.info("DynamoDB tables verified and updated")
        except Exception as e:
            logger.error(f"Error ensuring DynamoDB tables: {e}")
            raise
    
    def start_crawl(self, seed_urls, max_depth=3, max_urls_per_domain=100):
        """Start a new crawl with the given seed URLs."""
        crawl_id = str(uuid.uuid4())
        logger.info(f"Starting crawl {crawl_id} with {len(seed_urls)} seed URLs (max_depth: {max_depth}, max_urls_per_domain: {max_urls_per_domain})")
        
        # Record crawl start in DynamoDB
        self._record_crawl_start(crawl_id, seed_urls, max_depth, max_urls_per_domain)
        
        # Normalize seed URLs
        normalized_urls = [normalize_url(url) for url in seed_urls]
        logger.debug(f"Normalized seed URLs: {normalized_urls}")
        
        # Create crawl tasks for seed URLs
        for url in normalized_urls:
            logger.info(f"Enqueueing seed URL: {url} for crawl {crawl_id}")
            self._enqueue_url(url, depth=0, max_depth=max_depth, 
                             max_urls_per_domain=max_urls_per_domain, crawl_id=crawl_id)
        
        logger.info(f"Crawl {crawl_id} started successfully with {len(seed_urls)} seed URLs")
        return crawl_id
    
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
    
    def _enqueue_url(self, url, depth=0, max_depth=3, max_urls_per_domain=100, crawl_id=None):
        """
        Enqueue a URL for crawling.
        
        Args:
            url (str): The URL to crawl
            depth (int): Current depth of the URL
            max_depth (int): Maximum depth to crawl
            max_urls_per_domain (int): Maximum URLs to crawl per domain
            crawl_id (str): ID of the crawl job
        """
        logger.info(f"_enqueue_url: called with url={url}, depth={depth}, max_depth={max_depth}, max_urls_per_domain={max_urls_per_domain}, crawl_id={crawl_id}")

        if not url or not crawl_id:
            logger.error(f"_enqueue_url: Missing required parameters - url={url}, crawl_id={crawl_id}")
            return

        if depth > max_depth:
            logger.warning(f"_enqueue_url: Skipping {url} - depth {depth} > max_depth {max_depth}")
            return

        with self.lock:
            if url in self.urls_in_progress or url in self.urls_completed:
                logger.warning(f"_enqueue_url: {url} in progress or completed")
                return
            domain = get_domain(url)
            if self.domain_url_counts[domain] >= max_urls_per_domain:
                logger.warning(f"_enqueue_url: Domain limit for {domain} reached")
                return

        # Check if URL is already in the frontier
        try:
            frontier_table = self.dynamodb.Table('url-frontier')
            job_id = f"job-{crawl_id}"  # Ensure consistent job_id format
            
            logger.debug(f"Getting item from url-frontier with url={url}, job_id={job_id}")
            
            try:
                # Use the correct key structure for the composite key
                response = frontier_table.get_item(
                    Key={
                        'url': url,
                        'job_id': job_id
                    }
                )
            except self.dynamodb.meta.client.exceptions.ResourceNotFoundException:
                # Table doesn't exist, create it
                self._ensure_dynamodb_tables()
                response = {}
            except botocore.exceptions.ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'ValidationException':
                    logger.error(f"Validation error getting item from url-frontier: {e}")
                    logger.error(f"Key elements used - url: {url}, job_id: {job_id}")
                    logger.error(f"Error details: {e.response['Error']}")
                    # Ensure table has correct schema
                    self._ensure_dynamodb_tables()
                    # Retry the operation
                    response = frontier_table.get_item(
                        Key={
                            'url': url,
                            'job_id': job_id
                        }
                    )
                else:
                    raise
            
            if 'Item' in response:
                status = response['Item'].get('status')
                logger.warning(f"_enqueue_url: {url} already in frontier with status {status}")
                if status in ['completed', 'in_progress']:
                    with self.lock:
                        self.urls_in_progress.discard(url)
                        if status == 'completed':
                            self.urls_completed.add(url)
                    logger.debug(f"Skipping URL {url} - already {status}")
                    return

            # Create a task for the URL
            task = {
                'url': url,
                'depth': depth,
                'max_depth': max_depth,
                'max_urls_per_domain': max_urls_per_domain,
                'task_id': f"task-{uuid.uuid4()}",
                'job_id': job_id,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }

            # Add to URL frontier
            try:
                frontier_table.put_item(
                    Item={
                        'url': url,
                        'job_id': job_id,
                        'status': 'pending',
                        'depth': depth,
                        'created_at': datetime.now(timezone.utc).isoformat(),
                        'task_id': task['task_id']
                    }
                )
            except self.dynamodb.meta.client.exceptions.ResourceNotFoundException:
                # Table doesn't exist, create it and retry
                self._ensure_dynamodb_tables()
                frontier_table.put_item(
                    Item={
                        'url': url,
                        'job_id': job_id,
                        'status': 'pending',
                        'depth': depth,
                        'created_at': datetime.now(timezone.utc).isoformat(),
                        'task_id': task['task_id']
                    }
                )

            # Send the task to the queue
            logger.info(f"_enqueue_url: About to send crawl task for {url} to SQS")
            self.sqs.send_message(
                QueueUrl=self.crawl_task_queue_url,
                MessageBody=json.dumps(task)
            )
            logger.debug(f"Task sent to queue for URL: {url}")

            # After successfully sending the task to SQS:
            with self.lock:
                self.urls_in_progress.add(url)
                self.domain_url_counts[domain] += 1

        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")
            logger.error(traceback.format_exc())
            with self.lock:
                self.urls_in_progress.discard(url)
                self.domain_url_counts[domain] -= 1

    def process_crawl_results(self):
        """Process crawl results from the result queue."""
        try:
            # Receive messages from the crawl result queue
            response = self.sqs.receive_message(
                QueueUrl=self.crawl_result_queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1
            )
            
            if 'Messages' not in response:
                return
            
            for message in response.get('Messages', []):
                try:
                    # Parse the message body
                    body = json.loads(message['Body'])
                    logger.info(f"Received message body: {json.dumps(body)}")
                    
                    # Handle different message types
                    if body.get('type') == 'heartbeat':
                        # Process heartbeat message
                        crawler_id = body.get('crawler_id')
                        logger.info(f"Received heartbeat from crawler: {crawler_id}")
                        self._update_crawler_status(crawler_id, body)
                    elif body.get('type') == 'result':
                        # Process crawl result message
                        url = body.get('url')
                        logger.info(f"Processing crawl result for URL: {url}")
                        
                        # Mark URL as completed in DynamoDB
                        self._update_url_status(url, 'completed', body)
                        logger.info(f"URL {url} marked as completed in DynamoDB")
                        
                        # Process discovered URLs
                        discovered_urls = body.get('discovered_urls', [])
                        if discovered_urls:
                            logger.info(f"Processing {len(discovered_urls)} discovered URLs from {url}")
                            current_depth = body.get('depth', 0)
                            max_depth = body.get('max_depth', 3)
                            max_urls_per_domain = body.get('max_urls_per_domain', 100)
                            crawl_id = body.get('job_id', '').replace('job-', '')  # Extract crawl_id from job_id
                            
                            # Enqueue discovered URLs
                            for discovered_url in discovered_urls:
                                self._enqueue_url(
                                    url=discovered_url,
                                    depth=current_depth + 1,
                                    max_depth=max_depth,
                                    max_urls_per_domain=max_urls_per_domain,
                                    crawl_id=crawl_id
                                )
                            logger.info(f"Enqueued {len(discovered_urls)} discovered URLs for crawling")
                        
                        # Forward to indexer
                        try:
                            # Create a new message for the indexer
                            index_message = {
                                'type': 'index',
                                'url': url,
                                'job_id': body.get('job_id'),
                                'task_id': body.get('task_id'),
                                'crawler_id': body.get('crawler_id'),
                                'timestamp': body.get('timestamp'),
                                's3_key': body.get('s3_key')
                            }
                            
                            # Send the message to the index task queue
                            self.sqs.send_message(
                                QueueUrl=self.index_task_queue_url,
                                MessageBody=json.dumps(index_message)
                            )
                            logger.info(f"Forwarded URL {url} to indexer")
                        except Exception as e:
                            logger.error(f"Error forwarding to indexer: {e}")
                            logger.error(traceback.format_exc())
                    else:
                        logger.warning(f"Unknown message type: {body.get('type')}")
                    
                    # Delete the message from the queue
                    self.sqs.delete_message(
                        QueueUrl=self.crawl_result_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    logger.error(traceback.format_exc())
        except Exception as e:
            logger.error(f"Error receiving messages: {e}")
            logger.error(traceback.format_exc())
            
    def _handle_heartbeat(self, heartbeat):
        """Handle a heartbeat message from a crawler node."""
        crawler_id = heartbeat.get('crawler_id')
        timestamp = float(heartbeat.get('timestamp', time.time()))
        
        if crawler_id:
            with self.lock:
                # Update the last heartbeat time for this crawler
                self.active_crawlers[crawler_id] = timestamp
                
            # Update the crawler status in DynamoDB
            try:
                self.dynamodb.Table('crawler-status').put_item(
                    Item={
                        'crawler_id': crawler_id,
                        'last_heartbeat': datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
                        'status': 'active',
                        'memory_usage': to_decimal(heartbeat.get('memory_usage', 0)),
                        'active_tasks': to_decimal(heartbeat.get('active_tasks', 0)),
                        'failed_tasks': to_decimal(heartbeat.get('failed_tasks', 0))
                    }
                )
            except Exception as e:
                print(f"Error updating crawler status in DynamoDB: {e}")
                
            print(f"Received heartbeat from crawler: {crawler_id}")
            
            # Send heartbeat to SQS
            self.sqs.send_message(
                QueueUrl=self.crawl_result_queue_url,
                MessageBody=json.dumps(heartbeat, default=decimal_default)
            )
    
    def _update_crawler_status(self, crawler_id, status_data):
        """Update crawler status in DynamoDB."""
        try:
            # Get the crawler-status table
            table = self.dynamodb.Table('crawler-status')
            
            # Update the crawler status
            table.put_item(
                Item={
                    'crawler_id': crawler_id,
                    'status': status_data.get('status', 'unknown'),
                    'last_heartbeat': datetime.now(timezone.utc).isoformat(),
                    'urls_crawled': status_data.get('urls_crawled', 0),
                    'current_url': status_data.get('current_url', 'none')
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
            
            # Get job_id from result or use a default value
            job_id = result.get('job_id', 'unknown') if result else 'unknown'
            
            # Add additional data if available
            if result:
                if 's3_raw_path' in result:
                    update_expr += ", s3_raw_path = :s3_path"
                    expr_attr_values[':s3_path'] = result['s3_raw_path']
                
                if 'task_id' in result:
                    update_expr += ", task_id = :task_id"
                    expr_attr_values[':task_id'] = result['task_id']
            
            self.dynamodb.Table('url-frontier').update_item(
                Key={'url': url, 'job_id': job_id},
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_attr_names,
                ExpressionAttributeValues=expr_attr_values
            )
        except Exception as e:
            logger.error(f"Error updating URL status: {e}")
            logger.error(traceback.format_exc())

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
    
    def start(self) -> None:
        """Start the master node."""
        logger.info("Starting master node")
        
        try:
            while True:
                try:
                    self.process_master_commands()                    
                    self.process_client_requests()
                    self.process_crawl_results()
                    self.process_crawl_tasks()
                    time.sleep(0.1)
                except Exception as e:
                    logger.error(f"Error in master node main loop: {e}")
                    logger.error(traceback.format_exc())
                    time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down gracefully...")
            logger.info("Master node shutdown complete")

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
                logger.debug("No messages received from crawl task queue")
                return
            
            logger.info(f"Received {len(response['Messages'])} messages from crawl task queue")
            for message in response['Messages']:
                try:
                    # Parse the message
                    task = json.loads(message['Body'])
                    message_id = message.get('MessageId', 'unknown')
                    logger.info(f"Processing message {message_id}, action: {task.get('action', 'unknown')}")
                    
                    # Check if this is a URL from the search interface
                    if task.get('action') == 'crawl_url' and task.get('source') == 'search_interface':
                        url = task.get('url')
                        if url:
                            logger.info(f"Received URL from search interface: {url}")
                            # Create a crawl task for this URL
                            crawl_id = str(uuid.uuid4())
                            logger.info(f"Creating new crawl {crawl_id} for URL: {url}")
                            self._enqueue_url(
                                url=url,
                                depth=0,
                                max_depth=3,  # Replace MAX_DEPTH with a literal value
                                max_urls_per_domain=100,  # Replace MAX_URLS_PER_DOMAIN with a literal value
                                crawl_id=crawl_id
                            )
                    # Handle other task types (like start_crawl from client)
                    elif task.get('action') == 'start_crawl':
                        url = task.get('url')
                        max_depth = task.get('max_depth', 3)  # Replace MAX_DEPTH with a literal value
                        max_urls_per_domain = task.get('max_urls_per_domain', 100)  # Replace MAX_URLS_PER_DOMAIN with a literal value
                        
                        if url:
                            crawl_id = str(uuid.uuid4())
                            logger.info(f"Starting new crawl {crawl_id} for URL: {url} (max_depth: {max_depth}, max_urls_per_domain: {max_urls_per_domain})")
                            self._enqueue_url(
                                url=url,
                                depth=0,
                                max_depth=max_depth,
                                max_urls_per_domain=max_urls_per_domain,
                                crawl_id=crawl_id
                            )
                    
                    # Delete the processed message
                    logger.debug(f"Deleting message {message_id} from crawl task queue")
                    self.sqs.delete_message(
                        QueueUrl=self.crawl_task_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    logger.info(f"Successfully processed message {message_id}")
                except Exception as e:
                    logger.error(f"Error processing crawl task message {message.get('MessageId', 'unknown')}: {e}")
                    logger.error(traceback.format_exc())
        except Exception as e:
            logger.error(f"Error receiving crawl tasks: {e}")
            logger.error(traceback.format_exc())

    def process_master_commands(self):
        """Process commands from the master command queue."""
        try:
            # Receive messages from the master command queue
            response = self.sqs.receive_message(
                QueueUrl=self.master_command_queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1
            )
            
            if 'Messages' not in response:
                return
            
            for message in response.get('Messages', []):
                try:
                    # Parse the message body
                    body = json.loads(message['Body'])
                    logger.info(f"Received command: {body}")
                    
                    # Process the command
                    if body.get('action') == 'start_crawl':
                        self._handle_start_crawl_command(body)
                    elif body.get('action') == 'crawl_url' and body.get('source') == 'search_interface':
                        self._handle_crawl_url_command(body)
                    elif body.get('type') == 'start_crawl':
                        self._handle_batch_crawl_command(body)
                    elif body.get('command') == 'start_crawl':
                        self._handle_dashboard_crawl_command(body)
                    elif body.get('action') == 'resend_urls':
                        self._handle_resend_urls_command(body)
                    else:
                        logger.warning(f"Unknown command format: {body}")
                    
                    # Delete the processed message
                    self.sqs.delete_message(
                        QueueUrl=self.master_command_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                except Exception as e:
                    logger.error(f"Error processing command: {e}")
                    logger.error(traceback.format_exc())
        except Exception as e:
            logger.error(f"Error receiving commands: {e}")
            logger.error(traceback.format_exc())

    def _clean_url(self, url):
        """Clean and validate a URL."""
        if not url:
            return None
        
        # Remove backticks and trim whitespace
        url = url.replace('`', '').strip()
        
        # Ensure URL has a scheme
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        return url

    def _handle_start_crawl_command(self, command):
        """Handle start_crawl command with URL cleaning."""
        url = self._clean_url(command.get('url', ''))
        if not url:
            logger.error("Received start_crawl command with empty URL")
            return
        
        max_depth = int(command.get('max_depth', 3))
        max_urls_per_domain = int(command.get('max_urls_per_domain', 100))
        
        logger.info(f"Starting crawl with cleaned URL: {url}, max_depth: {max_depth}, max_urls_per_domain: {max_urls_per_domain}")
        self.start_crawl([url], max_depth=max_depth, max_urls_per_domain=max_urls_per_domain)

    def _handle_crawl_url_command(self, command):
        """Handle crawl_url command from search interface."""
        url = self._clean_url(command.get('url'))
        if not url:
            logger.error("Received crawl_url command with empty URL")
            return
        
        logger.info(f"Processing URL from search interface: {url}")
        self._enqueue_url(
            url=url,
            depth=0,
            max_depth=command.get('max_depth', 3),
            max_urls_per_domain=command.get('max_urls_per_domain', 100),
            crawl_id=str(uuid.uuid4())
        )

    def _handle_batch_crawl_command(self, command):
        """Handle batch crawl command with multiple seed URLs."""
        seed_urls = [self._clean_url(url) for url in command.get('seed_urls', [])]
        seed_urls = [url for url in seed_urls if url]  # Filter out None values
        
        if not seed_urls:
            logger.warning("Received start_crawl command with no valid seed URLs")
            return
        
        logger.info(f"Starting batch crawl with {len(seed_urls)} seed URLs")
        self.start_crawl(
            seed_urls,
            max_depth=command.get('max_depth', 3),
            max_urls_per_domain=command.get('max_urls_per_domain', 100)
        )

    def _handle_dashboard_crawl_command(self, command):
        """Handle crawl command from dashboard."""
        seed_urls = [self._clean_url(url) for url in command.get('seed_urls', [])]
        seed_urls = [url for url in seed_urls if url]  # Filter out None values
        
        if not seed_urls:
            logger.warning("Received dashboard crawl command with no valid seed URLs")
            return
        
        logger.info(f"Starting dashboard crawl with {len(seed_urls)} seed URLs")
        self.start_crawl(
            seed_urls,
            max_depth=command.get('max_depth', 3),
            max_urls_per_domain=command.get('max_urls_per_domain', 100)
        )

    def _handle_resend_urls_command(self, command):
        """Handle command to resend URLs for crawling."""
        try:
            crawl_id = command.get('crawl_id')
            if not crawl_id:
                logger.error("Received resend_urls command without crawl_id")
                return

            logger.info(f"Resending URLs for crawl {crawl_id}")
            
            # Query the URL frontier for pending or failed URLs
            frontier_table = self.dynamodb.Table('url-frontier')
            job_id = f"job-{crawl_id}"
            
            # Scan for URLs with status 'pending' or 'failed'
            response = frontier_table.scan(
                FilterExpression='job_id = :job_id AND (#status = :pending OR #status = :failed)',
                ExpressionAttributeNames={
                    '#status': 'status'
                },
                ExpressionAttributeValues={
                    ':job_id': job_id,
                    ':pending': 'pending',
                    ':failed': 'failed'
                }
            )
            
            urls_to_resend = response.get('Items', [])
            logger.info(f"Found {len(urls_to_resend)} URLs to resend")
            
            # Resend each URL
            for item in urls_to_resend:
                url = item['url']
                depth = item.get('depth', 0)
                logger.info(f"Resending URL: {url} at depth {depth}")
                
                # Create a new task
                task = {
                    'url': url,
                    'depth': depth,
                    'max_depth': command.get('max_depth', 3),
                    'max_urls_per_domain': command.get('max_urls_per_domain', 100),
                    'task_id': f"task-{uuid.uuid4()}",
                    'job_id': job_id,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
                
                # Update URL status to pending
                frontier_table.update_item(
                    Key={'url': url, 'job_id': job_id},
                    UpdateExpression='SET #status = :status, task_id = :task_id, last_updated = :timestamp',
                    ExpressionAttributeNames={'#status': 'status'},
                    ExpressionAttributeValues={
                        ':status': 'pending',
                        ':task_id': task['task_id'],
                        ':timestamp': datetime.now(timezone.utc).isoformat()
                    }
                )
                
                # Send the task to the queue
                self.sqs.send_message(
                    QueueUrl=self.crawl_task_queue_url,
                    MessageBody=json.dumps(task)
                )
                logger.info(f"Resent URL {url} to crawl queue")
                
        except Exception as e:
            logger.error(f"Error resending URLs: {e}")
            logger.error(traceback.format_exc())

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
        logger.info(f"Received URL from client: {url}")
        
        # Normalize the URL
        normalized_url = normalize_url(url)
        
        # Start a crawl with this URL as the seed
        crawl_id = str(uuid.uuid4())
        self.start_crawl([normalized_url], max_depth, max_urls_per_domain)
        
        return crawl_id

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
    def _process_crawl_result(self, result):
        """Process a crawl result from a crawler node.
        
        Args:
            result (dict): The crawl result containing:
                - url: The crawled URL
                - content: The crawled content
                - discovered_urls: List of URLs discovered during crawl
                - crawl_id: The ID of the crawl job
                - depth: The depth at which this URL was crawled
                - s3_raw_path: Path to raw content in S3
        """
        try:
            url = result['url']
            logger.info(f"Processing crawl result for URL: {url}")
            
            # Update URL status to completed
            self._update_url_status(url, 'completed', result)
            
            # Store discovered URLs if any
            if 'discovered_urls' in result:
                self._store_discovered_urls(result)
            
            # Update tracking sets
            with self.lock:
                self.urls_in_progress.discard(url)
                self.urls_completed.add(url)
            
            # Store crawl metadata
            try:
                self.dynamodb.Table('crawl-metadata').put_item(
                    Item={
                        'url': url,
                        'job_id': result.get('crawl_id', 'unknown'),
                        'crawl_timestamp': datetime.now(timezone.utc).isoformat(),
                        'depth': result.get('depth', 0),
                        's3_raw_path': result.get('s3_raw_path', ''),
                        'content_length': self._get_content_length_from_s3(result.get('s3_key')),
                        'discovered_urls_count': len(result.get('discovered_urls', []))
                    }
                )
            except Exception as e:
                logger.error(f"Error storing crawl metadata: {e}")
            
            logger.info(f"URL {result.get('url')} marked as completed in DynamoDB")
            
        except Exception as e:
            logger.error(f"Error processing crawl result: {e}")
            logger.error(traceback.format_exc())

    def _get_content_length_from_s3(self, s3_key):
        if not s3_key:
            return 0
        try:
            s3_obj = self.s3.get_object(Bucket=CRAWL_DATA_BUCKET, Key=s3_key)
            content = s3_obj['Body'].read()
            return len(content)
        except Exception as e:
            logger.error(f"Error fetching content from S3 for key {s3_key}: {e}")
            return 0

    def _send_heartbeat(self):
        """Send a heartbeat to DynamoDB with enhanced status information."""
        logger.debug("Sending heartbeat")
        try:
            timestamp = datetime.now(timezone.utc).isoformat()
            status = {
                'indexer_id': self.indexer_id,
                'timestamp': timestamp,
                'status': 'active',
                'processed_count': Decimal(str(self.processed_count)),
                'document_count': Decimal(str(self.document_count)),
                'failed_tasks': Decimal(str(len(self.failed_tasks))),
                'last_successful_task': self.last_successful_task if hasattr(self, 'last_successful_task') else None,
                'memory_usage': Decimal(str(self._get_memory_usage())),
                'index_size': Decimal(str(self._get_index_size()))
            }
            self.dynamodb.Table('indexer-status').put_item(Item=status)
            self.last_heartbeat = time.time()
            logger.debug(f"Heartbeat sent at {timestamp}, status: {status}")
        except Exception as e:
            logger.error(f"Error sending heartbeat: {e}")
            logger.error(traceback.format_exc())

# Main execution block
if __name__ == "__main__":

    try:
        """Start the master node."""
        logger.info("Starting master node")
        master = MasterNode()
        try:
            while True:
                try:
                    master.process_master_commands()                    
                    master.process_client_requests()
                    master.process_crawl_results()
                    master.process_crawl_tasks()             
                    time.sleep(0.1)
                except Exception as e:
                    logger.error(f"Error in master node main loop: {e}")
                    logger.error(traceback.format_exc())
                    time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down gracefully...")
            logger.info("Master node shutdown complete")
    except Exception as e:
        logger.error(f"Fatal error initializing master node: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
