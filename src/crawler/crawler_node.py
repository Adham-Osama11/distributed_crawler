"""
Crawler Node for the distributed web crawling system.
Responsible for fetching web pages and extracting content and links.
"""
import json
import time
import uuid
import boto3
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.loader import ItemLoader
from scrapy.item import Item, Field
from scrapy import signals
from datetime import datetime, timezone
import sys
import os
import threading
import logging
import traceback

# Add the parent directory to the path so we can import common modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import (
    CRAWL_TASK_QUEUE, CRAWL_RESULT_QUEUE, 
    HEARTBEAT_INTERVAL, AWS_REGION , CRAWL_DATA_BUCKET
)
from common.utils import get_domain, normalize_url

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Crawler] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('crawler_node.log')
    ]
)
logger = logging.getLogger(__name__)

class WebPage(Item):
    """Scrapy Item for storing web page data."""
    url = Field()
    title = Field()
    description = Field()
    keywords = Field()
    text = Field()
    links = Field()
    timestamp = Field()
    content_type = Field()
    language = Field()
    html = Field()

class WebSpider(scrapy.Spider):
    """Scrapy spider for crawling web pages."""
    name = 'web_spider'
    
    def __init__(self, url=None, task_id=None, depth=0, *args, **kwargs):
        super(WebSpider, self).__init__(*args, **kwargs)
        self.start_urls = [url] if url else []
        self.task_id = task_id
        self.depth = depth
        self.results = []
    
    def parse(self, response):
        """Parse the response and extract content and links."""
        try:
            # Use ItemLoader for better data extraction
            # Pass the response object as the selector
            loader = ItemLoader(item=WebPage(), selector=response)
            
            # Extract metadata
            loader.add_value('url', response.url)
            loader.add_xpath('title', '//title/text()')
            loader.add_xpath('description', "//meta[@name='description']/@content")
            loader.add_xpath('keywords', "//meta[@name='keywords']/@content")
            loader.add_xpath('text', '//body//text()')
            loader.add_css('links', 'a::attr(href)')
            loader.add_value('timestamp', datetime.now(timezone.utc).isoformat())
            loader.add_value('content_type', response.headers.get('Content-Type', b'').decode('utf-8', errors='ignore'))
            loader.add_xpath('language', '//html/@lang')
            loader.add_value('html', response.text)
            
            # Load the item
            item = loader.load_item()
            
            # Clean up extracted fields
            if 'title' in item and isinstance(item['title'], list):
              item['title'] = ' '.join([t.strip() for t in item['title'] if t])

            if 'description' in item and isinstance(item['description'], list):
                item['description'] = ' '.join([d.strip() for d in item['description'] if d])
            elif 'description' not in item:
                 item['description'] = ''

            if 'keywords' not in item or not item['keywords']:
                item['keywords'] = ''
            elif isinstance(item['keywords'], list):
                item['keywords'] = ' '.join(item['keywords'])

            if 'content_type' in item and isinstance(item['content_type'], list):
               item['content_type'] = item['content_type'][0]

            if 'language' in item and isinstance(item['language'], list):
                item['language'] = item['language'][0]
            
            if 'text' in item and isinstance(item['text'], list):
                item['text'] = [t.strip() for t in item['text'] if t.strip()]

            # Extract links for further crawling
            discovered_urls = []
            for href in response.css('a::attr(href)').getall():
                discovered_url = response.urljoin(href)
                discovered_urls.append(normalize_url(discovered_url))
            
            # Store the result
            self.results.append({
                'task_id': self.task_id,
                'url': response.url,
                'content': dict(item),
                'discovered_urls': discovered_urls,
                'depth': self.depth
            })
        except Exception as e:
            print(f"Error parsing {response.url}: {e}")
            import traceback
            print(traceback.format_exc())


class CrawlerNode:
    """
    Crawler Node class that fetches and processes web pages.
    """
    def __init__(self, crawler_id=None):
        self.crawler_id = crawler_id or f"crawler-{uuid.uuid4()}"
        # Use the region from config instead of hardcoding
        self.sqs = boto3.client('sqs', region_name=AWS_REGION)
        
        # Initialize S3 and DynamoDB clients with the same region
        self.s3 = boto3.client('s3', region_name=AWS_REGION)
        self.dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        
        # Get queue URLs or create them if they don't exist
        try:
            logger.info(f"Connecting to SQS queue: {CRAWL_TASK_QUEUE}")
            response = self.sqs.get_queue_url(QueueName=CRAWL_TASK_QUEUE)
            self.crawl_task_queue_url = response['QueueUrl']
            logger.info(f"Connected to crawl task queue: {self.crawl_task_queue_url}")
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
            logger.warning(f"Queue {CRAWL_RESULT_QUEUE} does not exist, creating it")
            response = self.sqs.create_queue(
                QueueName=CRAWL_RESULT_QUEUE,
                Attributes={
                    'VisibilityTimeout': '60',  # 1 minute
                    'MessageRetentionPeriod': '86400'  # 1 day
                }
            )
            self.crawl_result_queue_url = response['QueueUrl']
        logger.info(f"Connected to crawl result queue: {self.crawl_result_queue_url}")
        
        try:
            # Test connectivity to task queue
            self.sqs.get_queue_attributes(
                QueueUrl=self.crawl_task_queue_url,
                AttributeNames=['QueueArn']
            )
            logger.info(f"Successfully connected to task queue: {self.crawl_task_queue_url}")
            
            # Test connectivity to result queue
            self.sqs.get_queue_attributes(
                QueueUrl=self.crawl_result_queue_url,
                AttributeNames=['QueueArn']
            )
            logger.info(f"Successfully connected to result queue: {self.crawl_result_queue_url}")
        except Exception as e:
            logger.error(f"Error testing queue connectivity: {e}")
        
        try:
            # Test connectivity to task queue
            self.sqs.get_queue_attributes(
                QueueUrl=self.crawl_task_queue_url,
                AttributeNames=['QueueArn']
            )
            logger.info(f"Successfully connected to task queue: {self.crawl_task_queue_url}")
            
            # Test connectivity to result queue
            self.sqs.get_queue_attributes(
                QueueUrl=self.crawl_result_queue_url,
                AttributeNames=['QueueArn']
            )
            logger.info(f"Successfully connected to result queue: {self.crawl_result_queue_url}")
        except Exception as e:
            logger.error(f"Error testing queue connectivity: {e}")
        # Initialize Scrapy settings
        self.settings = get_project_settings()
        self.settings.update({
            'USER_AGENT': 'DistributedCrawler/1.0',
            'ROBOTSTXT_OBEY': True,
            'DOWNLOAD_DELAY': 1.0,  # Basic politeness - 1 second delay
            'CONCURRENT_REQUESTS': 1,  # Start with just 1 concurrent request
            'LOG_LEVEL': 'ERROR',  # Only show errors, not info messages
            'LOG_ENABLED': False,  # Disable logging completely if you want no output
            'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',  # Add this line to fix the deprecation warning
        })
        logger.info("Scrapy settings initialized")
        # Flag to control the crawler
        self.running = False
        
        # Ensure DynamoDB tables exist
        self._ensure_dynamodb_tables()
        
        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeats)
        self.heartbeat_thread.daemon = True
        logger.info("Crawler node initialization complete")
    
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
                        {'AttributeName': 'job_id', 'KeyType': 'RANGE'}  # Add job_id as a sort key
                    ],
                    'AttributeDefinitions': [
                        {'AttributeName': 'url', 'AttributeType': 'S'},
                        {'AttributeName': 'job_id', 'AttributeType': 'S'}  # Define job_id attribute
                    ]
                }
            }
            
            # Check if tables exist and create them if they don't
            existing_tables = [table.name for table in self.dynamodb.tables.all()]
            
            for table_name, schema in tables_to_create.items():
                if table_name not in existing_tables:
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
                        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
                        print(f"Table {table_name} created successfully")
                    except Exception as e:
                        print(f"Error creating DynamoDB table {table_name}: {e}")
        except Exception as e:
            print(f"Error ensuring DynamoDB tables: {e}")
    
    def start(self):
        """Start the crawler node."""
        print(f"Starting crawler node: {self.crawler_id}")
        self.running = True
        
        # Start heartbeat thread
        self.heartbeat_thread.start()
        logger.info("Heartbeat thread started")

        # Register with master node
        self._register_with_master()
        logger.info("Registered with master node")

        # Main crawling loop
        logger.info("Entering main crawling loop")
        while self.running:
            try:
                # Get a task from the queue
                task = self._get_task()
                
                if task:
                    # Process the task
                    logger.info(f"Processing task for URL: {task.get('url')} (depth: {task.get('depth', 0)})")
                    self._process_task(task)
                else:
                    # No tasks available, wait a bit
                    logger.debug("No tasks available, waiting...")
                    time.sleep(1)
            except Exception as e:
                import traceback
                logger.error(f"Error in crawler main loop: {e}")
                logger.error(traceback.format_exc())  # Print the full stack trac
                time.sleep(1)
    
    def stop(self):
        """Stop the crawler node."""
        print(f"Stopping crawler node: {self.crawler_id}")
        self.running = False
    
    def _register_with_master(self):
        """Register this crawler with the master node."""
        # In a real implementation, this would send a registration message to the master
        # For now, we'll just send a heartbeat which will register us
        self._send_heartbeat()
    
    def _send_heartbeats(self):
        """Send periodic heartbeats to the master node."""
        while self.running:
            try:
                self._send_heartbeat()
                time.sleep(HEARTBEAT_INTERVAL)
            except Exception as e:
                print(f"Error sending heartbeat: {e}")
                time.sleep(1)
    
    def _send_heartbeat(self):
        """Send a heartbeat to the master node."""
        heartbeat = {
            'type': 'heartbeat',  # Add message type
            'crawler_id': self.crawler_id,
            'timestamp': time.time(),
            'status': 'active'
        }
        
        try:
            self.sqs.send_message(
                QueueUrl=self.crawl_result_queue_url,  # Make sure this is the RESULT queue
                MessageBody=json.dumps(heartbeat)
            )
        except Exception as e:
            print(f"Error sending heartbeat: {e}")
    
    def _get_task(self):
        """Get a task from the crawl task queue and update URL frontier."""
        try:
            logger.debug(f"Attempting to receive message from queue: {self.crawl_task_queue_url}")
            response = self.sqs.receive_message(
                QueueUrl=self.crawl_task_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=5
            )
            
            if 'Messages' in response and response['Messages']:
                message = response['Messages'][0]
                task = json.loads(message['Body'])
                
                # Check message type - only process 'task' type messages
                message_type = task.get('type', '')
                if message_type == 'heartbeat':
                    logger.warning(f"Received heartbeat message in task queue, deleting: {task}")
                    self.sqs.delete_message(
                        QueueUrl=self.crawl_task_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    return None
                
                # Validate task structure
                if 'url' not in task:
                    logger.error(f"Received malformed task without 'url' field: {task}")
                    # Delete the invalid message from the queue
                    self.sqs.delete_message(
                        QueueUrl=self.crawl_task_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    logger.info("Deleted malformed task from queue")
                    return None
                
                # Delete the message from the queue
                logger.debug(f"Deleting message from queue: {message['MessageId']}")
                self.sqs.delete_message(
                    QueueUrl=self.crawl_task_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                
                # Update URL frontier in DynamoDB
                try:
                    # First check if the table exists
                    frontier_table = self.dynamodb.Table('url-frontier')
                    
                    # Ensure job_id is present in the task
                    job_id = task.get('job_id')
                    if not job_id:
                        job_id = f"job-{uuid.uuid4()}"
                        task['job_id'] = job_id
                        logger.info(f"Generated new job_id: {job_id} for URL: {task['url']}")
                    
                    # Try to update the URL status
                    try:
                        logger.debug(f"Updating URL frontier for URL: {task['url']}, job_id: {job_id}")
                        frontier_table.update_item(
                            Key={
                                'url': task['url'],
                                'job_id': job_id
                            },
                            UpdateExpression="SET #status = :status, last_crawled = :timestamp",
                            ExpressionAttributeNames={
                                '#status': 'status'
                            },
                            ExpressionAttributeValues={
                                ':status': 'in_progress',
                                ':timestamp': datetime.now(timezone.utc).isoformat()
                            }
                        )
                        logger.debug(f"Successfully updated URL frontier for URL: {task['url']}")
                    except Exception as e:
                        # If update fails, try to put the item instead
                        logger.warning(f"Failed to update URL frontier for URL: {task['url']}, error: {e}")
                        logger.info(f"Trying to put item instead of update for URL: {task['url']}")
                        frontier_table.put_item(
                            Item={
                                'url': task['url'],
                                'job_id': job_id,
                                'status': 'in_progress',
                                'last_crawled': datetime.now(timezone.utc).isoformat(),
                                'depth': task.get('depth', 0),
                                'crawler_id': self.crawler_id
                            }
                        )
                        logger.debug(f"Successfully put item in URL frontier for URL: {task['url']}")
                except Exception as e:
                    logger.error(f"Error updating URL frontier: {e}")
                    logger.error(traceback.format_exc())
                
                logger.info(f"Received task: {task['url']} (depth: {task.get('depth', 0)}, job_id: {job_id})")
                return task
            
            return None
        except Exception as e:
            logger.error(f"Error getting task: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def _process_task(self, task):
        """Process a crawl task using Scrapy."""
        url = task['url']
        # Use get() with a default value instead of direct access
        task_id = task.get('task_id', f"task-{uuid.uuid4()}")
        depth = task.get('depth', 0)
        max_depth = task.get('max_depth', 3)
        
        logger.info(f"Processing URL: {url} (task_id: {task_id}, depth: {depth}/{max_depth})")
        
        # Create a temporary directory for this crawl
        import tempfile
        temp_dir = tempfile.mkdtemp()
        logger.debug(f"Created temporary directory: {temp_dir}")
        
        try:
            # Use CrawlerRunner instead of CrawlerProcess
            from scrapy.crawler import CrawlerRunner
            from twisted.internet import reactor
            import crochet
            
            # Initialize crochet
            crochet.setup()
            logger.debug("Crochet setup complete")
            
            # Create a new CrawlerRunner for each task with reduced logging
            self.settings.update({'LOG_LEVEL': 'ERROR'})  # Only show errors, not info messages
            runner = CrawlerRunner(self.settings)
            logger.debug("CrawlerRunner created")
            
            # Define a callback to handle results after the crawl is complete
            def handle_spider_closed(spider):
                logger.info(f"Spider closed for URL: {url}")
                # Process the results from the spider
                if hasattr(spider, 'results') and spider.results:
                    logger.info(f"Spider returned {len(spider.results)} results")
                    for res in spider.results:
                        # Print only the parsed content
                        logger.info(f"Processing result for URL: {res['url']}")
                        content = res['content']
                        title = content.get('title', ['No title'])[0] if isinstance(content.get('title'), list) else content.get('title', 'No title')
                        logger.info(f"Title: {title}")
                        logger.info(f"Discovered URLs: {len(res['discovered_urls'])}")
                        
                        # Send the result to the master
                        logger.info(f"Sending result to master for URL: {res['url']}")
                        self._send_result(res, task)
                else:
                    logger.warning(f"Spider returned no results for URL: {url}")
            
            # Use crochet to run the crawl in a controlled way
            logger.info(f"Starting spider for URL: {url}")
            @crochet.wait_for(timeout=180)
            def run_spider():
                crawler = runner.create_crawler(WebSpider)
                crawler.signals.connect(handle_spider_closed, signal=signals.spider_closed)
                logger.debug(f"Spider signals connected for URL: {url}")
                return runner.crawl(crawler, url=url, task_id=task_id, depth=depth)
            
            # Run the spider with crochet
            run_spider()
            logger.info(f"Spider completed for URL: {url}")
            
        except Exception as e:
            logger.error(f"Error processing task for URL {url}: {e}")
            logger.error(traceback.format_exc())
            
            # Update URL frontier to mark as failed
            try:
                logger.info(f"Updating URL frontier to mark URL as failed: {url}")
                self._update_url_frontier_after_failure(url, task, str(e))
            except Exception as update_error:
                logger.error(f"Error updating URL frontier after failure: {update_error}")
                logger.error(traceback.format_exc())
                
        finally:
            # Clean up the temporary directory
            import shutil
            try:
                shutil.rmtree(temp_dir)
                logger.debug(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as e:
                logger.error(f"Error cleaning up temporary directory: {e}")
    
    def _send_result(self, result, task):
        """Send the crawl result to the master node."""
        try:
            # Get the URL and job_id from the task
            url = result.get('url')
            job_id = task.get('job_id', 'unknown')
            
            logger.info(f"Preparing result for URL: {url}, job_id: {job_id}")
            
            # Store the content in S3
            content = result.get('content', {})
            content_key = None
            if content:
                # Create a unique key for the content
                content_key = f"content/{job_id}/{uuid.uuid4()}.json"
                
                # Upload the content to S3
                logger.info(f"Uploading content to S3 for URL: {url}, key: {content_key}")
                self.s3.put_object(
                    Bucket=CRAWL_DATA_BUCKET,
                    Key=content_key,
                    Body=json.dumps(content),
                    ContentType='application/json'
                )
                logger.debug(f"Content uploaded to S3 for URL: {url}")
                
                # Add the content location to the metadata with job_id
                logger.info(f"Updating crawl metadata for URL: {url}")
                self.dynamodb.Table('crawl-metadata').put_item(
                    Item={
                        'url': url,
                        'job_id': job_id,  # Include job_id in the key
                        'content_location': content_key,
                        'title': content.get('title', ''),
                        'description': content.get('description', ''),
                        'crawled_at': datetime.now(timezone.utc).isoformat(),
                        'content_type': content.get('content_type', ''),
                        'language': content.get('language', ''),
                        'crawler_id': self.crawler_id
                    }
                )
                logger.debug(f"Crawl metadata updated for URL: {url}")
            
            # Prepare the result message
            message = {
                'type': 'result',
                'crawler_id': self.crawler_id,
                'task_id': result.get('task_id'),
                'url': url,
                'job_id': job_id,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'title': content.get('title', ''),
                'discovered_urls': result.get('discovered_urls', []),
                's3_key': content_key,
                'depth': result.get('depth', 0)
            }
            
            # Send the result to the master node
            logger.info(f"Sending result to master for URL: {url}")
            self.sqs.send_message(
                QueueUrl=self.crawl_result_queue_url,
                MessageBody=json.dumps(message)
            )
            logger.debug(f"Result sent to master for URL: {url}")
            
            # Update URL frontier to mark as completed
            logger.info(f"Updating URL frontier to mark URL as completed: {url}")
            self._update_url_frontier_after_completion(url, task)
            
            logger.info(f"Successfully processed and sent result for URL: {url}")
        except Exception as e:
            logger.error(f"Error sending result for URL {url}: {e}")
            logger.error(traceback.format_exc())
    def _update_url_frontier_after_completion(self, url, task):
        """
        Update the URL frontier after successfully completing a task.
        
        Args:
            url (str): The URL that was crawled
            task (dict): The task that was completed
        """
        try:
            frontier_table = self.dynamodb.Table('url-frontier')
            
            job_id = task.get('job_id')
            if not job_id:
                job_id = f"job-{uuid.uuid4()}"
                logger.warning(f"Missing job_id for completed URL: {url}, generated new one: {job_id}")
            
            update_expression = """
                SET #status = :status, 
                    completed_at = :completed_at, 
                    crawler_id = :crawler_id,
                    last_updated = :last_updated
            """
            
            frontier_table.update_item(
                Key={
                    'url': url,
                    'job_id': job_id
                },
                UpdateExpression=update_expression,
                ExpressionAttributeNames={
                    '#status': 'status'
                },
                ExpressionAttributeValues={
                    ':status': 'completed',
                    ':completed_at': datetime.now(timezone.utc).isoformat(),
                    ':crawler_id': self.crawler_id,
                    ':last_updated': datetime.now(timezone.utc).isoformat()
                },
                ReturnValues="ALL_NEW"
            )
            logger.info(f"Successfully updated URL frontier for completed URL: {url}")
        except self.dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            logger.error(f"Conditional check failed when updating URL {url}")
            raise
        except Exception as e:
            logger.error(f"Error updating URL frontier for completed URL {url}: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def _update_url_frontier_after_failure(self, url, task, error_message):
        """
        Update the URL frontier after failing to crawl a URL.
        
        Args:
            url (str): The URL that failed to crawl
            task (dict): The task that failed
            error_message (str): The error message
        """
        try:
            frontier_table = self.dynamodb.Table('url-frontier')
            
            job_id = task.get('job_id')
            if not job_id:
                job_id = f"job-{uuid.uuid4()}"
                logger.warning(f"Missing job_id for failed URL: {url}, generated new one: {job_id}")
            
            update_expression = """
                SET #status = :status,
                    failed_at = :failed_at,
                    error_message = :error_message,
                    crawler_id = :crawler_id,
                    retry_count = if_not_exists(retry_count, :zero) + :one,
                    last_updated = :last_updated
            """
            
            frontier_table.update_item(
                Key={
                    'url': url,
                    'job_id': job_id
                },
                UpdateExpression=update_expression,
                ExpressionAttributeNames={
                    '#status': 'status'
                },
                ExpressionAttributeValues={
                    ':status': 'failed',
                    ':failed_at': datetime.now(timezone.utc).isoformat(),
                    ':error_message': str(error_message)[:1024],  # Truncate long error messages
                    ':crawler_id': self.crawler_id,
                    ':zero': 0,
                    ':one': 1,
                    ':last_updated': datetime.now(timezone.utc).isoformat()
                },
                ReturnValues="ALL_NEW"
            )
            logger.info(f"Successfully updated URL frontier for failed URL: {url}")
        except self.dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            logger.error(f"Conditional check failed when updating URL {url}")
            raise
        except Exception as e:
            logger.error(f"Error updating URL frontier for failed URL {url}: {str(e)}")
            logger.error(traceback.format_exc())
            raise

if __name__ == "__main__":
    # Create and start a crawler node
    crawler = CrawlerNode()
    try:
        crawler.start()
    except KeyboardInterrupt:
        crawler.stop()
        
        # Don't try to update URL frontier on shutdown as 'task' might not be defined
        print("Crawler stopped by user")
        
        # After processing is complete, update URL frontier
        try:
            # Use crawler.dynamodb instead of self.dynamodb
            frontier_table = crawler.dynamodb.Table('url-frontier')
            
            # Update the URL status to completed
            frontier_table.update_item(
                Key={'url':    task['url'],
                     'job_id': task['job_id']
                },
                UpdateExpression="SET #status = :status, last_crawled = :timestamp",
                ExpressionAttributeNames={
                    '#status': 'status'
                },
                ExpressionAttributeValues={
                    ':status': 'completed',
                    ':timestamp': datetime.now(timezone.utc).isoformat()
                }
            )
        except Exception as e:
            print(f"Error updating URL frontier after completion: {e}")
            
        # Update URL frontier to mark as failed
        try:
            # Use crawler.dynamodb instead of self.dynamodb
            frontier_table = crawler.dynamodb.Table('url-frontier')
            
            # Update the URL status to failed
            frontier_table.update_item(
                Key={'url': task['url']},
                UpdateExpression="SET #status = :status, last_crawled = :timestamp",
                ExpressionAttributeNames={
                    '#status': 'status'
                },
                ExpressionAttributeValues={
                    ':status': 'failed',
                    ':timestamp': datetime.now(timezone.utc).isoformat()
                }
            )
        except Exception as e_inner:
            print(f"Error updating URL frontier after failure: {e_inner}")
    
    