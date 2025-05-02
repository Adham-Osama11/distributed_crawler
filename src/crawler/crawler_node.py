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

# Add the parent directory to the path so we can import common modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import (
    CRAWL_TASK_QUEUE, CRAWL_RESULT_QUEUE, 
    HEARTBEAT_INTERVAL
)
from common.utils import get_domain, normalize_url

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
        # Specify the region when creating the client
        self.sqs = boto3.client('sqs', region_name='us-east-1')  # Use the region from your config
        
        # Initialize S3 and DynamoDB clients
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        
        # Get queue URLs
        response = self.sqs.get_queue_url(QueueName=CRAWL_TASK_QUEUE)
        self.crawl_task_queue_url = response['QueueUrl']
        
        response = self.sqs.get_queue_url(QueueName=CRAWL_RESULT_QUEUE)
        self.crawl_result_queue_url = response['QueueUrl']
        
        # Initialize Scrapy settings
        self.settings = get_project_settings()
        self.settings.update({
            'USER_AGENT': 'DistributedCrawler/1.0',
            'ROBOTSTXT_OBEY': True,
            'DOWNLOAD_DELAY': 1.0,  # Basic politeness - 1 second delay
            'CONCURRENT_REQUESTS': 1,  # Start with just 1 concurrent request
            'LOG_LEVEL': 'ERROR',  # Only show errors, not info messages
            'LOG_ENABLED': False,  # Disable logging completely if you want no output
        })
        
        # Flag to control the crawler
        self.running = False
        
        # Ensure DynamoDB tables exist
        self._ensure_dynamodb_tables()
        
        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeats)
        self.heartbeat_thread.daemon = True
    
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
        
        # Register with master node
        self._register_with_master()
        
        # Main crawling loop
        while self.running:
            try:
                # Get a task from the queue
                task = self._get_task()
                
                if task:
                    # Process the task
                    self._process_task(task)
                else:
                    # No tasks available, wait a bit
                    time.sleep(1)
            except Exception as e:
                print(f"Error in crawler main loop: {e}")
                import traceback
                print(traceback.format_exc())  # Print the full stack trace
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
            'crawler_id': self.crawler_id,
            'timestamp': time.time(),
            'status': 'active'
        }
        
        try:
            self.sqs.send_message(
                QueueUrl=self.crawl_result_queue_url,
                MessageBody=json.dumps(heartbeat)
            )
        except Exception as e:
            print(f"Error sending heartbeat: {e}")
    
    def _get_task(self):
        """Get a task from the crawl task queue and update URL frontier."""
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.crawl_task_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=5
            )
            
            if 'Messages' in response and response['Messages']:
                message = response['Messages'][0]
                task = json.loads(message['Body'])
                
                # Delete the message from the queue
                self.sqs.delete_message(
                    QueueUrl=self.crawl_task_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                
                # Update URL frontier in DynamoDB
                try:
                    # First check if the table exists
                    try:
                        frontier_table = self.dynamodb.Table('url-frontier')
                        
                        # Ensure job_id is present in the task
                        job_id = task.get('job_id')
                        if not job_id:
                            job_id = f"job-{uuid.uuid4()}"
                            task['job_id'] = job_id
                        
                        # Try to update the URL status
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
                    except Exception as e:
                        # If update fails, try to put the item instead
                        if "ValidationException" in str(e):
                            print(f"Trying to put item instead of update for URL: {task['url']}")
                            # Ensure there's always a job_id
                            job_id = task.get('job_id')
                            if not job_id:
                                job_id = f"job-{uuid.uuid4()}"
                                task['job_id'] = job_id
                    
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
                except Exception as e:
                    print(f"Error updating URL frontier: {e}")
                
                print(f"Received task: {task['url']}")
                return task
            
            return None
        except Exception as e:
            print(f"Error getting task: {e}")
            return None
    
    def _process_task(self, task):
        """Process a crawl task using Scrapy."""
        url = task['url']
        # Use get() with a default value instead of direct access
        task_id = task.get('task_id', f"task-{uuid.uuid4()}")
        depth = task.get('depth', 0)
        max_depth = task.get('max_depth', 3)
        
        print(f"Processing URL: {url} (depth: {depth}/{max_depth})")
        
        # Create a temporary directory for this crawl
        import tempfile
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Use CrawlerRunner instead of CrawlerProcess
            from scrapy.crawler import CrawlerRunner
            from twisted.internet import reactor
            import crochet
            
            # Initialize crochet
            crochet.setup()
            
            # Create a new CrawlerRunner for each task with reduced logging
            self.settings.update({'LOG_LEVEL': 'ERROR'})  # Only show errors, not info messages
            runner = CrawlerRunner(self.settings)
            
            # Define a callback to handle results after the crawl is complete
            def handle_spider_closed(spider):
                # Process the results from the spider
                if hasattr(spider, 'results') and spider.results:
                    for res in spider.results:
                        # Print only the parsed content
                        print("\n--- PARSED CONTENT ---")
                        print(f"URL: {res['url']}")
                        content = res['content']
                        print(f"Title: {content.get('title', ['No title'])[0] if isinstance(content.get('title'), list) else content.get('title', 'No title')}")
                        print(f"Description: {content.get('description', ['No description'])[0] if isinstance(content.get('description'), list) else content.get('description', 'No description')}")
                        print(f"Keywords: {content.get('keywords', ['No keywords'])[0] if isinstance(content.get('keywords'), list) else content.get('keywords', 'No keywords')}")
                        print(f"Language: {content.get('language', ['Not specified'])[0] if isinstance(content.get('language'), list) else content.get('language', 'Not specified')}")
                        print(f"Content Type: {content.get('content_type', 'Unknown')}")
                        print(f"Discovered URLs: {len(res['discovered_urls'])}")
                        print("--------------------\n")
                        
                        # Send the result to the master
                        self._send_result(res, task)
            
            # Use crochet to run the crawl in a controlled way
            @crochet.wait_for(timeout=180)
            def run_spider():
                crawler = runner.create_crawler(WebSpider)
                crawler.signals.connect(handle_spider_closed, signal=signals.spider_closed)
                return runner.crawl(crawler, url=url, task_id=task_id, depth=depth)
            
            # Run the spider with crochet
            run_spider()
            
        except Exception as e:
            print(f"Error processing task: {e}")
            import traceback
            print(traceback.format_exc())
        finally:
            # Clean up the temporary directory
            import shutil
            try:
                shutil.rmtree(temp_dir)
            except:
                pass
    
    def _send_result(self, result, original_task):
        """Send crawl results back to the master node and store in S3/DynamoDB."""
        # Add crawler ID and original task parameters to the result
        result['crawler_id'] = self.crawler_id
        result['max_depth'] = original_task.get('max_depth', 3)
        result['max_urls_per_domain'] = original_task.get('max_urls_per_domain', 100)
        
        # Store raw HTML content in S3
        s3_client = boto3.client('s3')
        s3_bucket = 'web-crawl-storage'
        s3_raw_path = f"raw-content/{result['task_id']}/{uuid.uuid4()}.html"
        
        try:
            # Store the raw HTML in S3
            if 'content' in result and 'html' in result['content']:
                html_content = result['content']['html']
                
                # Convert to string if it's a list or other type
                if isinstance(html_content, list):
                    html_content = ''.join(html_content)
                elif not isinstance(html_content, str):
                    html_content = str(html_content)
                
                s3_client.put_object(
                    Bucket=s3_bucket,
                    Key=s3_raw_path,
                    Body=html_content,
                    ContentType='text/html'
                )
                
                # Update the result with S3 path
                result['s3_raw_path'] = s3_raw_path
                
                # Remove the full HTML content which is likely causing the size issue
                del result['content']['html']
        except Exception as e:
            print(f"Error storing content in S3: {e}")
        
        # Update DynamoDB with crawl metadata
        try:
            dynamodb = boto3.resource('dynamodb')
            metadata_table = dynamodb.Table('crawl-metadata')
            
            # Get job_id from the original task or generate a new one
            job_id = original_task.get('job_id', f"job-{uuid.uuid4()}")
            
            metadata_table.put_item(
                Item={
                    'url': result['url'],
                    'job_id': job_id,  # Add the required job_id field
                    'title': result['content'].get('title', ''),
                    'description': result['content'].get('description', ''),
                    'content_type': result['content'].get('content_type', ''),
                    'language': result['content'].get('language', ''),
                    'crawl_time': datetime.now(timezone.utc).isoformat(),
                    's3_raw_path': s3_raw_path,
                    'http_status': 200,  # Assuming success since we have content
                    'crawler_id': self.crawler_id
                }
            )
        except Exception as e:
            print(f"Error updating DynamoDB: {e}")
        
        # Create a copy of the result to modify for sending
        result_to_send = result.copy()
        
        # Send the message to SQS
        message_body = json.dumps(result_to_send)
        message_size = len(message_body.encode('utf-8'))
        
        # Truncate text content if it's too large
        if 'content' in result_to_send and 'text' in result_to_send['content']:
            text_content = result_to_send['content']['text']
            if isinstance(text_content, list) and len(text_content) > 100:
                # Keep only the first 100 text elements
                result_to_send['content']['text'] = text_content[:100]
                result_to_send['content']['text_truncated'] = True
        
        # Limit the number of discovered URLs if there are too many
        if 'discovered_urls' in result_to_send and len(result_to_send['discovered_urls']) > 100:
            result_to_send['discovered_urls'] = result_to_send['discovered_urls'][:100]
            result_to_send['discovered_urls_truncated'] = True
        
        try:
            # Convert to JSON and check size
            message_body = json.dumps(result_to_send)
            message_size = len(message_body.encode('utf-8'))
            
            # If still too large, take more drastic measures
            if message_size > 250000:  # Leave some margin below the 262144 limit
                print(f"Warning: Message size ({message_size} bytes) is still large. Taking additional measures.")
                
                # Keep only essential data
                minimal_result = {
                    'crawler_id': result_to_send['crawler_id'],
                    'task_id': result_to_send['task_id'],
                    'url': result_to_send['url'],
                    'depth': result_to_send['depth'],
                    'max_depth': result_to_send['max_depth'],
                    'max_urls_per_domain': result_to_send['max_urls_per_domain'],
                    'content': {
                        'url': result_to_send['content'].get('url'),
                        'title': result_to_send['content'].get('title'),
                        'description': result_to_send['content'].get('description'),
                    }
                }
                
                # Include a subset of discovered URLs
                if 'discovered_urls' in result_to_send:
                    minimal_result['discovered_urls'] = result_to_send['discovered_urls'][:20]
                    minimal_result['discovered_urls_truncated'] = True
                
                # Use the minimal result instead
                message_body = json.dumps(minimal_result)
                print(f"Reduced message size from {message_size} to {len(message_body.encode('utf-8'))} bytes")
            
            # Send the message
            self.sqs.send_message(
                QueueUrl=self.crawl_result_queue_url,
                MessageBody=message_body
            )
            print(f"Sent result for URL: {result['url']}")
        except Exception as e:
            print(f"Error sending result: {e}")


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