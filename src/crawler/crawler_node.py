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
        # Use ItemLoader for better data extraction
        loader = ItemLoader(item=WebPage())
        
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


class CrawlerNode:
    """
    Crawler Node class that fetches and processes web pages.
    """
    def __init__(self, crawler_id=None):
        self.crawler_id = crawler_id or f"crawler-{uuid.uuid4()}"
        # Specify the region when creating the client
        self.sqs = boto3.client('sqs', region_name='us-east-1')  # Use the region from your config
        
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
            'LOG_LEVEL': 'INFO'
        })
        
        # Flag to control the crawler
        self.running = False
        
        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeats)
        self.heartbeat_thread.daemon = True
    
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
        """Get a task from the crawl task queue."""
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
                
                print(f"Received task: {task['url']}")
                return task
            
            return None
        except Exception as e:
            print(f"Error getting task: {e}")
            return None
    
    def _process_task(self, task):
        """Process a crawl task using Scrapy."""
        url = task['url']
        task_id = task['task_id']
        depth = task.get('depth', 0)
        max_depth = task.get('max_depth', 3)
        
        print(f"Processing URL: {url} (depth: {depth}/{max_depth})")
        
        # Create a new CrawlerProcess for each task
        process = CrawlerProcess(self.settings)
        
        # Define a callback to handle results after the crawl is complete
        def handle_spider_closed(spider):
            # Send results back to the master
            for result in spider.results:
                self._send_result(result, task)
        
        # Create a spider instance with the correct parameters
        spider = WebSpider(url=url, task_id=task_id, depth=depth)
        
        # Add a callback for when the spider closes
        process.crawl(WebSpider, url=url, task_id=task_id, depth=depth)
        
        # Run the spider
        process.start()
    
    def _send_result(self, result, original_task):
        """Send crawl results back to the master node."""
        # Add crawler ID and original task parameters to the result
        result['crawler_id'] = self.crawler_id
        result['max_depth'] = original_task.get('max_depth', 3)
        result['max_urls_per_domain'] = original_task.get('max_urls_per_domain', 100)
        
        try:
            self.sqs.send_message(
                QueueUrl=self.crawl_result_queue_url,
                MessageBody=json.dumps(result)
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