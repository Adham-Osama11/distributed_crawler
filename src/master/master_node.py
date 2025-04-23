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

# Add the parent directory to the path so we can import common modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import (
    CRAWL_TASK_QUEUE, CRAWL_RESULT_QUEUE, 
    INDEX_TASK_QUEUE, HEARTBEAT_INTERVAL
)
from common.utils import get_domain, normalize_url

class MasterNode:
    def __init__(self):
        # Initialize AWS clients
        self.sqs = boto3.client('sqs')
        
        # Create queues if they don't exist
        self._create_queues()
        
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
    
    def start_crawl(self, seed_urls, max_depth=3, max_urls_per_domain=100):
        """Start a new crawl with the given seed URLs."""
        print(f"Starting crawl with {len(seed_urls)} seed URLs")
        
        # Normalize seed URLs
        normalized_urls = [normalize_url(url) for url in seed_urls]
        
        # Create crawl tasks for seed URLs
        for url in normalized_urls:
            self._enqueue_url(url, depth=0, max_depth=max_depth, 
                             max_urls_per_domain=max_urls_per_domain)
    
    def _enqueue_url(self, url, depth, max_depth, max_urls_per_domain):
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
        task = {
            'task_id': task_id,
            'url': url,
            'depth': depth,
            'max_depth': max_depth,
            'max_urls_per_domain': max_urls_per_domain,
            'timestamp': time.time()
        }
        
        # Send task to SQS
        try:
            self.sqs.send_message(
                QueueUrl=self.crawl_task_queue_url,
                MessageBody=json.dumps(task)
            )
            print(f"Enqueued URL: {url} (depth: {depth})")
            return True
        except Exception as e:
            print(f"Error enqueuing URL {url}: {e}")
            # Remove URL from in-progress set if enqueuing fails
            with self.lock:
                self.urls_in_progress.remove(url)
                self.domain_url_counts[domain] -= 1
            return False
    
    def process_crawl_results(self):
        """Process crawl results from the result queue."""
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
                    
                    # Update crawler heartbeat
                    crawler_id = result.get('crawler_id')
                    if crawler_id:
                        with self.lock:
                            self.active_crawlers[crawler_id] = time.time()
                    
                    # Process the crawled URL
                    url = result.get('url')
                    if url:
                        with self.lock:
                            if url in self.urls_in_progress:
                                self.urls_in_progress.remove(url)
                                self.urls_completed.add(url)
                    
                    # Forward content to indexer
                    if 'content' in result and result['content']:
                        self._send_to_indexer(result)
                    
                    # Process discovered URLs
                    if 'discovered_urls' in result and result['discovered_urls']:
                        depth = result.get('depth', 0) + 1
                        max_depth = result.get('max_depth', 3)
                        max_urls_per_domain = result.get('max_urls_per_domain', 100)
                        
                        for discovered_url in result['discovered_urls']:
                            self._enqueue_url(
                                discovered_url, depth, max_depth, max_urls_per_domain
                            )
                    
                    # Delete the processed message
                    self.sqs.delete_message(
                        QueueUrl=self.crawl_result_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                except Exception as e:
                    print(f"Error processing crawl result: {e}")
        except Exception as e:
            print(f"Error receiving crawl results: {e}")
    
    def _send_to_indexer(self, result):
        """Send crawled content to the indexer."""
        index_task = {
            'url': result['url'],
            'content': result['content'],
            'timestamp': time.time()
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
                    
                    # TODO: Reassign tasks from failed crawlers
                    # This would require tracking which crawler is handling which URLs
    
    def run(self):
        """Run the master node main loop."""
        print("Master node running...")
        try:
            while True:
                # Process crawl results
                self.process_crawl_results()
                
                # Sleep briefly to avoid tight loop
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("Master node shutting down...")

if __name__ == "__main__":
    master = MasterNode()
    
    # Example: Start a crawl with some seed URLs
    seed_urls = [
        "https://example.com",
        "https://wikipedia.org"
    ]
    
    master.start_crawl(seed_urls)
    master.run()