import unittest
import time
import threading
from unittest.mock import Mock, patch
import boto3
import sys
import os
from datetime import datetime, timezone, timedelta
import uuid
import json
from moto import mock_sqs, mock_dynamodb, mock_s3
from botocore.exceptions import ClientError

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Import the modules from src
from src.crawler.crawler_node import CrawlerNode
from src.master.master_node import MasterNode
from src.indexer.indexer_node import WhooshIndex

def wait_for_table_status(dynamodb, table_name, status='ACTIVE', timeout=30):
    """Wait for a DynamoDB table to reach a specific status."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = dynamodb.meta.client.describe_table(TableName=table_name)
            if response['Table']['TableStatus'] == status:
                return True
        except ClientError:
            if status == 'DELETED':
                return True
        time.sleep(1)
    return False

def create_test_table(dynamodb, table_name, key_schema, attribute_definitions):
    """Create a DynamoDB table with proper waiting."""
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        wait_for_table_status(dynamodb, table_name)
        return table
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            # Table is being created, wait for it
            wait_for_table_status(dynamodb, table_name)
            return dynamodb.Table(table_name)
        raise

def delete_test_table(dynamodb, table_name):
    """Delete a DynamoDB table with proper waiting."""
    try:
        table = dynamodb.Table(table_name)
        table.delete()
        wait_for_table_status(dynamodb, table_name, status='DELETED')
    except ClientError as e:
        if e.response['Error']['Code'] != 'ResourceNotFoundException':
            raise

class TestFaultTolerance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment once for all tests."""
        cls.aws_region = 'us-east-1'
        cls.test_tables = ['url-frontier', 'crawler-status']
        cls.test_queues = [
            'crawl-task-queue',
            'crawl-result-queue',
            'index-task-queue',
            'index-result-queue'
        ]
        cls.test_bucket = 'crawl-results-test'

    def setUp(self):
        """Set up test environment for each test."""
        # Initialize AWS clients
        self.sqs = boto3.client('sqs', region_name=self.aws_region)
        self.dynamodb = boto3.resource('dynamodb', region_name=self.aws_region)
        self.s3 = boto3.client('s3', region_name=self.aws_region)

        # Clean up any existing resources
        self._cleanup_resources()

        # Create test resources
        self._create_test_resources()

        # Initialize system components
        self.master = MasterNode()
        self.crawler = CrawlerNode()
        self.indexer = WhooshIndex()

    def _cleanup_resources(self):
        """Clean up test resources."""
        # Delete tables
        for table_name in self.test_tables:
            delete_test_table(self.dynamodb, table_name)

        # Delete queues
        for queue_name in self.test_queues:
            try:
                queue_url = self.sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
                self.sqs.delete_queue(QueueUrl=queue_url)
            except ClientError:
                pass

        # Delete S3 bucket
        try:
            self.s3.delete_bucket(Bucket=self.test_bucket)
        except ClientError:
            pass

    def _create_test_resources(self):
        """Create test resources."""
        # Create tables
        self.url_frontier = create_test_table(
            self.dynamodb,
            'url-frontier',
            [{'AttributeName': 'url', 'KeyType': 'HASH'}],
            [{'AttributeName': 'url', 'AttributeType': 'S'}]
        )

        self.crawler_status = create_test_table(
            self.dynamodb,
            'crawler-status',
            [{'AttributeName': 'crawler_id', 'KeyType': 'HASH'}],
            [{'AttributeName': 'crawler_id', 'AttributeType': 'S'}]
        )

        # Create queues
        self.queues = {}
        for queue_name in self.test_queues:
            self.queues[queue_name] = self.sqs.create_queue(QueueName=queue_name)

        # Create S3 bucket
        try:
            self.s3.create_bucket(Bucket=self.test_bucket)
        except ClientError as e:
            if e.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
                raise

    def test_crawler_node_failure(self):
        """Test crawler node failure and recovery."""
        # Start crawler node
        crawler_thread = threading.Thread(target=self.crawler.run)
        crawler_thread.daemon = True
        crawler_thread.start()
        
        # Wait for crawler to start
        time.sleep(2)
        
        # Verify crawler is active
        response = self.crawler_status.get_item(
            Key={'crawler_id': self.crawler.crawler_id}
        )
        self.assertEqual(response['Item']['status'], 'active')
        
        # Simulate crawler failure
        self.crawler.stop()
        time.sleep(2)
        
        # Verify crawler is marked as inactive
        response = self.crawler_status.get_item(
            Key={'crawler_id': self.crawler.crawler_id}
        )
        self.assertEqual(response['Item']['status'], 'inactive')
        
        # Restart crawler
        self.crawler = CrawlerNode()
        crawler_thread = threading.Thread(target=self.crawler.run)
        crawler_thread.daemon = True
        crawler_thread.start()
        
        # Verify crawler is active again
        time.sleep(2)
        response = self.crawler_status.get_item(
            Key={'crawler_id': self.crawler.crawler_id}
        )
        self.assertEqual(response['Item']['status'], 'active')

    def test_task_timeout_handling(self):
        """Test handling of task timeouts."""
        # Add a task that will timeout
        task_id = str(uuid.uuid4())
        job_id = str(uuid.uuid4())
        
        self.sqs.send_message(
            QueueUrl=self.queues['crawl-task-queue']['QueueUrl'],
            MessageBody=json.dumps({
                'url': 'http://slow-site.com',
                'depth': 1,
                'task_id': task_id,
                'job_id': job_id,
                'timestamp': (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
            })
        )
        
        # Start crawler with short timeout
        self.crawler.task_timeout = 2
        crawler_thread = threading.Thread(target=self.crawler.run)
        crawler_thread.daemon = True
        crawler_thread.start()
        
        # Wait for timeout
        time.sleep(5)
        
        # Verify task is marked as failed
        response = self.url_frontier.get_item(
            Key={'url': 'http://slow-site.com'}
        )
        self.assertIn('Item', response)
        self.assertEqual(response['Item']['status'], 'failed')
        self.assertIn('timeout', response['Item'].get('error', '').lower())

    def test_network_failure_recovery(self):
        """Test recovery from network failures."""
        # Simulate network failure
        with patch('boto3.client') as mock_client:
            mock_client.side_effect = Exception('Network error')
            
            # Attempt to process task
            task = {'url': 'http://test.com', 'depth': 1}
            self.crawler._process_task(task)
            
            # Verify error is handled
            response = self.url_frontier.get_item(
                Key={'url': 'http://test.com'}
            )
            self.assertIn('Item', response)
            self.assertEqual(response['Item']['status'], 'failed')
            self.assertIn('network', response['Item'].get('error', '').lower())

    def test_concurrent_failures(self):
        """Test handling of multiple concurrent failures."""
        # Start multiple crawler nodes
        crawlers = []
        for _ in range(3):
            crawler = CrawlerNode()
            crawlers.append(crawler)
            thread = threading.Thread(target=crawler.run)
            thread.daemon = True
            thread.start()
        
        # Wait for crawlers to start
        time.sleep(2)
        
        # Simulate various failures
        crawlers[0].stop()  # Node failure
        crawlers[1]._process_task({'url': 'http://timeout.com', 'depth': 1})  # Timeout
        crawlers[2]._process_task({'url': 'http://error.com', 'depth': 1})  # Error
        
        # Verify all failures are handled
        time.sleep(2)
        for crawler in crawlers:
            response = self.crawler_status.get_item(
                Key={'crawler_id': crawler.crawler_id}
            )
            self.assertIn(response['Item']['status'], ['active', 'inactive', 'error'])

    def test_master_node_recovery(self):
        """Test master node failure and recovery."""
        # Start a crawl
        test_urls = ["https://example.com"]
        crawl_id = self.master.start_crawl(test_urls, max_depth=1)
        
        # Wait for crawl to start
        time.sleep(2)
        
        # Simulate master node failure
        self.master.stop()
        
        # Create new master node
        new_master = MasterNode()
        
        # Verify crawl state is recovered
        time.sleep(2)
        crawl_status = new_master.get_crawl_status(crawl_id)
        self.assertIsNotNone(crawl_status)
        self.assertIn('status', crawl_status)
        
        # Resume crawl
        new_master.resume_crawl(crawl_id)
        time.sleep(2)
        
        # Verify crawl continues
        frontier_items = self.url_frontier.scan()['Items']
        self.assertGreater(len(frontier_items), 0)

    def test_data_persistence(self):
        """Test data persistence across node failures."""
        # Add test content to S3
        test_content = "Test page content"
        content_key = f"test-content-{uuid.uuid4()}"
        
        self.s3.put_object(
            Bucket=self.test_bucket,
            Key=content_key,
            Body=test_content
        )
        
        # Add result to queue
        result = {
            'url': 'https://example.com/persistence-test',
            'content_key': content_key,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        self.sqs.send_message(
            QueueUrl=self.queues['crawl-result-queue']['QueueUrl'],
            MessageBody=json.dumps(result)
        )
        
        # Process result
        self.indexer.process_next_result()
        
        # Simulate indexer failure and restart
        self.indexer = WhooshIndex()
        
        # Verify data persistence
        search_results = self.indexer.search('test')
        self.assertGreater(len(search_results), 0)
        self.assertIn('https://example.com/persistence-test', 
                     [r.get('url') for r in search_results])

    def tearDown(self):
        """Clean up after each test."""
        # Stop all components
        if hasattr(self, 'master'):
            self.master.stop()
        if hasattr(self, 'crawler'):
            self.crawler.stop()
        
        # Clean up resources
        self._cleanup_resources()

if __name__ == '__main__':
    unittest.main() 