import unittest
import time
import threading
from unittest.mock import Mock, patch
import boto3
from moto import mock_sqs, mock_dynamodb, mock_s3
from src.crawler.crawler_node import CrawlerNode
from src.master.master_node import MasterNode
from src.indexer.indexer_node import WhooshIndex
from botocore.exceptions import ClientError

def create_table_if_not_exists(dynamodb, table_name, key_schema, attribute_definitions, throughput):
    try:
        table = dynamodb.Table(table_name)
        table.load()  # Will raise if table does not exist
        print(f"Table {table_name} already exists.")
        return table
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"Creating table {table_name}...")
            return dynamodb.create_table(
                TableName=table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attribute_definitions,
                ProvisionedThroughput=throughput
            )
        else:
            raise

def delete_table_if_exists(dynamodb, table_name):
    try:
        table = dynamodb.Table(table_name)
        table.delete()
        table.wait_until_not_exists()
    except ClientError as e:
        if e.response['Error']['Code'] != 'ResourceNotFoundException':
            raise

class TestFaultTolerance(unittest.TestCase):
    def setUp(self):
        """Set up test environment."""
        self.aws_region = 'us-east-1'
        self.sqs = boto3.client('sqs', region_name=self.aws_region)
        self.dynamodb = boto3.resource('dynamodb', region_name=self.aws_region)
        self.s3 = boto3.client('s3', region_name=self.aws_region)
        
        # Create test queues
        self.crawl_task_queue = self.sqs.create_queue(QueueName='crawl-task-queue')
        self.crawl_result_queue = self.sqs.create_queue(QueueName='crawl-result-queue')
        self.index_task_queue = self.sqs.create_queue(QueueName='index-task-queue')
        self.index_result_queue = self.sqs.create_queue(QueueName='index-result-queue')
        
        # Delete and recreate test tables
        delete_table_if_exists(self.dynamodb, 'url-frontier')
        self.url_frontier = self.dynamodb.create_table(
            TableName='url-frontier',
            KeySchema=[{'AttributeName': 'url', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'url', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        
        self.crawler_status = self.dynamodb.create_table(
            TableName='crawler-status',
            KeySchema=[{'AttributeName': 'crawler_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'crawler_id', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        
        # Create test bucket
        self.s3.create_bucket(Bucket='crawl-results')
        
        # Initialize nodes
        self.master = MasterNode()
        self.crawler = CrawlerNode()
        self.indexer = WhooshIndex()

    def test_crawler_node_failure(self):
        """Test crawler node failure and recovery."""
        # Start crawler node
        crawler_thread = threading.Thread(target=self.crawler.run)
        crawler_thread.start()
        
        # Simulate crawler failure
        time.sleep(2)
        self.crawler.stop()
        
        # Verify crawler is marked as inactive
        time.sleep(2)
        response = self.dynamodb.Table('crawler-status').get_item(
            Key={'crawler_id': self.crawler.crawler_id}
        )
        self.assertEqual(response['Item']['status'], 'inactive')
        
        # Restart crawler
        self.crawler = CrawlerNode()
        crawler_thread = threading.Thread(target=self.crawler.run)
        crawler_thread.start()
        
        # Verify crawler is marked as active
        time.sleep(2)
        response = self.dynamodb.Table('crawler-status').get_item(
            Key={'crawler_id': self.crawler.crawler_id}
        )
        self.assertEqual(response['Item']['status'], 'active')

    def test_task_timeout_handling(self):
        """Test task timeout handling."""
        # Add a task that will timeout
        self.sqs.send_message(
            QueueUrl=self.crawl_task_queue['QueueUrl'],
            MessageBody='{"url": "http://slow-site.com", "depth": 1}'
        )
        
        # Start crawler with short timeout
        self.crawler.task_timeout = 2
        crawler_thread = threading.Thread(target=self.crawler.run)
        crawler_thread.start()
        
        # Wait for timeout
        time.sleep(3)
        
        # Verify task is marked as failed
        response = self.dynamodb.Table('url-frontier').get_item(
            Key={'url': 'http://slow-site.com'}
        )
        self.assertEqual(response['Item']['status'], 'failed')
        self.assertEqual(response['Item']['error'], 'timeout')

    def test_network_failure_recovery(self):
        """Test recovery from network failures."""
        # Simulate network failure
        with patch('boto3.client') as mock_client:
            mock_client.side_effect = Exception('Network error')
            
            # Attempt to process task
            self.crawler._process_task({'url': 'http://test.com'})
            
            # Verify error is handled
            self.assertIn('Network error', self.crawler.failed_tasks['http://test.com']['error'])

    def test_disk_space_recovery(self):
        """Test recovery from disk space issues."""
        # Simulate low disk space
        with patch('psutil.disk_usage') as mock_disk:
            mock_disk.return_value = type('obj', (object,), {
                'percent': 95,
                'free': 1000000
            })
            
            # Attempt to process task
            self.crawler._process_task({'url': 'http://test.com'})
            
            # Verify error is handled
            self.assertIn('disk space', self.crawler.failed_tasks['http://test.com']['error'])

    def test_memory_usage_recovery(self):
        """Test recovery from high memory usage."""
        # Simulate high memory usage
        with patch('psutil.virtual_memory') as mock_memory:
            mock_memory.return_value = type('obj', (object,), {
                'percent': 95,
                'available': 1000000
            })
            
            # Attempt to process task
            self.crawler._process_task({'url': 'http://test.com'})
            
            # Verify error is handled
            self.assertIn('memory', self.crawler.failed_tasks['http://test.com']['error'])

    def test_concurrent_failures(self):
        """Test handling of multiple concurrent failures."""
        # Start multiple crawler nodes
        crawlers = []
        for _ in range(3):
            crawler = CrawlerNode()
            crawlers.append(crawler)
            threading.Thread(target=crawler.run).start()
        
        # Simulate various failures
        time.sleep(2)
        crawlers[0].stop()  # Node failure
        crawlers[1]._process_task({'url': 'http://timeout.com'})  # Timeout
        crawlers[2]._process_task({'url': 'http://error.com'})  # Error
        
        # Verify all failures are handled
        time.sleep(2)
        for crawler in crawlers:
            response = self.dynamodb.Table('crawler-status').get_item(
                Key={'crawler_id': crawler.crawler_id}
            )
            self.assertIn(response['Item']['status'], ['active', 'inactive', 'error'])

    def tearDown(self):
        """Clean up test environment."""
        # Delete test queues
        self.sqs.delete_queue(QueueUrl=self.crawl_task_queue['QueueUrl'])
        self.sqs.delete_queue(QueueUrl=self.crawl_result_queue['QueueUrl'])
        self.sqs.delete_queue(QueueUrl=self.index_task_queue['QueueUrl'])
        self.sqs.delete_queue(QueueUrl=self.index_result_queue['QueueUrl'])
        
        # Delete test tables
        self.url_frontier.delete()
        self.crawler_status.delete()
        
        # Delete test bucket
        self.s3.delete_bucket(Bucket='crawl-results')

if __name__ == '__main__':
    unittest.main() 