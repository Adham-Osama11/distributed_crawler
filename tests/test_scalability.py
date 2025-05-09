import unittest
import time
import threading
import statistics
from concurrent.futures import ThreadPoolExecutor
import boto3
from moto import mock_sqs, mock_dynamodb, mock_s3
from src.crawler.crawler_node import CrawlerNode
from src.master.master_node import MasterNode
from src.indexer.indexer_node import WhooshIndex

class TestScalability(unittest.TestCase):
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
        
        # Create test tables
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
        
        # Initialize master node
        self.master = MasterNode()

    def test_node_scaling(self):
        """Test system performance with varying number of nodes."""
        node_counts = [1, 2, 4, 8]
        results = {}
        
        for node_count in node_counts:
            # Start nodes
            crawlers = []
            for _ in range(node_count):
                crawler = CrawlerNode()
                crawlers.append(crawler)
                threading.Thread(target=crawler.run).start()
            
            # Add test tasks
            test_urls = [f'http://test{i}.com' for i in range(100)]
            for url in test_urls:
                self.sqs.send_message(
                    QueueUrl=self.crawl_task_queue['QueueUrl'],
                    MessageBody=f'{{"url": "{url}", "depth": 1}}'
                )
            
            # Measure performance
            start_time = time.time()
            processed_urls = 0
            while processed_urls < len(test_urls) and time.time() - start_time < 300:  # 5-minute timeout
                response = self.sqs.receive_message(
                    QueueUrl=self.crawl_result_queue['QueueUrl'],
                    MaxNumberOfMessages=10
                )
                if 'Messages' in response:
                    processed_urls += len(response['Messages'])
                time.sleep(1)
            
            end_time = time.time()
            duration = end_time - start_time
            urls_per_second = processed_urls / duration
            
            results[node_count] = {
                'duration': duration,
                'urls_per_second': urls_per_second,
                'processed_urls': processed_urls
            }
            
            # Stop nodes
            for crawler in crawlers:
                crawler.stop()
        
        # Analyze results
        self._analyze_scaling_results(results)

    def test_concurrent_task_processing(self):
        """Test system performance with concurrent task processing."""
        # Start multiple crawler nodes
        node_count = 4
        crawlers = []
        for _ in range(node_count):
            crawler = CrawlerNode()
            crawlers.append(crawler)
            threading.Thread(target=crawler.run).start()
        
        # Add concurrent tasks
        task_count = 100
        test_urls = [f'http://test{i}.com' for i in range(task_count)]
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for url in test_urls:
                future = executor.submit(
                    self.sqs.send_message,
                    QueueUrl=self.crawl_task_queue['QueueUrl'],
                    MessageBody=f'{{"url": "{url}", "depth": 1}}'
                )
                futures.append(future)
        
        # Measure performance
        start_time = time.time()
        processed_urls = 0
        while processed_urls < task_count and time.time() - start_time < 300:
            response = self.sqs.receive_message(
                QueueUrl=self.crawl_result_queue['QueueUrl'],
                MaxNumberOfMessages=10
            )
            if 'Messages' in response:
                processed_urls += len(response['Messages'])
            time.sleep(1)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Verify results
        self.assertGreaterEqual(processed_urls, task_count * 0.95)  # 95% success rate
        self.assertLess(duration, 60)  # Should complete within 60 seconds
        
        # Stop nodes
        for crawler in crawlers:
            crawler.stop()

    def test_resource_utilization(self):
        """Test resource utilization under load."""
        # Start multiple crawler nodes
        node_count = 4
        crawlers = []
        for _ in range(node_count):
            crawler = CrawlerNode()
            crawlers.append(crawler)
            threading.Thread(target=crawler.run).start()
        
        # Add tasks
        task_count = 200
        test_urls = [f'http://test{i}.com' for i in range(task_count)]
        for url in test_urls:
            self.sqs.send_message(
                QueueUrl=self.crawl_task_queue['QueueUrl'],
                MessageBody=f'{{"url": "{url}", "depth": 1}}'
            )
        
        # Monitor resource usage
        resource_metrics = []
        start_time = time.time()
        while time.time() - start_time < 300:  # 5-minute monitoring
            metrics = {
                'cpu': [],
                'memory': [],
                'disk': []
            }
            
            for crawler in crawlers:
                metrics['cpu'].append(crawler.cpu_usage)
                metrics['memory'].append(crawler.memory_usage)
                metrics['disk'].append(crawler.disk_usage)
            
            resource_metrics.append({
                'timestamp': time.time(),
                'avg_cpu': statistics.mean(metrics['cpu']),
                'avg_memory': statistics.mean(metrics['memory']),
                'avg_disk': statistics.mean(metrics['disk'])
            })
            
            time.sleep(1)
        
        # Analyze resource usage
        self._analyze_resource_usage(resource_metrics)
        
        # Stop nodes
        for crawler in crawlers:
            crawler.stop()

    def _analyze_scaling_results(self, results):
        """Analyze scaling test results."""
        # Calculate scaling efficiency
        base_performance = results[1]['urls_per_second']
        scaling_factors = []
        
        for node_count, result in results.items():
            if node_count > 1:
                expected_performance = base_performance * node_count
                actual_performance = result['urls_per_second']
                scaling_factor = actual_performance / expected_performance
                scaling_factors.append(scaling_factor)
        
        # Verify scaling efficiency
        avg_scaling_factor = statistics.mean(scaling_factors)
        self.assertGreaterEqual(avg_scaling_factor, 0.7)  # 70% scaling efficiency

    def _analyze_resource_usage(self, resource_metrics):
        """Analyze resource usage metrics."""
        # Calculate average resource usage
        avg_cpu = statistics.mean(m['avg_cpu'] for m in resource_metrics)
        avg_memory = statistics.mean(m['avg_memory'] for m in resource_metrics)
        avg_disk = statistics.mean(m['avg_disk'] for m in resource_metrics)
        
        # Verify resource usage is within acceptable limits
        self.assertLess(avg_cpu, 80)  # CPU usage below 80%
        self.assertLess(avg_memory, 80)  # Memory usage below 80%
        self.assertLess(avg_disk, 80)  # Disk usage below 80%

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