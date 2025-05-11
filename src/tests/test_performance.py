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

class TestPerformance(unittest.TestCase):
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
        
        # Initialize nodes
        self.master = MasterNode()
        self.crawler = CrawlerNode()
        self.indexer = WhooshIndex()

    def test_crawl_performance(self):
        """Test crawling performance metrics."""
        # Start crawler
        crawler_thread = threading.Thread(target=self.crawler.run)
        crawler_thread.start()
        
        # Test URLs with different characteristics
        test_urls = [
            'http://small-page.com',  # Small page
            'http://medium-page.com',  # Medium page
            'http://large-page.com',  # Large page
            'http://dynamic-page.com',  # Dynamic content
            'http://static-page.com'   # Static content
        ]
        
        performance_metrics = {}
        
        for url in test_urls:
            # Measure crawl time
            start_time = time.time()
            self.crawler._process_task({'url': url, 'depth': 1})
            end_time = time.time()
            
            # Calculate metrics
            crawl_time = end_time - start_time
            response_time = self.crawler.last_response_time
            bytes_downloaded = self.crawler.last_bytes_downloaded
            
            performance_metrics[url] = {
                'crawl_time': crawl_time,
                'response_time': response_time,
                'bytes_downloaded': bytes_downloaded,
                'bytes_per_second': bytes_downloaded / crawl_time if crawl_time > 0 else 0
            }
        
        # Analyze performance
        self._analyze_crawl_performance(performance_metrics)
        
        # Stop crawler
        self.crawler.stop()

    def test_indexing_performance(self):
        """Test indexing performance metrics."""
        # Test documents with different characteristics
        test_documents = [
            {
                'url': 'http://test1.com',
                'title': 'Test Document 1',
                'content': 'This is a small test document.',
                'metadata': {'type': 'small'}
            },
            {
                'url': 'http://test2.com',
                'title': 'Test Document 2',
                'content': 'This is a medium test document with more content.',
                'metadata': {'type': 'medium'}
            },
            {
                'url': 'http://test3.com',
                'title': 'Test Document 3',
                'content': 'This is a large test document with lots of content and keywords.',
                'metadata': {'type': 'large'}
            }
        ]
        
        indexing_metrics = {}
        
        for doc in test_documents:
            # Measure indexing time
            start_time = time.time()
            self.indexer.add_document(doc)
            end_time = time.time()
            
            # Calculate metrics
            indexing_time = end_time - start_time
            doc_size = len(str(doc))
            
            indexing_metrics[doc['url']] = {
                'indexing_time': indexing_time,
                'doc_size': doc_size,
                'bytes_per_second': doc_size / indexing_time if indexing_time > 0 else 0
            }
        
        # Analyze performance
        self._analyze_indexing_performance(indexing_metrics)

    def test_search_performance(self):
        """Test search performance metrics."""
        # Add test documents
        test_documents = [
            {
                'url': 'http://test1.com',
                'title': 'Python Programming',
                'content': 'Python is a popular programming language.',
                'metadata': {'type': 'programming'}
            },
            {
                'url': 'http://test2.com',
                'title': 'Web Development',
                'content': 'Web development involves HTML, CSS, and JavaScript.',
                'metadata': {'type': 'web'}
            },
            {
                'url': 'http://test3.com',
                'title': 'Data Science',
                'content': 'Data science combines statistics and programming.',
                'metadata': {'type': 'data'}
            }
        ]
        
        for doc in test_documents:
            self.indexer.add_document(doc)
        
        # Test different search queries
        test_queries = [
            'python',
            'web development',
            'data science',
            'programming language',
            'statistics'
        ]
        
        search_metrics = {}
        
        for query in test_queries:
            # Measure search time
            start_time = time.time()
            results = self.indexer.search(query)
            end_time = time.time()
            
            # Calculate metrics
            search_time = end_time - start_time
            result_count = len(results)
            
            search_metrics[query] = {
                'search_time': search_time,
                'result_count': result_count,
                'results_per_second': result_count / search_time if search_time > 0 else 0
            }
        
        # Analyze performance
        self._analyze_search_performance(search_metrics)

    def test_system_performance(self):
        """Test overall system performance."""
        # Start multiple crawler nodes
        node_count = 4
        crawlers = []
        for _ in range(node_count):
            crawler = CrawlerNode()
            crawlers.append(crawler)
            threading.Thread(target=crawler.run).start()
        
        # Add test tasks
        task_count = 100
        test_urls = [f'http://test{i}.com' for i in range(task_count)]
        
        # Measure system performance
        start_time = time.time()
        processed_urls = 0
        system_metrics = []
        
        while processed_urls < task_count and time.time() - start_time < 300:
            # Add tasks
            for url in test_urls[processed_urls:processed_urls+10]:
                self.sqs.send_message(
                    QueueUrl=self.crawl_task_queue['QueueUrl'],
                    MessageBody=f'{{"url": "{url}", "depth": 1}}'
                )
            
            # Process results
            response = self.sqs.receive_message(
                QueueUrl=self.crawl_result_queue['QueueUrl'],
                MaxNumberOfMessages=10
            )
            if 'Messages' in response:
                processed_urls += len(response['Messages'])
            
            # Record metrics
            system_metrics.append({
                'timestamp': time.time(),
                'processed_urls': processed_urls,
                'active_crawlers': sum(1 for c in crawlers if c.is_active()),
                'queue_size': self._get_queue_size(self.crawl_task_queue['QueueUrl']),
                'cpu_usage': statistics.mean(c.cpu_usage for c in crawlers),
                'memory_usage': statistics.mean(c.memory_usage for c in crawlers)
            })
            
            time.sleep(1)
        
        # Analyze system performance
        self._analyze_system_performance(system_metrics)
        
        # Stop crawlers
        for crawler in crawlers:
            crawler.stop()

    def _analyze_crawl_performance(self, metrics):
        """Analyze crawling performance metrics."""
        # Calculate average metrics
        avg_crawl_time = statistics.mean(m['crawl_time'] for m in metrics.values())
        avg_response_time = statistics.mean(m['response_time'] for m in metrics.values())
        avg_bytes_per_second = statistics.mean(m['bytes_per_second'] for m in metrics.values())
        
        # Verify performance meets requirements
        self.assertLess(avg_crawl_time, 5.0)  # Average crawl time under 5 seconds
        self.assertLess(avg_response_time, 2.0)  # Average response time under 2 seconds
        self.assertGreater(avg_bytes_per_second, 10000)  # At least 10KB/s throughput

    def _analyze_indexing_performance(self, metrics):
        """Analyze indexing performance metrics."""
        # Calculate average metrics
        avg_indexing_time = statistics.mean(m['indexing_time'] for m in metrics.values())
        avg_bytes_per_second = statistics.mean(m['bytes_per_second'] for m in metrics.values())
        
        # Verify performance meets requirements
        self.assertLess(avg_indexing_time, 1.0)  # Average indexing time under 1 second
        self.assertGreater(avg_bytes_per_second, 50000)  # At least 50KB/s indexing throughput

    def _analyze_search_performance(self, metrics):
        """Analyze search performance metrics."""
        # Calculate average metrics
        avg_search_time = statistics.mean(m['search_time'] for m in metrics.values())
        avg_results_per_second = statistics.mean(m['results_per_second'] for m in metrics.values())
        
        # Verify performance meets requirements
        self.assertLess(avg_search_time, 0.1)  # Average search time under 100ms
        self.assertGreater(avg_results_per_second, 10)  # At least 10 results per second

    def _analyze_system_performance(self, metrics):
        """Analyze system performance metrics."""
        # Calculate average metrics
        avg_cpu_usage = statistics.mean(m['cpu_usage'] for m in metrics)
        avg_memory_usage = statistics.mean(m['memory_usage'] for m in metrics)
        avg_queue_size = statistics.mean(m['queue_size'] for m in metrics)
        
        # Calculate throughput
        total_time = metrics[-1]['timestamp'] - metrics[0]['timestamp']
        total_urls = metrics[-1]['processed_urls']
        throughput = total_urls / total_time if total_time > 0 else 0
        
        # Verify performance meets requirements
        self.assertLess(avg_cpu_usage, 80)  # Average CPU usage under 80%
        self.assertLess(avg_memory_usage, 80)  # Average memory usage under 80%
        self.assertLess(avg_queue_size, 50)  # Average queue size under 50
        self.assertGreater(throughput, 1)  # At least 1 URL per second throughput

    def _get_queue_size(self, queue_url):
        """Get the current size of a queue."""
        response = self.sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        return int(response['Attributes']['ApproximateNumberOfMessages'])

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