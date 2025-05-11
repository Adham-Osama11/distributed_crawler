import unittest
import time
import threading
from bs4 import BeautifulSoup
import boto3
from moto import mock_sqs, mock_dynamodb, mock_s3
from src.crawler.crawler_node import CrawlerNode
from src.master.master_node import MasterNode
from src.indexer.indexer_node import WhooshIndex

class TestCrawlQuality(unittest.TestCase):
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

    def test_content_extraction(self):
        """Test content extraction quality."""
        # Test HTML content
        test_html = """
        <html>
            <head>
                <title>Test Page</title>
                <meta name="description" content="Test description">
            </head>
            <body>
                <h1>Main Heading</h1>
                <p>This is a test paragraph with <a href="http://test.com">link</a>.</p>
                <div class="content">
                    <h2>Sub Heading</h2>
                    <p>More content here.</p>
                </div>
                <script>var x = 1;</script>
                <style>.test { color: red; }</style>
            </body>
        </html>
        """
        
        # Process content
        soup = BeautifulSoup(test_html, 'html.parser')
        content = self.crawler._extract_content(soup)
        
        # Verify content extraction
        self.assertIn('Main Heading', content)
        self.assertIn('This is a test paragraph', content)
        self.assertIn('Sub Heading', content)
        self.assertIn('More content here', content)
        self.assertNotIn('var x = 1', content)  # Script content should be removed
        self.assertNotIn('.test { color: red; }', content)  # Style content should be removed

    def test_link_extraction(self):
        """Test link extraction quality."""
        # Test HTML with links
        test_html = """
        <html>
            <body>
                <a href="http://test1.com">Link 1</a>
                <a href="http://test2.com">Link 2</a>
                <a href="http://test3.com">Link 3</a>
                <a href="javascript:void(0)">Invalid Link</a>
                <a href="mailto:test@example.com">Email Link</a>
            </body>
        </html>
        """
        
        # Process content
        soup = BeautifulSoup(test_html, 'html.parser')
        links = self.crawler._extract_links(soup, 'http://test.com')
        
        # Verify link extraction
        self.assertEqual(len(links), 3)  # Only HTTP links should be included
        self.assertIn('http://test1.com', links)
        self.assertIn('http://test2.com', links)
        self.assertIn('http://test3.com', links)
        self.assertNotIn('javascript:void(0)', links)
        self.assertNotIn('mailto:test@example.com', links)

    def test_metadata_extraction(self):
        """Test metadata extraction quality."""
        # Test HTML with metadata
        test_html = """
        <html>
            <head>
                <title>Test Page</title>
                <meta name="description" content="Test description">
                <meta name="keywords" content="test, web, crawling">
                <meta name="author" content="Test Author">
                <meta name="robots" content="index, follow">
            </head>
            <body>
                <h1>Content</h1>
            </body>
        </html>
        """
        
        # Process content
        soup = BeautifulSoup(test_html, 'html.parser')
        metadata = self.crawler._extract_metadata(soup)
        
        # Verify metadata extraction
        self.assertEqual(metadata['title'], 'Test Page')
        self.assertEqual(metadata['description'], 'Test description')
        self.assertEqual(metadata['keywords'], 'test, web, crawling')
        self.assertEqual(metadata['author'], 'Test Author')
        self.assertEqual(metadata['robots'], 'index, follow')

    def test_duplicate_detection(self):
        """Test duplicate content detection."""
        # Test similar content
        content1 = "This is a test paragraph about web crawling."
        content2 = "This is a test paragraph about web crawling."  # Exact duplicate
        content3 = "This is a test paragraph about web crawling and indexing."  # Similar content
        
        # Check duplicates
        is_duplicate1 = self.crawler._is_duplicate_content(content1, content2)
        is_duplicate2 = self.crawler._is_duplicate_content(content1, content3)
        
        # Verify duplicate detection
        self.assertTrue(is_duplicate1)  # Exact duplicates should be detected
        self.assertFalse(is_duplicate2)  # Similar content should not be marked as duplicate

    def test_content_quality(self):
        """Test content quality assessment."""
        # Test different content types
        test_contents = [
            {
                'content': 'This is a well-written article about web crawling.',
                'expected_quality': 'high'
            },
            {
                'content': 'Buy now! Click here! Special offer!',
                'expected_quality': 'low'
            },
            {
                'content': 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.',
                'expected_quality': 'low'
            }
        ]
        
        for test in test_contents:
            quality = self.crawler._assess_content_quality(test['content'])
            self.assertEqual(quality, test['expected_quality'])

    def test_crawl_depth_control(self):
        """Test crawl depth control."""
        # Test URLs with different depths
        test_urls = [
            {'url': 'http://test1.com', 'depth': 1},
            {'url': 'http://test1.com/page1', 'depth': 2},
            {'url': 'http://test1.com/page1/subpage', 'depth': 3}
        ]
        
        # Set max depth
        self.crawler.max_depth = 2
        
        # Check depth control
        for url_info in test_urls:
            should_crawl = self.crawler._should_crawl_url(url_info['url'], url_info['depth'])
            if url_info['depth'] <= self.crawler.max_depth:
                self.assertTrue(should_crawl)
            else:
                self.assertFalse(should_crawl)

    def test_robots_txt_compliance(self):
        """Test robots.txt compliance."""
        # Test robots.txt rules
        robots_txt = """
        User-agent: *
        Disallow: /private/
        Disallow: /admin/
        Allow: /public/
        """
        
        # Set up crawler with robots.txt
        self.crawler._parse_robots_txt(robots_txt)
        
        # Test URLs
        test_urls = [
            'http://test.com/public/page',  # Should be allowed
            'http://test.com/private/page',  # Should be disallowed
            'http://test.com/admin/page',  # Should be disallowed
            'http://test.com/other/page'  # Should be allowed
        ]
        
        # Check compliance
        self.assertTrue(self.crawler._is_allowed_by_robots(test_urls[0]))
        self.assertFalse(self.crawler._is_allowed_by_robots(test_urls[1]))
        self.assertFalse(self.crawler._is_allowed_by_robots(test_urls[2]))
        self.assertTrue(self.crawler._is_allowed_by_robots(test_urls[3]))

    def test_content_freshness(self):
        """Test content freshness detection."""
        # Test content with different timestamps
        test_contents = [
            {
                'content': 'Old content',
                'timestamp': time.time() - 86400 * 30  # 30 days old
            },
            {
                'content': 'Recent content',
                'timestamp': time.time() - 86400  # 1 day old
            },
            {
                'content': 'Current content',
                'timestamp': time.time()  # Current
            }
        ]
        
        for content in test_contents:
            is_fresh = self.crawler._is_content_fresh(content['timestamp'])
            if content['timestamp'] > time.time() - 86400 * 7:  # 7 days
                self.assertTrue(is_fresh)
            else:
                self.assertFalse(is_fresh)

    def test_crawl_completeness(self):
        """Test crawl completeness."""
        # Start crawler
        crawler_thread = threading.Thread(target=self.crawler.run)
        crawler_thread.start()
        
        # Add test URLs
        test_urls = [
            'http://test1.com',
            'http://test2.com',
            'http://test3.com'
        ]
        
        for url in test_urls:
            self.sqs.send_message(
                QueueUrl=self.crawl_task_queue['QueueUrl'],
                MessageBody=f'{{"url": "{url}", "depth": 1}}'
            )
        
        # Wait for crawling to complete
        time.sleep(5)
        
        # Check crawl completeness
        for url in test_urls:
            response = self.dynamodb.Table('url-frontier').get_item(
                Key={'url': url}
            )
            self.assertEqual(response['Item']['status'], 'completed')
            self.assertIn('content', response['Item'])
            self.assertIn('metadata', response['Item'])
        
        # Stop crawler
        self.crawler.stop()

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