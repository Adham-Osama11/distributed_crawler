"""
Configuration settings for the distributed web crawling system.
"""

# SQS queue names
CRAWL_TASK_QUEUE = 'crawl-task-queue'
CRAWL_RESULT_QUEUE = 'crawl-result-queue'
INDEX_TASK_QUEUE = 'index-task-queue'

# Crawler settings
CRAWLER_USER_AGENT = 'DistributedCrawler/1.0'
CRAWLER_DOWNLOAD_DELAY = 1.0  # seconds
CRAWLER_CONCURRENT_REQUESTS = 1

# Heartbeat settings
HEARTBEAT_INTERVAL = 30  # seconds

# AWS region
AWS_REGION = 'us-east-1'  # Change to your preferred region

# AWS S3 bucket names
CRAWL_DATA_BUCKET = "crawl-data-bucket"
INDEX_DATA_BUCKET = "index-data-bucket"

# Crawler settings
CRAWL_DELAY = 1  # seconds between requests to the same domain
MAX_DEPTH = 3    # maximum depth for crawling
MAX_URLS_PER_DOMAIN = 100  # maximum URLs to crawl per domain
USER_AGENT = "DistributedCrawler/1.0"

# Master node settings
HEARTBEAT_INTERVAL = 30  # seconds between heartbeat checks

# Indexer settings
INDEX_BATCH_SIZE = 50  # number of documents to index in one batch