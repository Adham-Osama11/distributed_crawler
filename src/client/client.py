"""
Client interface for the distributed web crawling system.
"""
import argparse
import boto3
import json
import sys
import os

# Add the parent directory to the path so we can import common modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import CRAWL_TASK_QUEUE
from common.utils import normalize_url

def start_crawl(seed_urls, max_depth=3, max_urls_per_domain=100):
    """Start a new crawl with the given seed URLs."""
    print(f"Starting crawl with {len(seed_urls)} seed URLs")
    
    # Connect to SQS
    sqs = boto3.client('sqs')
    
    # Get queue URL
    response = sqs.get_queue_url(QueueName=CRAWL_TASK_QUEUE)
    queue_url = response['QueueUrl']
    
    # Normalize seed URLs
    normalized_urls = [normalize_url(url) for url in seed_urls]
    
    # Send seed URLs to the master node via the crawl task queue
    for url in normalized_urls:
        # Create a message for the master node
        message = {
            'action': 'start_crawl',
            'url': url,
            'max_depth': max_depth,
            'max_urls_per_domain': max_urls_per_domain
        }
        
        # Send the message
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message)
        )
        print(f"Sent seed URL to master: {url}")
    
    print("Crawl started. The master node will process the seed URLs.")

def main():
    """Main function to run the client interface."""
    parser = argparse.ArgumentParser(description='Distributed Web Crawler Client')
    parser.add_argument('--seed-urls', nargs='+', required=True, help='List of seed URLs to start crawling')
    parser.add_argument('--depth', type=int, default=3, help='Maximum crawl depth')
    parser.add_argument('--max-urls', type=int, default=100, help='Maximum URLs per domain')
    args = parser.parse_args()
    
    start_crawl(args.seed_urls, args.depth, args.max_urls)

if __name__ == "__main__":
    main()