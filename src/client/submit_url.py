"""
Simple client to submit URLs to the distributed web crawler.
"""
import sys
import json
import boto3
import uuid
import time
import os

# Add the parent directory to the path so we can import common modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import MASTER_COMMAND_QUEUE, AWS_REGION

def submit_url(url, max_depth=3, max_urls_per_domain=100):
    """Submit a URL for crawling."""
    try:
        # Initialize AWS client
        sqs = boto3.client('sqs', region_name=AWS_REGION)
        
        # Get the master command queue URL
        response = sqs.get_queue_url(QueueName=MASTER_COMMAND_QUEUE)
        queue_url = response['QueueUrl']
        
        # Create the command
        command = {
            'type': 'crawl_url',
            'url': url,
            'max_depth': max_depth,
            'max_urls_per_domain': max_urls_per_domain,
            'timestamp': time.time()
        }
        
        # Send the command to the queue
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(command)
        )
        
        print(f"Submitted URL for crawling: {url}")
        return True
    except Exception as e:
        print(f"Error submitting URL: {e}")
        return False

def main():
    """Main function to handle command line arguments."""
    if len(sys.argv) < 2:
        print("Usage: python submit_url.py <url> [max_depth] [max_urls_per_domain]")
        return
    
    url = sys.argv[1]
    max_depth = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    max_urls_per_domain = int(sys.argv[3]) if len(sys.argv) > 3 else 100
    
    submit_url(url, max_depth, max_urls_per_domain)

if __name__ == "__main__":
    main()