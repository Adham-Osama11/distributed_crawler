"""
Test script for the Crawler Node component.
This script tests the crawler node's functionality in isolation.
"""
import sys
import os
import time
import uuid
import json
import boto3
from datetime import datetime, timezone

# Add the parent directory to the path so we can import the crawler node
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawler.crawler_node import CrawlerNode, WebSpider
from common.config import AWS_REGION, CRAWL_TASK_QUEUE, CRAWL_RESULT_QUEUE

def test_crawler_initialization():
    """Test that the crawler node initializes correctly."""
    print("Testing crawler node initialization...")
    crawler_id = f"test-crawler-{uuid.uuid4()}"
    crawler = CrawlerNode(crawler_id=crawler_id)
    print(f"Crawler node initialized with ID: {crawler_id}")
    
    # Verify DynamoDB tables were created
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    tables = [table.name for table in dynamodb.tables.all()]
    required_tables = ['url-frontier', 'crawl-metadata']
    
    for table in required_tables:
        if table in tables:
            print(f"✓ Table '{table}' exists")
        else:
            print(f"✗ Table '{table}' does not exist")
    
    return crawler, crawler_id

def test_process_task(crawler):
    """Test the crawler's ability to process a task."""
    print("\nTesting task processing...")
    
    # Create a mock task
    task_id = f"test-task-{uuid.uuid4()}"
    job_id = f"test-job-{uuid.uuid4()}"
    
    mock_task = {
        'url': "https://www.python.org/",
        'depth': 0,
        'max_depth': 1,
        'task_id': task_id,
        'job_id': job_id,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    
    # Process the task
    print(f"Processing task for URL: {mock_task['url']}")
    crawler._process_task(mock_task)
    
    # Wait for the task to complete
    print("Waiting for task to complete...")
    time.sleep(10)
    
    # Check if the URL was updated in the frontier
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    frontier_table = dynamodb.Table('url-frontier')
    
    try:
        response = frontier_table.get_item(
            Key={
                'url': mock_task['url'],
                'job_id': mock_task['job_id']
            }
        )
        
        if 'Item' in response:
            print(f"✓ URL found in frontier with status: {response['Item'].get('status')}")
        else:
            print(f"✗ URL not found in frontier")
    except Exception as e:
        print(f"Error checking frontier: {e}")
    
    # Check if a result was sent to the result queue
    sqs = boto3.client('sqs', region_name=AWS_REGION)
    
    try:
        response = sqs.get_queue_url(QueueName=CRAWL_RESULT_QUEUE)
        queue_url = response['QueueUrl']
        
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5
        )
        
        if 'Messages' in response:
            print(f"✓ Found {len(response['Messages'])} messages in the result queue")
            
            # Check if any of the messages are for our task
            for message in response['Messages']:
                result = json.loads(message['Body'])
                
                if result.get('task_id') == task_id:
                    print(f"✓ Result for task {task_id} found in queue")
                    print(f"  Discovered URLs: {len(result.get('discovered_urls', []))}")
                    
                    # Don't delete the message
                    break
            else:
                print(f"✗ No result for task {task_id} found in queue")
        else:
            print("✗ No messages found in the result queue")
    except Exception as e:
        print(f"Error checking result queue: {e}")

def test_heartbeat(crawler, crawler_id):
    """Test the crawler's heartbeat functionality."""
    print("\nTesting heartbeat functionality...")
    
    # Send a heartbeat
    crawler._send_heartbeat()
    print("Heartbeat sent")
    
    # Check if the heartbeat was received
    sqs = boto3.client('sqs', region_name=AWS_REGION)
    
    try:
        response = sqs.get_queue_url(QueueName=CRAWL_RESULT_QUEUE)
        queue_url = response['QueueUrl']
        
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5
        )
        
        if 'Messages' in response:
            for message in response['Messages']:
                heartbeat = json.loads(message['Body'])
                
                if heartbeat.get('crawler_id') == crawler_id and heartbeat.get('status') == 'active':
                    print(f"✓ Heartbeat for crawler {crawler_id} found in queue")
                    
                    # Don't delete the message
                    break
            else:
                print(f"✗ No heartbeat for crawler {crawler_id} found in queue")
        else:
            print("✗ No messages found in the result queue")
    except Exception as e:
        print(f"Error checking result queue: {e}")

def test_url_frontier_updates(crawler):
    """Test the crawler's ability to update the URL frontier."""
    print("\nTesting URL frontier updates...")
    
    # Create a mock task
    task_id = f"test-task-{uuid.uuid4()}"
    job_id = f"test-job-{uuid.uuid4()}"
    
    mock_task = {
        'url': "https://www.python.org/",
        'depth': 0,
        'max_depth': 1,
        'task_id': task_id,
        'job_id': job_id,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    
    # Test updating after completion
    print("Testing update after completion...")
    crawler._update_url_frontier_after_completion(mock_task['url'], mock_task)
    
    # Check if the URL was updated in the frontier
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    frontier_table = dynamodb.Table('url-frontier')
    
    try:
        response = frontier_table.get_item(
            Key={
                'url': mock_task['url'],
                'job_id': mock_task['job_id']
            }
        )
        
        if 'Item' in response:
            print(f"✓ URL found in frontier with status: {response['Item'].get('status')}")
            print(f"  Completed at: {response['Item'].get('completed_at')}")
        else:
            print(f"✗ URL not found in frontier")
    except Exception as e:
        print(f"Error checking frontier: {e}")
    
    # Test updating after failure
    print("\nTesting update after failure...")
    mock_task['url'] = "https://www.python.org/"
    crawler._update_url_frontier_after_failure(mock_task['url'], mock_task, "Test error message")
    
    try:
        response = frontier_table.get_item(
            Key={
                'url': mock_task['url'],
                'job_id': mock_task['job_id']
            }
        )
        
        if 'Item' in response:
            print(f"✓ URL found in frontier with status: {response['Item'].get('status')}")
            print(f"  Failed at: {response['Item'].get('failed_at')}")
            print(f"  Error message: {response['Item'].get('error_message')}")
        else:
            print(f"✗ URL not found in frontier")
    except Exception as e:
        print(f"Error checking frontier: {e}")

def main():
    """Run all tests."""
    print("=== CRAWLER NODE TESTS ===")
    
    # Test initialization
    crawler, crawler_id = test_crawler_initialization()
    
    # Test heartbeat
    test_heartbeat(crawler, crawler_id)
    
    # Test URL frontier updates
    test_url_frontier_updates(crawler)
    
    # Test task processing
    test_process_task(crawler)
    
    print("\nAll tests completed.")

if __name__ == "__main__":
    main()