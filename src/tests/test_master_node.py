"""
Test script for the Master Node component.
This script tests the master node's functionality in isolation.
"""
import sys
import os
import time
import uuid
import json
import boto3
from datetime import datetime, timezone

# Add the parent directory to the path so we can import the master node
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from master.master_node import MasterNode
from common.config import AWS_REGION, CRAWL_TASK_QUEUE, CRAWL_RESULT_QUEUE

def test_master_initialization():
    """Test that the master node initializes correctly."""
    print("Testing master node initialization...")
    master = MasterNode()
    print("Master node initialized successfully")
    
    # Verify DynamoDB tables were created
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    tables = [table.name for table in dynamodb.tables.all()]
    required_tables = ['url-frontier', 'crawl-metadata', 'master-status']
    
    for table in required_tables:
        if table in tables:
            print(f"✓ Table '{table}' exists")
        else:
            print(f"✗ Table '{table}' does not exist")
    
    return master

def test_start_crawl(master):
    """Test the start_crawl functionality."""
    print("\nTesting start_crawl functionality...")
    
    # Use a simple test URL
    test_urls = ["https://example.com"]
    crawl_id = master.start_crawl(test_urls, max_depth=1, max_urls_per_domain=5)
    
    print(f"Crawl started with ID: {crawl_id}")
    
    # Verify that tasks were added to the queue
    sqs = boto3.client('sqs', region_name=AWS_REGION)
    
    try:
        response = sqs.get_queue_url(QueueName=CRAWL_TASK_QUEUE)
        queue_url = response['QueueUrl']
        
        # Check for messages in the queue
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5
        )
        
        if 'Messages' in response:
            print(f"✓ Found {len(response['Messages'])} messages in the task queue")
            
            # Examine the first message
            message = response['Messages'][0]
            task = json.loads(message['Body'])
            
            print(f"Task details:")
            print(f"  URL: {task.get('url')}")
            print(f"  Depth: {task.get('depth')}")
            print(f"  Job ID: {task.get('job_id')}")
            
            # Don't delete the message so the crawler can process it
        else:
            print("✗ No messages found in the task queue")
    except Exception as e:
        print(f"Error checking queue: {e}")
    
    # Verify URL frontier was updated
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    frontier_table = dynamodb.Table('url-frontier')
    
    for url in test_urls:
        try:
            # Query for the URL in the frontier
            response = frontier_table.scan(
                FilterExpression="contains(#url, :url_val)",
                ExpressionAttributeNames={'#url': 'url'},
                ExpressionAttributeValues={':url_val': url}
            )
            
            if response['Items']:
                print(f"✓ URL '{url}' found in frontier with status: {response['Items'][0].get('status')}")
            else:
                print(f"✗ URL '{url}' not found in frontier")
        except Exception as e:
            print(f"Error checking frontier: {e}")
    
    return crawl_id

def test_process_results(master):
    """Test the master's ability to process crawl results."""
    print("\nTesting result processing...")
    
    # Create a mock result message
    mock_result = {
        'crawler_id': f"test-crawler-{uuid.uuid4()}",
        'task_id': f"test-task-{uuid.uuid4()}",
        'url': "https://www.python.org/",
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'discovered_urls': [
            "https://www.python.org/about/gettingstarted/",
            "https://docs.python.org/3/"
        ],
        'depth': 0
    }
    
    # Send the mock result to the result queue
    sqs = boto3.client('sqs', region_name=AWS_REGION)
    
    try:
        response = sqs.get_queue_url(QueueName=CRAWL_RESULT_QUEUE)
        queue_url = response['QueueUrl']
        
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(mock_result)
        )
        
        print(f"✓ Mock result sent to result queue")
        
        # Give the master node time to process the result
        print("Waiting for master to process result...")
        time.sleep(10)
        
        # Check if the discovered URLs were added to the frontier
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        frontier_table = dynamodb.Table('url-frontier')
        
        for url in mock_result['discovered_urls']:
            try:
                # Query for the URL in the frontier
                response = frontier_table.scan(
                    FilterExpression="contains(#url, :url_val)",
                    ExpressionAttributeNames={'#url': 'url'},
                    ExpressionAttributeValues={':url_val': url}
                )
                
                if response['Items']:
                    print(f"✓ Discovered URL '{url}' found in frontier with status: {response['Items'][0].get('status')}")
                else:
                    print(f"✗ Discovered URL '{url}' not found in frontier")
            except Exception as e:
                print(f"Error checking frontier: {e}")
    except Exception as e:
        print(f"Error sending mock result: {e}")

def main():
    """Run all tests."""
    print("=== MASTER NODE TESTS ===")
    
    # Test initialization
    master = test_master_initialization()
    
    # Test start_crawl
    crawl_id = test_start_crawl(master)
    
    # Test result processing
    test_process_results(master)
    
    print("\nAll tests completed.")

if __name__ == "__main__":
    main()


'''
## 5. What These Tests Verify
### Master Node Tests:
- Proper initialization and table creation
- Ability to start a crawl and enqueue URLs
- Processing of crawl results and updating the URL frontier
### Crawler Node Tests:
- Proper initialization and table creation
- Heartbeat functionality
- URL frontier updates (both successful and failed crawls)
- Task processing and result sending
## 6. Interpreting Test Results
- ✓ indicates a successful test
- ✗ indicates a failed test
If any tests fail, examine the error messages to identify the issue. Common problems include:

- AWS credential issues
- Missing or misconfigured DynamoDB tables
- SQS queue connectivity problems
- Incorrect message formats

'''