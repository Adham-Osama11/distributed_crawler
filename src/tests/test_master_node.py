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
import threading
import random
from datetime import datetime, timezone, timedelta

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
        'url': "https://example.com/",
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'discovered_urls': [
            "https://example.com/page-1",
            "https://example.com/page-0",
            "hhttps://example.com/page-2"
        ],
        'depth': 0,
        'max_depth': 2,
        'crawl_id': str(uuid.uuid4()),
        'content_key': f"test-content-{uuid.uuid4()}"
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

def test_fault_tolerance(master):
    """Test the master node's fault tolerance capabilities."""
    print("\nTesting fault tolerance...")
    
    # 1. Test handling of stale tasks
    print("Testing stale task handling...")
    
    # Create a stale task in the frontier
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    frontier_table = dynamodb.Table('url-frontier')
    
    stale_url = f"https://example.com/stale-{uuid.uuid4()}"
    stale_job_id = f"job-{uuid.uuid4()}"
    stale_time = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    
    try:
        frontier_table.put_item(
            Item={
                'url': stale_url,
                'job_id': stale_job_id,
                'status': 'in_progress',
                'created_at': stale_time,
                'updated_at': stale_time,
                'depth': 0
            }
        )
        print(f"✓ Created stale task for URL: {stale_url}")
        
        # Run the master's task recovery function
        print("Running task recovery...")
        master._recover_stale_tasks()
        
        # Check if the stale task was reset
        response = frontier_table.get_item(
            Key={
                'url': stale_url,
                'job_id': stale_job_id
            }
        )
        
        if 'Item' in response:
            item = response['Item']
            if item.get('status') == 'pending':
                print(f"✓ Stale task was reset to 'pending' status")
            else:
                print(f"✗ Stale task was not reset, status: {item.get('status')}")
        else:
            print(f"✗ Could not find stale task after recovery attempt")
    except Exception as e:
        print(f"Error testing stale task handling: {e}")
    
    # 2. Test handling of crawler node failures
    print("\nTesting crawler node failure handling...")
    
    # Simulate a failed crawler by creating a stale heartbeat
    crawler_status_table = dynamodb.Table('crawler-status')
    failed_crawler_id = f"test-crawler-{uuid.uuid4()}"
    stale_heartbeat = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
    
    try:
        crawler_status_table.put_item(
            Item={
                'crawler_id': failed_crawler_id,
                'status': 'active',
                'last_heartbeat': stale_heartbeat,
                'urls_crawled': 10,
                'current_url': 'https://example.com/page'
            }
        )
        print(f"✓ Created stale crawler record: {failed_crawler_id}")
        
        # Run the master's crawler health check
        print("Running crawler health check...")
        master._check_crawler_health()
        
        # Check if the crawler was marked as failed
        response = crawler_status_table.get_item(
            Key={
                'crawler_id': failed_crawler_id
            }
        )
        
        if 'Item' in response:
            item = response['Item']
            if item.get('status') == 'failed' or item.get('status') == 'stopped':
                print(f"✓ Failed crawler was correctly marked as {item.get('status')}")
            else:
                print(f"✗ Failed crawler was not marked as failed, status: {item.get('status')}")
        else:
            print(f"✗ Could not find crawler record after health check")
    except Exception as e:
        print(f"Error testing crawler failure handling: {e}")

def test_scalability(master):
    """Test the master node's ability to handle multiple crawlers and tasks."""
    print("\nTesting scalability...")
    
    # 1. Test handling of multiple concurrent crawlers
    print("Testing multiple crawler handling...")
    
    # Simulate multiple active crawlers
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    crawler_status_table = dynamodb.Table('crawler-status')
    
    crawler_count = 5
    crawler_ids = []
    
    try:
        # Create multiple crawler records
        for i in range(crawler_count):
            crawler_id = f"test-crawler-{uuid.uuid4()}"
            crawler_ids.append(crawler_id)
            
            crawler_status_table.put_item(
                Item={
                    'crawler_id': crawler_id,
                    'status': 'active',
                    'last_heartbeat': datetime.now(timezone.utc).isoformat(),
                    'urls_crawled': random.randint(5, 50),
                    'current_url': f'https://example.com/page-{i}'
                }
            )
        
        print(f"✓ Created {crawler_count} active crawler records")
        
        # Test the master's ability to distribute tasks to multiple crawlers
        print("Testing task distribution to multiple crawlers...")
        
        # Start a new crawl with multiple URLs
        test_urls = [f"https://example.com/page-{i}" for i in range(10)]
        crawl_id = master.start_crawl(test_urls, max_depth=1, max_urls_per_domain=5)
        
        print(f"Started crawl with ID: {crawl_id} and {len(test_urls)} URLs")
        
        # Run the master's task distribution function
        print("Running task distribution...")
        tasks_distributed = master.distribute_tasks()
        
        print(f"Distributed {tasks_distributed} tasks")
        
        # Check if tasks were distributed
        if tasks_distributed > 0:
            print(f"✓ Master successfully distributed tasks to multiple crawlers")
        else:
            print(f"✗ Master failed to distribute tasks to multiple crawlers")
    except Exception as e:
        print(f"Error testing multiple crawler handling: {e}")
    
    # Clean up test crawler records
    try:
        for crawler_id in crawler_ids:
            crawler_status_table.delete_item(
                Key={
                    'crawler_id': crawler_id
                }
            )
        print(f"✓ Cleaned up {len(crawler_ids)} test crawler records")
    except Exception as e:
        print(f"Error cleaning up test crawler records: {e}")

def test_master_commands(master):
    """Test the master node's ability to process commands."""
    print("\nTesting master command processing...")
    
    # Test pause command
    print("Testing pause command...")
    
    sqs = boto3.client('sqs', region_name=AWS_REGION)
    
    try:
        # Get the command queue URL
        response = sqs.get_queue_url(QueueName='master-command-queue')
        command_queue_url = response['QueueUrl']
        
        # Send a pause command
        pause_command = {
            'command': 'pause',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        sqs.send_message(
            QueueUrl=command_queue_url,
            MessageBody=json.dumps(pause_command)
        )
        
        print(f"✓ Sent pause command to master")
        
        # Give the master time to process the command
        print("Waiting for master to process command...")
        time.sleep(5)
        
        # Check if the master's state was updated
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        master_status_table = dynamodb.Table('master-status')
        
        response = master_status_table.get_item(
            Key={
                'master_id': 'master'
            }
        )
        
        if 'Item' in response:
            item = response['Item']
            if item.get('status') == 'paused':
                print(f"✓ Master status was correctly updated to 'paused'")
            else:
                print(f"✗ Master status was not updated, status: {item.get('status')}")
        else:
            print(f"✗ Could not find master status record")
        
        # Send a resume command
        resume_command = {
            'command': 'resume',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        sqs.send_message(
            QueueUrl=command_queue_url,
            MessageBody=json.dumps(resume_command)
        )
        
        print(f"✓ Sent resume command to master")
        
        # Give the master time to process the command
        print("Waiting for master to process command...")
        time.sleep(5)
        
        # Check if the master's state was updated
        response = master_status_table.get_item(
            Key={
                'master_id': 'master'
            }
        )
        
        if 'Item' in response:
            item = response['Item']
            if item.get('status') == 'running':
                print(f"✓ Master status was correctly updated to 'running'")
            else:
                print(f"✗ Master status was not updated, status: {item.get('status')}")
        else:
            print(f"✗ Could not find master status record")
    except Exception as e:
        print(f"Error testing master commands: {e}")

def main():
    """Run all tests."""
    print("=== MASTER NODE TESTS ===")
    
    # Test initialization
    master = test_master_initialization()
    
    # Test start_crawl
    crawl_id = test_start_crawl(master)
    
    # Test result processing
    test_process_results(master)
    
    # Test fault tolerance
    test_fault_tolerance(master)
    
    # Test scalability
    test_scalability(master)
    
    # Test master commands
    test_master_commands(master)
    
    print("\nAll tests completed.")

if __name__ == "__main__":
    main()


'''
## 5. What These Tests Verify
### Master Node Tests:
- Proper initialization and table creation
- Ability to start a crawl and enqueue URLs
- Processing of crawl results and updating the URL frontier
- Fault tolerance: handling stale tasks and crawler failures
- Scalability: handling multiple crawlers and distributing tasks
- Command processing: pause and resume functionality

## 6. Interpreting Test Results
- ✓ indicates a successful test
- ✗ indicates a failed test
If any tests fail, examine the error messages to identify the issue. Common problems include:

- AWS credential issues
- Missing or misconfigured DynamoDB tables
- SQS queue connectivity problems
- Incorrect message formats
- Timing issues (some tests may need longer wait times)

## 7. Phase 4 Requirements Coverage
These tests address the Phase 4 requirements for:
- Functional Testing: Testing all core master node functions
- Fault Tolerance Testing: Simulating failures and verifying recovery
- Scalability Testing: Testing with multiple crawlers and tasks
- System Refinement: Identifying potential issues for improvement
'''