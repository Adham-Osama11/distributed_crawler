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
import threading
from datetime import datetime, timezone, timedelta

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
    required_tables = ['url-frontier', 'crawl-metadata', 'crawler-status']
    
    for table in required_tables:
        if table in tables:
            print(f"✓ Table '{table}' exists")
        else:
            print(f"✗ Table '{table}' does not exist")
    
    # Verify crawler status was registered
    try:
        crawler_status_table = dynamodb.Table('crawler-status')
        response = crawler_status_table.get_item(
            Key={
                'crawler_id': crawler_id
            }
        )
        
        if 'Item' in response:
            print(f"✓ Crawler registered in status table with status: {response['Item'].get('status')}")
        else:
            print(f"✗ Crawler not registered in status table")
    except Exception as e:
        print(f"Error checking crawler status: {e}")
    
    return crawler, crawler_id

def test_process_task(crawler):
    """Test the crawler's ability to process a task."""
    print("\nTesting task processing...")
    
    # Create a mock task
    task_id = f"test-task-{uuid.uuid4()}"
    crawl_id = f"test-crawl-{uuid.uuid4()}"
    job_id = f"job-{crawl_id}"
    
    mock_task = {
        'url': "https://example.com/",
        'depth': 0,
        'max_depth': 1,
        'max_urls_per_domain': 10,
        'task_id': task_id,
        'job_id': job_id,
        'crawl_id': crawl_id,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    
    # Process the task
    print(f"Processing task for URL: {mock_task['url']}")
    crawler.process_task(mock_task)
    
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
    
    return task_id, job_id

def test_heartbeat(crawler, crawler_id):
    """Test the crawler's heartbeat functionality."""
    print("\nTesting heartbeat functionality...")
    
    # Send a heartbeat
    crawler._send_heartbeat()
    print("Heartbeat sent")
    
    # Check if the heartbeat was updated in DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    
    try:
        crawler_status_table = dynamodb.Table('crawler-status')
        response = crawler_status_table.get_item(
            Key={
                'crawler_id': crawler_id
            }
        )
        
        if 'Item' in response:
            last_heartbeat = response['Item'].get('last_heartbeat')
            if last_heartbeat:
                # Parse the timestamp
                heartbeat_time = datetime.fromisoformat(last_heartbeat.replace('Z', '+00:00'))
                now = datetime.now(timezone.utc)
                
                # Check if the heartbeat is recent (within the last minute)
                if (now - heartbeat_time).total_seconds() < 60:
                    print(f"✓ Recent heartbeat found in DynamoDB: {last_heartbeat}")
                else:
                    print(f"✗ Heartbeat is not recent: {last_heartbeat}")
            else:
                print(f"✗ No heartbeat timestamp found in crawler status")
        else:
            print(f"✗ Crawler not found in status table")
    except Exception as e:
        print(f"Error checking heartbeat: {e}")

def test_url_frontier_updates(crawler):
    """Test the crawler's ability to update the URL frontier."""
    print("\nTesting URL frontier updates...")
    
    # Create a mock task
    task_id = f"test-task-{uuid.uuid4()}"
    crawl_id = f"test-crawl-{uuid.uuid4()}"
    job_id = f"job-{crawl_id}"
    
    mock_task = {
        'url': "https://example.com/test-update",
        'depth': 0,
        'max_depth': 1,
        'max_urls_per_domain': 10,
        'task_id': task_id,
        'job_id': job_id,
        'crawl_id': crawl_id,
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
    mock_task['url'] = "https://example.com/test-failure"
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

def test_fault_tolerance(crawler, crawler_id):
    """Test the crawler's fault tolerance capabilities."""
    print("\nTesting fault tolerance...")
    
    # Test task timeout handling
    print("Testing task timeout handling...")
    
    # Create a mock task with a very short timeout
    task_id = f"test-task-{uuid.uuid4()}"
    crawl_id = f"test-crawl-{uuid.uuid4()}"
    job_id = f"job-{crawl_id}"
    
    mock_task = {
        'url': "https://example.com/timeout-test",
        'depth': 0,
        'max_depth': 1,
        'max_urls_per_domain': 10,
        'task_id': task_id,
        'job_id': job_id,
        'crawl_id': crawl_id,
        'timestamp': (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(),  # Make it look old
        'timeout': 1  # 1 second timeout
    }
    
    # Add the task to the crawler's in-progress tasks
    crawler.in_progress_tasks[task_id] = mock_task
    
    # Run the timeout check
    print("Running timeout check...")
    crawler._check_task_timeouts()
    
    # Check if the task was removed from in-progress tasks
    if task_id not in crawler.in_progress_tasks:
        print(f"✓ Timed-out task was removed from in-progress tasks")
    else:
        print(f"✗ Timed-out task was not removed from in-progress tasks")
    
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
            status = response['Item'].get('status')
            if status == 'failed' or status == 'pending':
                print(f"✓ URL status was updated to '{status}' after timeout")
            else:
                print(f"✗ URL status was not updated correctly after timeout: {status}")
        else:
            print(f"✗ URL not found in frontier after timeout")
    except Exception as e:
        print(f"Error checking frontier: {e}")
    
    # Test recovery from failure
    print("\nTesting recovery from failure...")
    
    # Simulate a crawler failure and recovery
    # First, update the crawler status to 'failed'
    crawler_status_table = dynamodb.Table('crawler-status')
    
    try:
        crawler_status_table.update_item(
            Key={
                'crawler_id': crawler_id
            },
            UpdateExpression="SET #status = :status",
            ExpressionAttributeNames={
                '#status': 'status'
            },
            ExpressionAttributeValues={
                ':status': 'failed'
            }
        )
        print(f"✓ Crawler status set to 'failed'")
        
        # Now simulate recovery by calling the recovery method
        crawler._recover_from_failure()
        
        # Check if the crawler status was updated
        response = crawler_status_table.get_item(
            Key={
                'crawler_id': crawler_id
            }
        )
        
        if 'Item' in response:
            status = response['Item'].get('status')
            if status == 'active':
                print(f"✓ Crawler status was recovered to 'active'")
            else:
                print(f"✗ Crawler status was not recovered: {status}")
        else:
            print(f"✗ Crawler not found in status table after recovery")
    except Exception as e:
        print(f"Error testing recovery: {e}")

def test_robots_txt_compliance(crawler):
    """Test the crawler's compliance with robots.txt."""
    print("\nTesting robots.txt compliance...")
    
    # Create a spider with robots.txt enabled
    spider = WebSpider(url="https://www.google.com/", task_id="test-robots", depth=0)
    
    # Check if the spider has robots.txt middleware enabled
    if hasattr(spider, 'robotstxt_obey'):
        print(f"✓ Spider has robotstxt_obey attribute: {spider.robotstxt_obey}")
    else:
        print(f"✗ Spider does not have robotstxt_obey attribute")
    
    # Check if the crawler settings include robots.txt middleware
    if 'ROBOTSTXT_OBEY' in crawler.settings:
        print(f"✓ Crawler settings include ROBOTSTXT_OBEY: {crawler.settings['ROBOTSTXT_OBEY']}")
    else:
        print(f"✗ Crawler settings do not include ROBOTSTXT_OBEY")
    
    # Check if the crawler settings include a download delay for politeness
    if 'DOWNLOAD_DELAY' in crawler.settings:
        print(f"✓ Crawler settings include DOWNLOAD_DELAY: {crawler.settings['DOWNLOAD_DELAY']}")
    else:
        print(f"✗ Crawler settings do not include DOWNLOAD_DELAY")

def test_concurrent_tasks(crawler):
    """Test the crawler's ability to handle concurrent tasks."""
    print("\nTesting concurrent task handling...")
    
    # Create multiple mock tasks
    task_count = 3
    tasks = []
    
    for i in range(task_count):
        task_id = f"test-task-{uuid.uuid4()}"
        crawl_id = f"test-crawl-{uuid.uuid4()}"
        job_id = f"job-{crawl_id}"
        
        mock_task = {
            'url': f"https://example.com/concurrent-test-{i}",
            'depth': 0,
            'max_depth': 1,
            'max_urls_per_domain': 10,
            'task_id': task_id,
            'job_id': job_id,
            'crawl_id': crawl_id,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        tasks.append(mock_task)
    
    # Process tasks concurrently
    threads = []
    for task in tasks:
        thread = threading.Thread(target=crawler.process_task, args=(task,))
        threads.append(thread)
        thread.start()
        print(f"Started thread for task: {task['task_id']}")
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    print("All task threads completed")
    
    # Check if all tasks were processed
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    frontier_table = dynamodb.Table('url-frontier')
    
    for task in tasks:
        try:
            response = frontier_table.get_item(
                Key={
                    'url': task['url'],
                    'job_id': task['job_id']
                }
            )
            
            if 'Item' in response:
                print(f"✓ URL '{task['url']}' found in frontier with status: {response['Item'].get('status')}")
            else:
                print(f"✗ URL '{task['url']}' not found in frontier")
        except Exception as e:
            print(f"Error checking frontier for {task['url']}: {e}")

def main():
    """Run all tests."""
    print("=== CRAWLER NODE TESTS ===")
    
    # Test initialization
    crawler, crawler_id = test_crawler_initialization()
    
    # Test heartbeat
    test_heartbeat(crawler, crawler_id)
    
    # Test URL frontier updates
    test_url_frontier_updates(crawler)
    
    # Test fault tolerance
    test_fault_tolerance(crawler, crawler_id)
    
    # Test robots.txt compliance
    test_robots_txt_compliance(crawler)
    
    # Test task processing
    task_id, job_id = test_process_task(crawler)
    
    # Test concurrent tasks
    test_concurrent_tasks(crawler)
    
    print("\nAll tests completed.")

if __name__ == "__main__":
    main()


'''
## What These Tests Verify
### Crawler Node Tests:
- Proper initialization and table creation
- Task processing and result generation
- Heartbeat functionality for health monitoring
- URL frontier updates after completion and failure
- Fault tolerance: task timeout handling and recovery from failures
- Robots.txt compliance and politeness settings
- Concurrent task processing capability

## Interpreting Test Results
- ✓ indicates a successful test
- ✗ indicates a failed test
If any tests fail, examine the error messages to identify the issue. Common problems include:

- AWS credential issues
- Missing or misconfigured DynamoDB tables
- SQS queue connectivity problems
- Incorrect message formats
- Timing issues (some tests may need longer wait times)

## Phase 4 Requirements Coverage
These tests address the Phase 4 requirements for:
- Functional Testing: Testing all core crawler node functions
- Fault Tolerance Testing: Simulating failures and verifying recovery
- Crawl Quality Evaluation: Testing robots.txt compliance and politeness
- Scalability Testing: Testing concurrent task processing
'''