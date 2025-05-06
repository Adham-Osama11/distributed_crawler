"""
Test script for fault tolerance in the distributed web crawler.
"""
import sys
import os
import time
import uuid
import json
import boto3
import subprocess
import signal
import psutil
from datetime import datetime, timezone

# Add the parent directory to the path so we can import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import AWS_REGION, CRAWL_TASK_QUEUE, CRAWL_RESULT_QUEUE

def start_process(command):
    """Start a process and return its process object."""
    print(f"Starting process: {command}")
    return subprocess.Popen(command, shell=True)

def kill_process(process):
    """Kill a process."""
    print(f"Killing process with PID: {process.pid}")
    if os.name == 'nt':  # Windows
        process.kill()
    else:  # Unix
        os.kill(process.pid, signal.SIGKILL)

def test_crawler_fault_tolerance():
    """Test crawler node fault tolerance."""
    print("\n=== Testing Crawler Node Fault Tolerance ===")
    
    # Start master node
    master_process = start_process("python src\\master\\master_node.py")
    time.sleep(5)  # Give master time to initialize
    
    # Start two crawler nodes
    crawler1_id = f"test-crawler-{uuid.uuid4()}"
    crawler2_id = f"test-crawler-{uuid.uuid4()}"
    
    crawler1_process = start_process(f"python src\\crawler\\crawler_node.py --crawler_id {crawler1_id}")
    crawler2_process = start_process(f"python src\\crawler\\crawler_node.py --crawler_id {crawler2_id}")
    time.sleep(5)  # Give crawlers time to initialize
    
    # Submit a URL for crawling
    submit_process = start_process("python src\\client\\submit_url.py https://example.com")
    time.sleep(10)  # Give time for crawling to start
    
    # Kill one crawler node to simulate failure
    print("Simulating crawler node failure...")
    kill_process(crawler1_process)
    
    # Wait for the master to detect the failure (should be 2*HEARTBEAT_INTERVAL)
    print("Waiting for master to detect crawler failure...")
    time.sleep(70)  # Assuming HEARTBEAT_INTERVAL is 30 seconds
    
    # Check if the master detected the failure
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    crawler_status_table = dynamodb.Table('crawler-status')
    
    try:
        response = crawler_status_table.get_item(
            Key={'crawler_id': crawler1_id}
        )
        
        if 'Item' in response:
            status = response['Item'].get('status')
            print(f"Crawler status in DynamoDB: {status}")
            if status == 'failed':
                print("✓ Master successfully detected crawler failure")
            else:
                print(f"✗ Master did not mark crawler as failed (status: {status})")
        else:
            print("✗ Crawler entry not found in DynamoDB")
    except Exception as e:
        print(f"Error checking crawler status: {e}")
    
    # Clean up processes
    kill_process(master_process)
    kill_process(crawler2_process)
    
    print("Crawler fault tolerance test completed")

def test_indexer_fault_tolerance():
    """Test indexer node fault tolerance."""
    print("\n=== Testing Indexer Node Fault Tolerance ===")
    
    # Start master node
    master_process = start_process("python src\\master\\master_node.py")
    time.sleep(5)  # Give master time to initialize
    
    # Start crawler node
    crawler_process = start_process("python src\\crawler\\crawler_node.py")
    time.sleep(5)  # Give crawler time to initialize
    
    # Start indexer node
    indexer_process = start_process("python src\\indexer\\indexer_node.py")
    time.sleep(5)  # Give indexer time to initialize
    
    # Submit a URL for crawling
    submit_process = start_process("python src\\client\\submit_url.py https://example.com")
    time.sleep(20)  # Give time for crawling and indexing to start
    
    # Kill the indexer node to simulate failure
    print("Simulating indexer node failure...")
    kill_process(indexer_process)
    time.sleep(5)
    
    # Start a new indexer node
    print("Starting new indexer node...")
    new_indexer_process = start_process("python src\\indexer\\indexer_node.py")
    time.sleep(30)  # Give time for the new indexer to recover
    
    # Check if the new indexer loaded the index from S3
    # This would require checking the logs or adding specific test hooks
    
    # Clean up processes
    kill_process(master_process)
    kill_process(crawler_process)
    kill_process(new_indexer_process)
    
    print("Indexer fault tolerance test completed")

def test_master_fault_tolerance():
    """Test master node fault tolerance."""
    print("\n=== Testing Master Node Fault Tolerance ===")
    
    # Start master node
    master_process = start_process("python src\\master\\master_node.py")
    time.sleep(5)  # Give master time to initialize
    
    # Start crawler node
    crawler_process = start_process("python src\\crawler\\crawler_node.py")
    time.sleep(5)  # Give crawler time to initialize
    
    # Submit a URL for crawling
    submit_process = start_process("python src\\client\\submit_url.py https://example.com")
    time.sleep(15)  # Give time for crawling to start
    
    # Kill the master node to simulate failure
    print("Simulating master node failure...")
    kill_process(master_process)
    time.sleep(5)
    
    # Start a new master node
    print("Starting new master node...")
    new_master_process = start_process("python src\\master\\master_node.py")
    time.sleep(30)  # Give time for the new master to recover
    
    # Check if crawling continues
    # This would require checking the logs or adding specific test hooks
    
    # Clean up processes
    kill_process(new_master_process)
    kill_process(crawler_process)
    
    print("Master fault tolerance test completed")

def main():
    """Run all fault tolerance tests."""
    print("=== FAULT TOLERANCE TESTS ===")
    
    test_crawler_fault_tolerance()
    test_indexer_fault_tolerance()
    test_master_fault_tolerance()
    
    print("\nAll fault tolerance tests completed.")

if __name__ == "__main__":
    main()