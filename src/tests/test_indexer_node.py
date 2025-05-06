"""
Test script for the indexer node component.
"""
import sys
import os
import json
import uuid
import boto3
import time
from datetime import datetime, timezone

# Add the parent directory to the path so we can import indexer_node
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from indexer.indexer_node import IndexerNode
from common.config import AWS_REGION, CRAWL_DATA_BUCKET, INDEX_TASK_QUEUE

def test_indexer_initialization():
    """Test that the indexer node initializes correctly."""
    print("Testing indexer node initialization...")
    
    try:
        # Create a test indexer node without the indexer_id parameter
        indexer = IndexerNode()
        
        # Check if the indexer was initialized correctly
        if indexer:
            print(f"✓ Indexer node initialized successfully")
            # If the IndexerNode has an ID attribute with a different name, check it here
            if hasattr(indexer, 'node_id'):
                print(f"  Node ID: {indexer.node_id}")
        else:
            print(f"✗ Failed to initialize indexer node")
    except Exception as e:
        print(f"✗ Error in indexer initialization test: {e}")
        import traceback
        print(traceback.format_exc())

def test_process_document():
    """Test processing a document."""
    print("Testing document processing...")
    
    # Create a test indexer node without the indexer_id parameter
    try:
        indexer = IndexerNode()
        
        # Create a test document
        test_document = {
            'url': 'https://example.com/test',
            'title': 'Test Page',
            'description': 'This is a test page',
            'content': 'This is the content of the test page. It contains some keywords like distributed, crawler, and indexer.',
            'links': ['https://example.com/page1', 'https://example.com/page2']
        }
        
        # Process the document
        result = indexer.process_document(test_document)
        if result:
            print(f"✓ Successfully processed test document")
            print(f"  Result: {result}")
        else:
            print(f"✗ Failed to process test document")
    except Exception as e:
        print(f"✗ Error in document processing test: {e}")
        import traceback
        print(traceback.format_exc())

def test_index_operations():
    """Test index operations."""
    print("Testing index operations...")
    
    try:
        # Create a test indexer node without the indexer_id parameter
        indexer = IndexerNode()
        
        # Test adding a document to the index
        test_document = {
            'url': 'https://example.com/test',
            'title': 'Test Page',
            'description': 'This is a test page',
            'content': 'This is the content of the test page. It contains some keywords like distributed, crawler, and indexer.',
            'links': ['https://example.com/page1', 'https://example.com/page2']
        }
        
        indexer.add_to_index(test_document)
        print(f"✓ Successfully added test document to index")
        
        # Test searching the index
        search_results = indexer.search_index('crawler')
        if search_results:
            print(f"✓ Search results for 'crawler': {search_results}")
        else:
            print(f"✗ No search results found for 'crawler'")
    except Exception as e:
        print(f"✗ Error in index operations test: {e}")
        import traceback
        print(traceback.format_exc())

def test_queue_processing():
    """Test the indexer's ability to process tasks from the queue."""
    print("Testing queue processing...")
    
    try:
        # Create a test indexer node without the indexer_id parameter
        indexer = IndexerNode()
        
        # Create a test task
        test_task = {
            'type': 'index',
            'url': 'https://example.com/test',
            's3_key': f"test/document-{uuid.uuid4()}.json",
            'job_id': f"test-job-{uuid.uuid4()}"
        }
        
        # Send a test message to the index task queue
        indexer.sqs.send_message(
            QueueUrl=indexer.index_task_queue_url,
            MessageBody=json.dumps(test_task)
        )
        print(f"✓ Successfully sent test task to index task queue")
        
        # Test receiving the message
        response = indexer.sqs.receive_message(
            QueueUrl=indexer.index_task_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5
        )
        
        if 'Messages' in response and response['Messages']:
            message = response['Messages'][0]
            received_task = json.loads(message['Body'])
            print(f"✓ Successfully received test task from index task queue")
            print(f"  Task: {received_task}")
            
            # Delete the message
            indexer.sqs.delete_message(
                QueueUrl=indexer.index_task_queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
            print(f"✓ Successfully deleted test task from index task queue")
        else:
            print(f"✗ Failed to receive test task from index task queue")
    except Exception as e:
        print(f"✗ Error in queue processing test: {e}")
        import traceback
        print(traceback.format_exc())

def test_s3_operations():
    """Test S3 operations."""
    print("Testing S3 operations...")
    
    try:
        # Create a test indexer node without the indexer_id parameter
        indexer = IndexerNode()
        
        # Create a test document
        test_document = {
            'url': 'https://example.com/test',
            'title': 'Test Page',
            'description': 'This is a test page',
            'content': 'This is the content of the test page.',
            'indexed_at': datetime.now(timezone.utc).isoformat()
        }
        
        # Generate a unique key for this test
        test_key = f"test/index-{uuid.uuid4()}.json"
        
        # Upload to S3
        indexer.s3.put_object(
            Bucket=CRAWL_DATA_BUCKET,
            Key=test_key,
            Body=json.dumps(test_document),
            ContentType='application/json'
        )
        print(f"✓ Successfully uploaded test document to S3 with key: {test_key}")
        
        # Retrieve from S3
        response = indexer.s3.get_object(
            Bucket=CRAWL_DATA_BUCKET,
            Key=test_key
        )
        
        retrieved_document = json.loads(response['Body'].read().decode('utf-8'))
        if retrieved_document and retrieved_document.get('url') == test_document['url']:
            print(f"✓ Successfully retrieved test document from S3")
        else:
            print(f"✗ Retrieved document does not match uploaded document")
            
        # Clean up
        indexer.s3.delete_object(
            Bucket=CRAWL_DATA_BUCKET,
            Key=test_key
        )
        print(f"✓ Successfully deleted test document from S3")
    except Exception as e:
        print(f"✗ Error in S3 operations test: {e}")
        import traceback
        print(traceback.format_exc())

def run_tests():
    """Run all tests."""
    print("Running indexer node tests...")
    print("-" * 50)
    
    # Run tests
    test_indexer_initialization()
    print("\n" + "-" * 50 + "\n")
    
    test_process_document()
    print("\n" + "-" * 50 + "\n")
    
    test_index_operations()
    print("\n" + "-" * 50 + "\n")
    
    test_queue_processing()
    print("\n" + "-" * 50 + "\n")
    
    test_s3_operations()
    print("\n" + "-" * 50 + "\n")
    
    print("All indexer node tests completed")

if __name__ == "__main__":
    run_tests()