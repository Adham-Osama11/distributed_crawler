"""
Simple web interface for searching the distributed web crawler's index.
"""
import boto3
import json
import os
import sys
from flask import Flask, render_template, request, jsonify
import requests
from datetime import datetime
import tempfile
import shutil
import uuid

# Add the project root directory to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from indexer.indexer_node import WhooshIndex

app = Flask(__name__)

# Add the parent directory to the path so we can import common modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import AWS_REGION, CRAWL_DATA_BUCKET, INDEX_DATA_BUCKET, MASTER_COMMAND_QUEUE 

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
s3 = boto3.client('s3', region_name=AWS_REGION)

@app.route('/')
def index():
    """Render the search page."""
    # Get some basic stats to display
    stats = get_system_stats()
    return render_template('search.html', stats=stats)

# Add at the top of the file with other imports
from functools import lru_cache
import time

# Add this cache configuration
CACHE_TIMEOUT = 300  # 5 minutes in seconds
search_cache = {}

def get_cached_results(query):
    """Get cached search results if available and not expired."""
    if query in search_cache:
        timestamp, results = search_cache[query]
        if time.time() - timestamp < CACHE_TIMEOUT:
            return results
    return None

def cache_results(query, results):
    """Cache search results with current timestamp."""
    search_cache[query] = (time.time(), results)
    
    # Limit cache size to prevent memory issues
    if len(search_cache) > 100:
        # Remove oldest entries
        oldest_query = min(search_cache.keys(), key=lambda k: search_cache[k][0])
        search_cache.pop(oldest_query)

# Modify the search route to use caching
def track_search_query(query, results_count):
    """Track search queries in DynamoDB for analytics."""
    try:
        # Ensure the table exists
        table_name = 'search-analytics'
        try:
            table = dynamodb.Table(table_name)
            table.table_status  # This will raise an exception if the table doesn't exist
        except Exception:
            # Create the table if it doesn't exist
            dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'query', 'KeyType': 'HASH'},
                    {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'query', 'AttributeType': 'S'},
                    {'AttributeName': 'timestamp', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            table = dynamodb.Table(table_name)
        
        # Record the search query
        table.put_item(
            Item={
                'query': query,
                'timestamp': datetime.now().isoformat(),
                'results_count': results_count,
                'client_ip': request.remote_addr
            }
        )
        return True
    except Exception as e:
        print(f"Error tracking search query: {e}")
        return False

@app.route('/search')
def search():
    """Handle search requests using dashboard-style DynamoDB logic."""
    query = request.args.get('q', '')
    if not query:
        return jsonify({'results': [], 'count': 0, 'query': query, 'message': 'Please enter a search query'})
    print(f"Received search query: {query}")
    search_result = search_indexed_content(query)
    results = search_result.get('results', [])
    print(f"Results: {results}")
    return jsonify({
        'results': results,
        'count': len(results),
        'query': query,
        'timestamp': datetime.now().isoformat(),
        'cached': False
    })

@app.route('/stats')
def stats():
    """Return system statistics."""
    return jsonify(get_system_stats())

def get_system_stats():
    """Get basic system statistics."""
    try:
        # Get counts from DynamoDB tables
        url_count = get_table_count('url-frontier')
        
        # Try to get indexed count, but handle the case where the table doesn't exist
        try:
            indexed_count = get_table_count('indexed-documents')
        except Exception as e:
            print(f"Warning: Could not get count for indexed-documents table: {e}")
            # Try alternative table that might contain indexed documents
            try:
                indexed_count = get_table_count('crawl-metadata')
            except Exception:
                indexed_count = 0
        
        # Get active crawlers
        active_crawlers = get_active_crawlers()
        
        return {
            'urls_in_frontier': url_count,
            'urls_indexed': indexed_count,
            'active_crawlers': len(active_crawlers),
            'last_updated': datetime.now().isoformat()
        }
    except Exception as e:
        print(f"Error getting system stats: {e}")
        return {
            'error': str(e),
            'urls_in_frontier': 0,
            'urls_indexed': 0,
            'active_crawlers': 0,
            'last_updated': datetime.now().isoformat()
        }

def get_table_count(table_name):
    """Get the count of items in a DynamoDB table."""
    try:
        table = dynamodb.Table(table_name)
        response = table.scan(Select='COUNT')
        return response['Count']
    except Exception as e:
        print(f"Error getting count for table {table_name}: {e}")
        # Re-raise the exception so the calling function can handle it
        raise

def get_active_crawlers():
    """Get a list of active crawlers."""
    try:
        table = dynamodb.Table('crawler-status')
        response = table.scan(
            FilterExpression="#status = :status",
            ExpressionAttributeNames={
                '#status': 'status'
            },
            ExpressionAttributeValues={
                ':status': 'active'
            }
        )
        return response.get('Items', [])
    except Exception as e:
        print(f"Error getting active crawlers: {e}")
        return []

def perform_search(query, max_results=10):
    """
    Search the indexed-documents and crawl-metadata tables in DynamoDB for the query.
    Args:
        query (str): The search query
        max_results (int): Maximum number of results to return
    Returns:
        list: Search results
    """
    try:
        print(f"Searching DynamoDB for: {query}")
        results = []
        # First try the indexed-documents table
        try:
            table = dynamodb.Table('indexed-documents')
            response = table.scan()
            for item in response.get('Items', []):
                score = 0
                if query.lower() in item.get('title', '').lower():
                    score = 3
                elif query.lower() in item.get('description', '').lower():
                    score = 2
                if score > 0:
                    results.append({
                        'url': item['url'],
                        'title': item.get('title', 'No title'),
                        'description': item.get('description', 'No description available'),
                        'indexed_at': item.get('indexed_at', 'Unknown'),
                        'score': score
                    })
            results.sort(key=lambda x: x['score'], reverse=True)
            if results:
                return results[:max_results]
        except Exception as e:
            print(f"Error searching indexed-documents: {e}")
        # If no results or error, try crawl-metadata table
        try:
            table = dynamodb.Table('crawl-metadata')
            response = table.scan()
            for item in response.get('Items', []):
                score = 0
                if query.lower() in item.get('title', '').lower():
                    score = 3
                elif query.lower() in item.get('description', '').lower():
                    score = 2
                if score > 0:
                    results.append({
                        'url': item['url'],
                        'title': item.get('title', 'No title'),
                        'description': item.get('description', 'No description available'),
                        'indexed_at': item.get('crawl_time', 'Unknown'),
                        'score': score
                    })
            results.sort(key=lambda x: x['score'], reverse=True)
            return results[:max_results]
        except Exception as e:
            print(f"Error searching crawl-metadata: {e}")
        return []
    except Exception as e:
        print(f"Error searching using DynamoDB: {e}")
        return []

def download_whoosh_index_from_s3(local_dir):
    """
    Download the Whoosh index from S3 to a local directory.
    Args:
        local_dir (str): Local directory to download the index to
    """
    try:
        print(f"Downloading Whoosh index from S3 bucket: {INDEX_DATA_BUCKET}")
        response = s3.list_objects_v2(Bucket=INDEX_DATA_BUCKET, Prefix='whoosh_index/')
        if 'Contents' not in response:
            print(f"No index files found in S3 bucket: {INDEX_DATA_BUCKET}")
            return False
        for obj in response['Contents']:
            key = obj['Key']
            local_path = os.path.join(local_dir, os.path.relpath(key, 'whoosh_index/'))
            local_dir_path = os.path.dirname(local_path)
            os.makedirs(local_dir_path, exist_ok=True)
            print(f"Downloading {key} to {local_path}")
            s3.download_file(INDEX_DATA_BUCKET, key, local_path)
        print(f"Successfully downloaded Whoosh index to {local_dir}")
        return True
    except Exception as e:
        print(f"Error downloading Whoosh index from S3: {e}")
        import traceback
        traceback.print_exc()
        return False

def preprocess_query(query):
    """Preprocess the query for better search results."""
    # Convert to lowercase
    query = query.lower()
    
    # Remove extra whitespace
    query = ' '.join(query.split())
    
    # Remove special characters
    import re
    query = re.sub(r'[^\w\s]', ' ', query)
    
    # Remove stop words
    stop_words = {'a', 'an', 'the', 'and', 'or', 'but', 'is', 'are', 'in', 'on', 'at', 
                 'to', 'for', 'with', 'by', 'about', 'as', 'of', 'from', 'this', 'that', 
                 'these', 'those', 'it', 'its', 'they', 'them', 'their', 'we', 'us', 'our'}
    
    query_words = query.split()
    filtered_words = [word for word in query_words if word not in stop_words]
    
    # If all words were stop words, keep the original query
    if not filtered_words and query_words:
        filtered_words = query_words
    
    # Apply stemming
    try:
        from nltk.stem import PorterStemmer
        stemmer = PorterStemmer()
        stemmed_words = [stemmer.stem(word) for word in filtered_words]
        
        # Combine original and stemmed words for better matching
        # This preserves the original terms while also allowing for stemmed matches
        combined_words = list(set(filtered_words + stemmed_words))
        query = ' '.join(combined_words)
    except ImportError:
        # If NLTK is not available, just use the filtered words
        query = ' '.join(filtered_words)
        print("NLTK not available for stemming, using basic preprocessing only")
    
    return query

def apply_ranking_factors(results, query):
    """Apply additional ranking factors to improve search relevance."""
    query_terms = query.split()
    
    for result in results:
        # Initialize additional score components
        title_match_score = 0
        freshness_score = 0
        
        # Boost exact title matches
        if 'title' in result and result['title']:
            title_lower = result['title'].lower()
            if query in title_lower:
                title_match_score += 5
            
            # Count term frequency in title
            for term in query_terms:
                if term in title_lower:
                    title_match_score += 1
        
        # Boost fresher content if indexed_at is available
        if 'indexed_at' in result and result['indexed_at'] and result['indexed_at'] != 'Unknown':
            try:
                indexed_time = datetime.fromisoformat(result['indexed_at'])
                now = datetime.now()
                if isinstance(indexed_time, datetime):
                    # Content indexed within last day gets a boost
                    age_in_days = (now - indexed_time).days
                    if age_in_days < 1:
                        freshness_score = 3
                    elif age_in_days < 7:
                        freshness_score = 2
                    elif age_in_days < 30:
                        freshness_score = 1
            except (ValueError, TypeError):
                pass
        
        # Apply the additional scores
        result['score'] = result.get('score', 0) + title_match_score + freshness_score
    
    # Re-sort by the updated scores
    results.sort(key=lambda x: x.get('score', 0), reverse=True)
    
    return results

def get_latest_index_metadata():
    """Get the latest index metadata from DynamoDB."""
    try:
        table = dynamodb.Table('index-metadata')
        
        # First get all indexer_ids
        response = table.scan(
            ProjectionExpression="indexer_id",
            Select="SPECIFIC_ATTRIBUTES"
        )
        
        if not response.get('Items'):
            return None
            
        # Get the most recent indexer_id (you might want to implement a better strategy)
        indexer_ids = set(item['indexer_id'] for item in response['Items'])
        if not indexer_ids:
            return None
            
        # Just use the first indexer_id for simplicity
        indexer_id = next(iter(indexer_ids))
        
        # Now query for the latest item with this indexer_id
        query_response = table.query(
            KeyConditionExpression="indexer_id = :id",
            ExpressionAttributeValues={
                ":id": indexer_id
            },
            Limit=1,
            ScanIndexForward=False  # This is valid for query operations
        )
        
        if query_response.get('Items'):
            return query_response['Items'][0]
        return None
    except Exception as e:
        print(f"Error getting latest index metadata: {e}")
        return None

def search_using_s3_index(query, index_metadata):
    """Search using the index stored in S3."""
    try:
        # Get the index and documents from S3
        index_key = index_metadata.get('index_key')
        documents_key = index_metadata.get('documents_key')
        
        if not index_key or not documents_key:
            return []
        
        # Load the index from S3
        index_response = s3.get_object(Bucket=INDEX_DATA_BUCKET, Key=index_key)
        index_data = json.loads(index_response['Body'].read().decode('utf-8'))
        
        # Load the documents from S3
        documents_response = s3.get_object(Bucket=INDEX_DATA_BUCKET, Key=documents_key)
        documents_data = json.loads(documents_response['Body'].read().decode('utf-8'))
        
        # If no documents, return empty results
        if not documents_data:
            print("No documents found in the index")
            return []
            
        # Tokenize the query
        query_terms = [term.lower() for term in query.split() if term.strip()]
        
        # Calculate scores (simple term frequency)
        scores = {}
        for term in query_terms:
            if term in index_data:
                for url, count in index_data[term].items():
                    if url not in scores:
                        scores[url] = 0
                    scores[url] += count
        
        # If no matches found, return some default results
        if not scores:
            print("No matches found for query terms, returning default results")
            # Return the first 5 documents as default results
            default_urls = list(documents_data.keys())[:5]
            for url in default_urls:
                scores[url] = 1  # Give them all the same score
        
        # Sort by score
        results = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # Format results
        formatted_results = []
        for url, score in results:
            if url in documents_data:
                # Get additional metadata from DynamoDB if available
                metadata = get_url_metadata(url)
                
                result = {
                    'url': url,
                    'title': documents_data[url].get('title', 'No title'),
                    'score': score
                }
                
                # Add metadata if available
                if metadata:
                    result.update({
                        'description': metadata.get('description', 'No description available'),
                        'indexed_at': metadata.get('indexed_at', 'Unknown')
                    })
                else:
                    result['description'] = documents_data[url].get('description', 'No description available')
                
                formatted_results.append(result)
        
        return formatted_results
    except Exception as e:
        print(f"Error searching using S3 index: {e}")
        return []

def search_using_dynamodb(query):
    """Search directly using DynamoDB (fallback method)."""
    try:
        # Tokenize the query for better matching
        query_terms = [term.lower() for term in query.split() if term.strip()]
        if not query_terms:
            return []
            
        results = []
        
        # First try the indexed-documents table
        try:
            table = dynamodb.Table('indexed-documents')
            
            # Scan the table for potential matches
            response = table.scan(
                Limit=50  # Increase limit to get more potential matches
            )
            
            for item in response.get('Items', []):
                # Calculate a more sophisticated score
                score = 0
                title = item.get('title', '').lower()
                description = item.get('description', '').lower()
                
                # Check for exact matches first
                if query.lower() in title:
                    score += 10
                if query.lower() in description:
                    score += 5
                    
                # Then check for individual term matches
                for term in query_terms:
                    if term in title:
                        score += 3
                    if term in description:
                        score += 1
                
                # Only include results with a positive score
                if score > 0:
                    results.append({
                        'url': item['url'],
                        'title': item.get('title', 'No title'),
                        'description': item.get('description', 'No description available'),
                        'indexed_at': item.get('indexed_at', 'Unknown'),
                        'score': score
                    })
            
            # If we have enough results, return them
            if len(results) >= 5:
                results.sort(key=lambda x: x['score'], reverse=True)
                return results[:10]  # Return top 10 results
                
        except Exception as e:
            print(f"Error searching indexed-documents: {e}")
        
        # If no results or error, try crawl-metadata table
        try:
            table = dynamodb.Table('crawl-metadata')
            
            # Scan the table for potential matches
            response = table.scan(
                Limit=50  # Increase limit to get more potential matches
            )
            
            for item in response.get('Items', []):
                # Calculate a more sophisticated score
                score = 0
                title = item.get('title', '').lower()
                description = item.get('description', '').lower()
                
                # Check for exact matches first
                if query.lower() in title:
                    score += 10
                if query.lower() in description:
                    score += 5
                    
                # Then check for individual term matches
                for term in query_terms:
                    if term in title:
                        score += 3
                    if term in description:
                        score += 1
                
                # Only include results with a positive score
                if score > 0:
                    results.append({
                        'url': item['url'],
                        'title': item.get('title', 'No title'),
                        'description': item.get('description', 'No description available'),
                        'indexed_at': item.get('crawl_time', 'Unknown'),
                        'score': score
                    })
            
        except Exception as e:
            print(f"Error searching crawl-metadata: {e}")
        
        # Sort by score and return top results
        if results:
            results.sort(key=lambda x: x['score'], reverse=True)
            return results[:10]  # Return top 10 results
            
        # If all else fails, return empty results
        return []
    except Exception as e:
        print(f"Error searching using DynamoDB: {e}")
        return []

def get_url_metadata(url):
    """Get metadata for a URL from DynamoDB."""
    try:
        table = dynamodb.Table('crawl-metadata')
        response = table.get_item(
            Key={'url': url}
        )
        
        if 'Item' in response:
            return response['Item']
        return None
    except Exception as e:
        print(f"Error getting URL metadata: {e}")
        return None

@app.route('/submit-urls', methods=['POST'])
def submit_urls():
    """Handle submission of multiple URLs to crawl by sending them to the master node."""
    try:
        # Get URLs from form
        urls_text = request.form.get('urls', '')
        max_depth = int(request.form.get('max_depth', 3))
        max_urls_per_domain = int(request.form.get('max_urls_per_domain', 100))
        
        # Split by newline, comma, or space and clean up
        urls = []
        for separator in ['\n', ',', ' ']:
            if separator in urls_text:
                urls.extend([url.strip() for url in urls_text.split(separator) if url.strip()])
                break
        else:
            # If no separator found, treat as a single URL
            if urls_text.strip():
                urls = [urls_text.strip()]
        
        # Validate URLs
        valid_urls = []
        invalid_urls = []
        
        for url in urls:
            # Ensure URL has a scheme
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            
            # Basic validation
            import re
            url_pattern = re.compile(
                r'^(https?://)'  # http:// or https://
                r'([a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+' # domain
                r'[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?' # domain
                r'(/[a-zA-Z0-9._~:/?#[\]@!$&\'()*+,;=]*)?' # path
                r'$'
            )
            
            if url_pattern.match(url):
                valid_urls.append(url)
            else:
                invalid_urls.append(url)
        
        # Submit valid URLs to the master node
        submitted_urls = []
        for url in valid_urls:
            if submit_url_to_master(url, max_depth, max_urls_per_domain):
                submitted_urls.append(url)
        
        # Return results
        return jsonify({
            'success': True,
            'message': f'Submitted {len(submitted_urls)} URLs to master node',
            'submitted_urls': submitted_urls,
            'invalid_urls': invalid_urls,
            'failed_urls': [url for url in valid_urls if url not in submitted_urls]
        })
    
    except Exception as e:
        import traceback
        print(f"Error submitting URLs: {e}")
        print(traceback.format_exc())
        return jsonify({
            'success': False,
            'message': f'Error submitting URLs: {str(e)}'
        }), 500

def submit_url_to_master(url, max_depth, max_urls_per_domain):
    """Submit a URL to the master node for crawling."""
    try:
        # Create a message for the master node
        message = {
            'action': 'start_crawl',
            'url': url,
            'max_depth': max_depth,
            'max_urls_per_domain': max_urls_per_domain,
            'timestamp': datetime.now().isoformat(),
            'source': 'web_interface'
        }
        
        # Send to the master node queue
        sqs = boto3.client('sqs', region_name=AWS_REGION)
        
        # Get the master node queue URL
        queue_url_response = sqs.get_queue_url(QueueName=MASTER_COMMAND_QUEUE)
        queue_url = queue_url_response['QueueUrl']
        
        # Send the message
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message)
        )
        
        print(f"Successfully submitted URL to master node: {url}")
        return True
    
    except Exception as e:
        print(f"Error submitting URL to master node: {url}, error: {e}")
        import traceback
        print(traceback.format_exc())
        return False

@app.route('/debug-info',endpoint='debug_system_v1')
def debug_system():
    """Debug endpoint for system information."""
    if not request.args.get('key') == 'debug123':  # Simple authentication
        return jsonify({'error': 'Unauthorized'}), 401
    
    debug_info = {
        'system_stats': get_system_stats(),
        'cache_info': {
            'size': len(search_cache),
            'queries': list(search_cache.keys())
        },
        'tables': debug_tables(),
        'index_metadata': get_latest_index_metadata()
    }
    
    return jsonify(debug_info)

@app.route('/debug/tables', endpoint='debug_tables_route_v1')
def debug_tables():
    """Get information about DynamoDB tables for debugging."""
    tables_info = {}
    try:
        # List all tables
        tables = dynamodb.meta.client.list_tables()['TableNames']
        
        for table_name in tables:
            try:
                # Get item count
                table = dynamodb.Table(table_name)
                count = table.scan(Select='COUNT')['Count']
                
                # Get table status
                description = dynamodb.meta.client.describe_table(TableName=table_name)
                status = description['Table']['TableStatus']
                
                tables_info[table_name] = {
                    'count': count,
                    'status': status
                }
            except Exception as e:
                tables_info[table_name] = {'error': str(e)}
    except Exception as e:
        tables_info['error'] = str(e)
    
    return tables_info

@app.route('/debug/tables', endpoint='debug_tables_route_v2')
def debug_tables():
    """Debug endpoint to check DynamoDB tables."""
    try:
        tables = {}
        
        # Check indexed-documents table
        try:
            count = get_table_count('indexed-documents')
            tables['indexed-documents'] = {
                'exists': True,
                'count': count
            }
        except Exception as e:
            tables['indexed-documents'] = {
                'exists': False,
                'error': str(e)
            }
        
        # Check crawl-metadata table
        try:
            count = get_table_count('crawl-metadata')
            tables['crawl-metadata'] = {
                'exists': True,
                'count': count
            }
        except Exception as e:
            tables['crawl-metadata'] = {
                'exists': False,
                'error': str(e)
            }
        
        return jsonify({
            'tables': tables,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        })

@app.route('/suggest')
def suggest():
    """Provide search suggestions based on query prefix."""
    prefix = request.args.get('q', '').lower()
    if not prefix or len(prefix) < 2:
        return jsonify([])
    
    try:
        # Get suggestions from search analytics
        table = dynamodb.Table('search-analytics')
        
        # Scan for queries that start with the prefix
        response = table.scan(
            FilterExpression="begins_with(#q, :prefix)",
            ExpressionAttributeNames={
                '#q': 'query'
            },
            ExpressionAttributeValues={
                ':prefix': prefix
            },
            ProjectionExpression='query, results_count'
        )
        
        # Extract unique queries
        queries = {}
        for item in response.get('Items', []):
            query = item.get('query')
            if query:
                # Count occurrences and track if it had results
                if query not in queries:
                    queries[query] = {
                        'count': 1,
                        'has_results': item.get('results_count', 0) > 0
                    }
                else:
                    queries[query]['count'] += 1
        
        # Sort by frequency and filter out queries with no results
        suggestions = [
            query for query, data in sorted(
                queries.items(), 
                key=lambda x: x[1]['count'], 
                reverse=True
            ) if data['has_results']
        ]
        
        # Return top 5 suggestions
        return jsonify(suggestions[:5])
    except Exception as e:
        print(f"Error getting suggestions: {e}")
        return jsonify([])

@app.route('/debug-info', endpoint='debug_system_v2')
def debug_system():
    """Debug endpoint for system information."""
    if not request.args.get('key') == 'debug123':  # Simple authentication
        return jsonify({'error': 'Unauthorized'}), 401
    
    debug_info = {
        'system_stats': get_system_stats(),
        'cache_info': {
            'size': len(search_cache),
            'queries': list(search_cache.keys())
        },
        'tables': debug_tables(),
        'index_metadata': get_latest_index_metadata()
    }
    
    return jsonify(debug_info)

@app.route('/debug/tables' , endpoint='debug_tables_route_v3' )
def debug_tables():
    """Get information about DynamoDB tables for debugging."""
    tables_info = {}
    try:
        # List all tables
        tables = dynamodb.meta.client.list_tables()['TableNames']
        
        for table_name in tables:
            try:
                # Get item count
                table = dynamodb.Table(table_name)
                count = table.scan(Select='COUNT')['Count']
                
                # Get table status
                description = dynamodb.meta.client.describe_table(TableName=table_name)
                status = description['Table']['TableStatus']
                
                tables_info[table_name] = {
                    'count': count,
                    'status': status
                }
            except Exception as e:
                tables_info[table_name] = {'error': str(e)}
    except Exception as e:
        tables_info['error'] = str(e)
    
    return tables_info

@app.route('/debug/tables', endpoint='debug_tables_route_v4')
def debug_tables():
    """Debug endpoint to check DynamoDB tables."""
    try:
        tables = {}
        
        # Check indexed-documents table
        try:
            count = get_table_count('indexed-documents')
            tables['indexed-documents'] = {
                'exists': True,
                'count': count
            }
        except Exception as e:
            tables['indexed-documents'] = {
                'exists': False,
                'error': str(e)
            }
        
        # Check crawl-metadata table
        try:
            count = get_table_count('crawl-metadata')
            tables['crawl-metadata'] = {
                'exists': True,
                'count': count
            }
        except Exception as e:
            tables['crawl-metadata'] = {
                'exists': False,
                'error': str(e)
            }
        
        return jsonify({
            'tables': tables,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        })

def search_indexed_content(query):
    """Search for indexed content in DynamoDB."""
    try:
        if not query:
            return {"results": []}
            
        # First try the indexed-documents table
        try:
            table = dynamodb.Table('indexed-documents')
            
            # This is a simplified search that just looks for the query in the title
            response = table.scan()
            
            results = []
            for item in response.get('Items', []):
                # Simple scoring - if query is in title or description, give higher score
                score = 0
                if query.lower() in item.get('title', '').lower():
                    score = 3
                elif query.lower() in item.get('description', '').lower():
                    score = 2
                
                if score > 0:
                    results.append({
                        'url': item['url'],
                        'title': item.get('title', 'No title'),
                        'description': item.get('description', 'No description available'),
                        'indexed_at': item.get('indexed_at', 'Unknown'),
                        'score': score
                    })
            
            # Sort by score
            results.sort(key=lambda x: x['score'], reverse=True)
            
            if results:
                return {"results": results}
        except Exception as e:
            print(f"Error searching indexed-documents: {e}")
        
        # If no results or error, try crawl-metadata table
        try:
            table = dynamodb.Table('crawl-metadata')
            
            response = table.scan()
            
            results = []
            for item in response.get('Items', []):
                # Simple scoring - if query is in title or description, give higher score
                score = 0
                if query.lower() in item.get('title', '').lower():
                    score = 3
                elif query.lower() in item.get('description', '').lower():
                    score = 2
                
                if score > 0:
                    results.append({
                        'url': item['url'],
                        'title': item.get('title', 'No title'),
                        'description': item.get('description', 'No description available'),
                        'indexed_at': item.get('crawl_time', 'Unknown'),
                        'score': score
                    })
            
            # Sort by score
            results.sort(key=lambda x: x['score'], reverse=True)
            
            return {"results": results}
        except Exception as e:
            print(f"Error searching crawl-metadata: {e}")
        
        # If all else fails, return empty results
        return {"results": []}
    except Exception as e:
        print(f"Error searching using DynamoDB: {e}")
        return {"results": []}

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Start the search interface')
    parser.add_argument('--port', type=int, default=5000, help='Port to run the server on')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode')
    args = parser.parse_args()
    print(f"Starting search interface on port {args.port}")
    app.run(host='0.0.0.0', port=args.port, debug=args.debug)