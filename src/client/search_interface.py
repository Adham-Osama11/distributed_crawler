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

# Add the parent directory to the path so we can import common modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import AWS_REGION, INDEX_DATA_BUCKET

app = Flask(__name__)

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
    """Handle search requests with caching."""
    query = request.args.get('q', '')
    if not query:
        return jsonify({'results': [], 'message': 'Please enter a search query'})
    
    print(f"Received search query: {query}")
    
    # Check cache first
    cached_results = get_cached_results(query)
    if cached_results:
        print(f"Returning cached results for query: {query}")
        # Track cached query
        track_search_query(query, len(cached_results))
        return jsonify({
            'results': cached_results,
            'count': len(cached_results),
            'query': query,
            'timestamp': datetime.now().isoformat(),
            'cached': True
        })
    
    # If not in cache, perform search
    results = perform_search(query)
    
    # Cache the results
    cache_results(query, results)
    
    # Track the search query
    track_search_query(query, len(results))
    
    print(f"Found {len(results)} results for query: {query}")
    
    # Return results as JSON
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

def perform_search(query):
    """
    Perform a search against the index with improved relevance scoring.
    """
    try:
        # First, try to get the latest index metadata
        index_metadata = get_latest_index_metadata()
        
        # Preprocess the query for better matching
        processed_query = preprocess_query(query)
        
        results = []
        if index_metadata:
            # Use the S3-stored index for searching
            results = search_using_s3_index(processed_query, index_metadata)
        
        # If no results from S3 index, try DynamoDB
        if not results:
            results = search_using_dynamodb(processed_query)
            
        # Apply additional ranking factors
        results = apply_ranking_factors(results, processed_query)
            
        # If still no results, return a default "no results" message
        if not results:
            print("No results found for query: " + query)
            # Create a dummy result to avoid "No results found" message
            results = [{
                'url': '#',
                'title': 'No exact matches found',
                'description': 'Try a different search term or submit a URL for crawling below.',
                'indexed_at': datetime.now().isoformat(),
                'score': 0
            }]
            
        return results
    except Exception as e:
        print(f"Error performing search: {e}")
        # Return a fallback result instead of empty list
        return [{
            'url': '#',
            'title': 'Search Error',
            'description': f'An error occurred while searching: {str(e)}',
            'indexed_at': datetime.now().isoformat(),
            'score': 0
        }]

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

@app.route('/submit-url', methods=['POST'])
def submit_url():
    """Handle URL submission for crawling."""
    data = request.get_json()
    if not data or 'url' not in data:
        return jsonify({'success': False, 'message': 'URL is required'}), 400
    
    url = data['url']
    max_depth = data.get('max_depth', 3)
    max_urls_per_domain = data.get('max_urls_per_domain', 100)
    
    try:
        # Create an SQS client
        sqs = boto3.client('sqs', region_name=AWS_REGION)
        
        # Get the crawl task queue URL
        response = sqs.get_queue_url(QueueName='crawl-task-queue')
        queue_url = response['QueueUrl']
        
        # Create a task message
        task = {
            'action': 'crawl_url',
            'url': url,
            'max_depth': max_depth,
            'max_urls_per_domain': max_urls_per_domain,
            'timestamp': datetime.now().isoformat()
        }
        
        # Send the message to the queue
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(task)
        )
        
        return jsonify({
            'success': True, 
            'message': f'URL submitted for crawling: {url}',
            'url': url,
            'max_depth': max_depth,
            'max_urls_per_domain': max_urls_per_domain
        })
    except Exception as e:
        print(f"Error submitting URL: {e}")
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500

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

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    # Create the HTML template if it doesn't exist
    template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates', 'search.html')
    if not os.path.exists(template_path):
        with open(template_path, 'w') as f:
            f.write("""<!DOCTYPE html>
<html>
<head>
    <title>Distributed Web Crawler - Search</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 0;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
        }
        .search-box {
            text-align: center;
            margin: 40px 0;
        }
        .search-input {
            width: 70%;
            padding: 10px;
            font-size: 16px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        .search-button {
            padding: 10px 20px;
            font-size: 16px;
            background-color: #4285f4;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
        }
        .search-button:hover {
            background-color: #3b78e7;
        }
        .results {
            margin-top: 20px;
        }
        .result-card {
            background-color: white;
            border-radius: 4px;
            padding: 15px;
            margin-bottom: 15px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }
        .result-title {
            color: #1a0dab;
            font-size: 18px;
            margin: 0 0 5px 0;
        }
        .result-url {
            color: #006621;
            font-size: 14px;
            margin: 0 0 10px 0;
        }
        .result-description {
            color: #545454;
            font-size: 14px;
            margin: 0;
        }
        .stats {
            display: flex;
            justify-content: space-between;
            background-color: white;
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }
        .stat-item {
            text-align: center;
        }
        .stat-value {
            font-size: 24px;
            font-weight: bold;
            color: #4285f4;
        }
        .stat-label {
            font-size: 14px;
            color: #545454;
        }
        .loading {
            text-align: center;
            margin: 20px 0;
            display: none;
        }
        .no-results {
            text-align: center;
            margin: 20px 0;
            color: #545454;
        }
        /* Suggestions styling */
        .suggestions {
            position: absolute;
            width: 70%;
            max-height: 200px;
            overflow-y: auto;
            background-color: white;
            border: 1px solid #ddd;
            border-top: none;
            border-radius: 0 0 4px 4px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            z-index: 10;
        }
        
        .suggestion-item {
            padding: 10px 15px;
            cursor: pointer;
            transition: background-color 0.2s;
        }
        
        .suggestion-item:hover {
            background-color: #f5f5f5;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1 style="text-align: center;">Distributed Web Crawler</h1>
        
        <div class="stats">
            <div class="stat-item">
                <div class="stat-value">{{ stats.urls_in_frontier }}</div>
                <div class="stat-label">URLs in Frontier</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">{{ stats.urls_indexed }}</div>
                <div class="stat-label">URLs Indexed</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">{{ stats.active_crawlers }}</div>
                <div class="stat-label">Active Crawlers</div>
            </div>
        </div>
        
        <div class="search-box">
            <input type="text" id="search-input" class="search-input" placeholder="Enter search query...">
            <button id="search-button" class="search-button">Search</button>
        </div>
        
        <div id="loading" class="loading">
            <p>Searching...</p>
        </div>
        
        <div id="results" class="results"></div>
        
        <!-- Add URL submission form -->
        <div class="url-submission" style="margin-top: 40px; background-color: white; padding: 20px; border-radius: 4px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
            <h2 style="text-align: center;">Submit URL for Crawling</h2>
            <div style="display: flex; margin-bottom: 10px;">
                <input type="text" id="url-input" class="search-input" placeholder="Enter URL to crawl..." style="flex-grow: 1; margin-right: 10px;">
                <button id="submit-url-button" class="search-button">Submit</button>
            </div>
            <div style="display: flex; justify-content: space-between;">
                <div>
                    <label for="max-depth">Max Depth:</label>
                    <input type="number" id="max-depth" min="1" max="5" value="3" style="width: 50px;">
                </div>
                <div>
                    <label for="max-urls">Max URLs per Domain:</label>
                    <input type="number" id="max-urls" min="10" max="500" value="100" style="width: 70px;">
                </div>
            </div>
            <div id="url-submission-result" style="margin-top: 10px;"></div>
        </div>
    </div>
    
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const searchInput = document.getElementById('search-input');
            const searchButton = document.getElementById('search-button');
            const resultsDiv = document.getElementById('results');
            const loadingDiv = document.getElementById('loading');
            
            // Set up suggestions
            const suggestionsContainer = document.createElement('div');
            suggestionsContainer.className = 'suggestions';
            suggestionsContainer.style.display = 'none';
            searchInput.parentNode.insertBefore(suggestionsContainer, searchInput.nextSibling);
            
            let debounceTimer;
            
            searchInput.addEventListener('input', function() {
                const query = this.value.trim();
                
                // Clear previous timer
                clearTimeout(debounceTimer);
                
                // Hide suggestions if query is too short
                if (query.length < 2) {
                    suggestionsContainer.style.display = 'none';
                    return;
                }
                
                // Debounce the API call
                debounceTimer = setTimeout(() => {
                    fetch(`/suggest?q=${encodeURIComponent(query)}`)
                        .then(response => response.json())
                        .then(suggestions => {
                            // Clear previous suggestions
                            suggestionsContainer.innerHTML = '';
                            
                            if (suggestions.length > 0) {
                                // Create suggestion elements
                                suggestions.forEach(suggestion => {
                                    const div = document.createElement('div');
                                    div.className = 'suggestion-item';
                                    div.textContent = suggestion;
                                    div.addEventListener('click', () => {
                                        searchInput.value = suggestion;
                                        suggestionsContainer.style.display = 'none';
                                        document.getElementById('search-button').click();
                                    });
                                    suggestionsContainer.appendChild(div);
                                });
                                
                                suggestionsContainer.style.display = 'block';
                            } else {
                                suggestionsContainer.style.display = 'none';
                            }
                        })
                        .catch(error => {
                            console.error('Error fetching suggestions:', error);
                            suggestionsContainer.style.display = 'none';
                        });
                }, 300); // 300ms debounce
            });
            
            // Hide suggestions when clicking outside
            document.addEventListener('click', function(event) {
                if (!searchInput.contains(event.target) && !suggestionsContainer.contains(event.target)) {
                    suggestionsContainer.style.display = 'none';
                }
            });
            
            // Function to perform search
            function performSearch() {
                const query = searchInput.value.trim();
                if (!query) {
                    return;
                }
                
                // Show loading indicator
                loadingDiv.style.display = 'block';
                resultsDiv.innerHTML = '';
                
                // Perform search
                fetch(`/search?q=${encodeURIComponent(query)}`)
                    .then(response => response.json())
                    .then(data => {
                        // Hide loading indicator
                        loadingDiv.style.display = 'none';
                        
                        // Display results
                        if (data.results.length === 0) {
                            resultsDiv.innerHTML = '<div class="no-results">No results found</div>';
                            return;
                        }
                        
                        // Create result cards
                        let resultsHtml = '';
                        data.results.forEach(result => {
                            resultsHtml += `
                                <div class="result-card">
                                    <h3 class="result-title">${result.title || 'No Title'}</h3>
                                    <p class="result-url">${result.url}</p>
                                    <p class="result-description">${result.description || 'No description available'}</p>
                                </div>
                            `;
                        });
                        
                        resultsDiv.innerHTML = resultsHtml;
                    })
                    .catch(error => {
                        loadingDiv.style.display = 'none';
                        resultsDiv.innerHTML = `<div class="no-results">Error: ${error.message}</div>`;
                    });
            }
            
            // Add event listeners
            searchButton.addEventListener('click', performSearch);
            searchInput.addEventListener('keypress', function(e) {
                if (e.key === 'Enter') {
                    performSearch();
                }
            });
            
            // Set up URL submission
            const urlInput = document.getElementById('url-input');
            const submitUrlButton = document.getElementById('submit-url-button');
            const maxDepthInput = document.getElementById('max-depth');
            const maxUrlsInput = document.getElementById('max-urls');
            const urlSubmissionResult = document.getElementById('url-submission-result');
            
            submitUrlButton.addEventListener('click', function() {
                const url = urlInput.value.trim();
                if (!url) {
                    urlSubmissionResult.innerHTML = '<p style="color: red;">Please enter a URL</p>';
                    return;
                }
                
                const maxDepth = maxDepthInput.value;
                const maxUrls = maxUrlsInput.value;
                
                // Disable button during submission
                submitUrlButton.disabled = true;
                urlSubmissionResult.innerHTML = '<p>Submitting URL...</p>';
                
                // Submit URL for crawling
                fetch('/submit-url', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        url: url,
                        max_depth: maxDepth,
                        max_urls_per_domain: maxUrls
                    })
                })
                .then(response => response.json())
                .then(data => {
                    submitUrlButton.disabled = false;
                    if (data.success) {
                        urlSubmissionResult.innerHTML = `<p style="color: green;">${data.message}</p>`;
                        urlInput.value = '';
                    } else {
                        urlSubmissionResult.innerHTML = `<p style="color: red;">${data.message}</p>`;
                    }
                })
                .catch(error => {
                    submitUrlButton.disabled = false;
                    urlSubmissionResult.innerHTML = `<p style="color: red;">Error: ${error.message}</p>`;
                });
            });
            
            // Update stats periodically
            function updateStats() {
                fetch('/stats')
                    .then(response => response.json())
                    .then(data => {
                        document.querySelectorAll('.stat-item').forEach((item, index) => {
                            const statValue = item.querySelector('.stat-value');
                            if (index === 0) statValue.textContent = data.urls_in_frontier;
                            if (index === 1) statValue.textContent = data.urls_indexed;
                            if (index === 2) statValue.textContent = data.active_crawlers;
                        });
                    })
                    .catch(error => console.error('Error updating stats:', error));
            }
            
            // Update stats every 30 seconds
            updateStats(); // Initial update
            setInterval(updateStats, 30000);
        });
    </script>
</body>
</html>""")
    
    # Run the Flask app
app.run(debug=True, port=5000)