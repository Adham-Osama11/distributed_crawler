"""
Search interface for the distributed web crawling system.
Provides both command-line and web-based interfaces for searching the index.
"""
import argparse
import sys
import os
import json
import webbrowser
from flask import Flask, request, jsonify, render_template, send_from_directory
from datetime import datetime

# Add the parent directory to the path so we can import indexer
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from indexer.indexer_node import WhooshIndex
from common.config import AWS_REGION, INDEX_DATA_BUCKET

def load_index():
    """Load the index from disk."""
    try:
        # Try to load the Whoosh index
        index_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'whoosh_index')
        if os.path.exists(index_dir) and os.path.isdir(index_dir):
            index = WhooshIndex(index_dir=index_dir)
            print(f"Loaded Whoosh index from {index_dir}")
            return index
    except Exception as e:
        print(f"Error loading Whoosh index: {e}")
    
    print("Failed to load index. Make sure the crawler has indexed some pages.")
    return None

def format_results_for_cli(results, query):
    """Format search results for command-line display."""
    if not results:
        return f"No results found for '{query}'"
    
    output = [f"Search results for '{query}':"]
    output.append("-" * 80)
    
    for i, result in enumerate(results, 1):
        title = result.get('title', 'No Title')
        url = result.get('url', 'No URL')
        score = result.get('score', 0)
        description = result.get('description', '')
        
        # Truncate description if it's too long
        if len(description) > 200:
            description = description[:197] + "..."
        
        output.append(f"{i}. {title} (Score: {score:.2f})")
        output.append(f"   URL: {url}")
        if description:
            output.append(f"   Description: {description}")
        output.append("-" * 80)
    
    return "\n".join(output)

def cli_search():
    """Run the command-line search interface."""
    parser = argparse.ArgumentParser(description='Search the web index')
    parser.add_argument('query', nargs='?', help='Search query')
    parser.add_argument('--max-results', type=int, default=10, help='Maximum number of results to return')
    parser.add_argument('--web', action='store_true', help='Launch web interface instead of CLI')
    parser.add_argument('--port', type=int, default=5000, help='Port for web interface')
    args = parser.parse_args()
    
    if args.web:
        # Launch web interface
        start_web_interface(port=args.port)
        return
    
    if not args.query:
        parser.print_help()
        return
    
    # Load the index
    index = load_index()
    if not index:
        return
    
    # Search the index
    results = index.search(args.query, args.max_results)
    
    # Display results
    print(format_results_for_cli(results, args.query))

# Web interface
app = Flask(__name__)

@app.route('/')
def home():
    """Render the search page."""
    return render_template('search.html')

@app.route('/api/search', methods=['POST'])
def search_api():
    """API endpoint for search."""
    data = request.json
    query = data.get('query', '')
    max_results = data.get('max_results', 10)
    
    if not query:
        return jsonify({'error': 'No query provided'}), 400
    
    # Load the index
    index = load_index()
    if not index:
        return jsonify({'error': 'Failed to load index'}), 500
    
    # Search the index
    results = index.search(query, max_results)
    
    # Add timestamp
    response = {
        'query': query,
        'results': results,
        'result_count': len(results),
        'timestamp': datetime.now().isoformat()
    }
    
    return jsonify(response)

@app.route('/static/<path:path>')
def send_static(path):
    """Serve static files."""
    return send_from_directory('static', path)

def create_template_files():
    """Create necessary template and static files if they don't exist."""
    # Create templates directory
    templates_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
    os.makedirs(templates_dir, exist_ok=True)
    
    # Create static directory
    static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
    os.makedirs(static_dir, exist_ok=True)
    
    # Create search.html template
    search_html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Distributed Web Crawler - Search</title>
        <link rel="stylesheet" href="/static/style.css">
        <meta name="viewport" content="width=device-width, initial-scale=1">
    </head>
    <body>
        <div class="container">
            <h1>Web Search</h1>
            <div class="search-container">
                <input type="text" id="search-input" placeholder="Enter your search query...">
                <button id="search-button">Search</button>
            </div>
            <div id="results-container"></div>
        </div>
        <script src="/static/script.js"></script>
    </body>
    </html>
    """
    
    with open(os.path.join(templates_dir, 'search.html'), 'w') as f:
        f.write(search_html)
    
    # Create style.css
    style_css = """
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
    
    h1 {
        text-align: center;
        color: #333;
    }
    
    .search-container {
        display: flex;
        margin-bottom: 20px;
    }
    
    #search-input {
        flex: 1;
        padding: 10px;
        font-size: 16px;
        border: 1px solid #ddd;
        border-radius: 4px 0 0 4px;
    }
    
    #search-button {
        padding: 10px 20px;
        background-color: #4285f4;
        color: white;
        border: none;
        border-radius: 0 4px 4px 0;
        cursor: pointer;
    }
    
    #search-button:hover {
        background-color: #357ae8;
    }
    
    .result-item {
        background-color: white;
        padding: 15px;
        margin-bottom: 15px;
        border-radius: 4px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    
    .result-title {
        color: #1a0dab;
        font-size: 18px;
        margin-bottom: 5px;
    }
    
    .result-url {
        color: #006621;
        font-size: 14px;
        margin-bottom: 5px;
    }
    
    .result-description {
        color: #545454;
        font-size: 14px;
    }
    
    .result-score {
        color: #888;
        font-size: 12px;
        text-align: right;
    }
    """
    
    with open(os.path.join(static_dir, 'style.css'), 'w') as f:
        f.write(style_css)
    
    # Create script.js
    script_js = """
    document.addEventListener('DOMContentLoaded', function() {
        const searchInput = document.getElementById('search-input');
        const searchButton = document.getElementById('search-button');
        const resultsContainer = document.getElementById('results-container');
        
        // Search when button is clicked
        searchButton.addEventListener('click', performSearch);
        
        // Search when Enter key is pressed
        searchInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                performSearch();
            }
        });
        
        function performSearch() {
            const query = searchInput.value.trim();
            if (!query) return;
            
            // Show loading indicator
            resultsContainer.innerHTML = '<p>Searching...</p>';
            
            // Perform search request
            fetch('/api/search', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    query: query,
                    max_results: 20
                })
            })
            .then(response => response.json())
            .then(data => {
                displayResults(data);
            })
            .catch(error => {
                resultsContainer.innerHTML = `<p>Error: ${error.message}</p>`;
            });
        }
        
        function displayResults(data) {
            if (data.error) {
                resultsContainer.innerHTML = `<p>Error: ${data.error}</p>`;
                return;
            }
            
            if (!data.results || data.results.length === 0) {
                resultsContainer.innerHTML = `<p>No results found for "${data.query}"</p>`;
                return;
            }
            
            let html = `<h2>Search Results for "${data.query}" (${data.result_count} results)</h2>`;
            
            data.results.forEach((result, index) => {
                html += `
                <div class="result-item">
                    <div class="result-title">${index + 1}. ${result.title || 'No Title'}</div>
                    <div class="result-url"><a href="${result.url}" target="_blank">${result.url}</a></div>
                    <div class="result-description">${result.description || 'No description available'}</div>
                    <div class="result-score">Score: ${result.score.toFixed(2)}</div>
                </div>
                `;
            });
            
            resultsContainer.innerHTML = html;
        }
    });
    """
    
    with open(os.path.join(static_dir, 'script.js'), 'w') as f:
        f.write(script_js)

def start_web_interface(port=5000):
    """Start the web interface."""
    create_template_files()
    print(f"Starting web interface on http://localhost:{port}")
    webbrowser.open(f"http://localhost:{port}")
    app.run(host='0.0.0.0', port=port)

def main():
    """Main function to run the search interface."""
    cli_search()

if __name__ == "__main__":
    main()