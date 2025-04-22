"""
Simple search interface for the distributed web crawling system.
"""
import argparse
import sys
import os
import json

# Add the parent directory to the path so we can import indexer
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from indexer.indexer_node import SimpleIndex

def load_index():
    """Load the index from disk."""
    index = SimpleIndex()
    if index.load_from_disk():
        return index
    else:
        print("Failed to load index. Make sure the crawler has indexed some pages.")
        return None

def main():
    """Main function to run the search interface."""
    parser = argparse.ArgumentParser(description='Search the web index')
    parser.add_argument('query', help='Search query')
    parser.add_argument('--max-results', type=int, default=10, help='Maximum number of results to return')
    args = parser.parse_args()
    
    # Load the index
    index = load_index()
    if not index:
        return
    
    # Search the index
    results = index.search(args.query, args.max_results)
    
    if not results:
        print(f"No results found for '{args.query}'")
        return
    
    # Display results
    print(f"Search results for '{args.query}':")
    print("-" * 80)
    for i, result in enumerate(results, 1):
        print(f"{i}. {result['title']} (Score: {result['score']})")
        print(f"   URL: {result['url']}")
        print("-" * 80)

if __name__ == "__main__":
    main()