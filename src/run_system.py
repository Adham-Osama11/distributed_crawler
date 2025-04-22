"""
Main script to run the distributed web crawling system.
"""
import argparse
import subprocess
import time
import os
import sys
import signal

def start_master():
    """Start the master node."""
    print("Starting master node...")
    master_process = subprocess.Popen(
        [sys.executable, 'master/master_node.py'],
        cwd=os.path.dirname(os.path.abspath(__file__))
    )
    return master_process

def start_crawler(crawler_id):
    """Start a crawler node."""
    print(f"Starting crawler node {crawler_id}...")
    crawler_process = subprocess.Popen(
        [sys.executable, 'crawler/crawler_node.py'],
        cwd=os.path.dirname(os.path.abspath(__file__))
    )
    return crawler_process

def start_indexer():
    """Start the indexer node."""
    print("Starting indexer node...")
    indexer_process = subprocess.Popen(
        [sys.executable, 'indexer/indexer_node.py'],
        cwd=os.path.dirname(os.path.abspath(__file__))
    )
    return indexer_process

def main():
    """Main function to run the system."""
    parser = argparse.ArgumentParser(description='Run the distributed web crawling system')
    parser.add_argument('--crawlers', type=int, default=2, help='Number of crawler nodes to start')
    parser.add_argument('--seed-urls', nargs='+', help='List of seed URLs to start crawling')
    args = parser.parse_args()
    
    processes = []
    
    try:
        # Start master node
        master_process = start_master()
        processes.append(master_process)
        
        # Give the master node time to initialize
        time.sleep(5)
        
        # Start indexer node
        indexer_process = start_indexer()
        processes.append(indexer_process)
        
        # Start crawler nodes
        for i in range(args.crawlers):
            crawler_process = start_crawler(i)
            processes.append(crawler_process)
        
        # If seed URLs are provided, start a crawl
        if args.seed_urls:
            time.sleep(5)  # Give the system time to initialize
            seed_urls_str = ' '.join(args.seed_urls)
            subprocess.run(
                [sys.executable, 'client/client.py', '--seed-urls'] + args.seed_urls,
                cwd=os.path.dirname(os.path.abspath(__file__))
            )
        
        # Wait for user to press Ctrl+C
        print("\nSystem is running. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("\nShutting down the system...")
        
        # Terminate all processes
        for process in processes:
            process.send_signal(signal.SIGINT)
        
        # Wait for processes to terminate
        for process in processes:
            process.wait()
        
        print("System shutdown complete.")

if __name__ == "__main__":
    main()