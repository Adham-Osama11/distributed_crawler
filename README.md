<!-- README.md -->

# Distributed Web Crawling & Indexing System

## Overview
This repository contains the skeleton for a distributed web crawling and indexing system.  
It defines three main components:
- **MasterNode**: Schedules URL‐crawl tasks, monitors crawler health, handles task re‐queueing.  
- **CrawlerNode**: Fetches web pages, parses links & text, reports results back to Master.  
- **IndexerNode**: Consumes processed text, builds/merges inverted index, serves search queries.

## Directory Structure
distributed_crawler/ ├── README.md ├── requirements.txt ├── .gitignore ├── src/ │ ├── init.py │ ├── master_node.py │ ├── crawler_node.py │ └── indexer_node.py
                                                                       └── tests/ ├── init.py ├── test_master.py ├── test_crawler.py └── test_indexer.py


## Setup

1. **Clone & enter** the repo:
    ```bash
    git clone <your-repo-url>
    cd distributed_crawler
2. **Create a virtual environment** (optional but recommended):
    ```bash
    python3 -m venv venv
    source venv/bin/activate

3.**Install dependencies**:
    ```bash
    
    pip install -r requirements.txt

