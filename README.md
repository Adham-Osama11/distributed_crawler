# Distributed Web Crawler System

A scalable, fault-tolerant distributed web crawling and indexing system built with Python, AWS services, and modern web technologies.

## 🌟 Features

- **Distributed Architecture**: Scalable crawler nodes that can be deployed across multiple machines
- **Fault Tolerance**: Automatic recovery from node failures and task retries
- **Real-time Search**: Built-in search interface with advanced query capabilities
- **AWS Integration**: Leverages AWS services (S3, DynamoDB, SQS) for reliable storage and messaging
- **Web Interface**: Modern, responsive web UI for searching and submitting URLs
- **Analytics**: Tracks crawling statistics and search analytics
- **Politeness**: Respects robots.txt and implements crawl delays
- **Content Processing**: Advanced text processing and indexing using Whoosh

## 🏗️ Architecture

The system consists of several key components:

- **Master Node**: Coordinates crawling tasks and manages worker nodes
- **Crawler Nodes**: Distributed workers that crawl web pages
- **Indexer Node**: Processes and indexes crawled content
- **Web Interface**: User-friendly search interface and URL submission portal

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- AWS Account with appropriate permissions
- AWS CLI configured with credentials

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/distributed-web-crawler.git
cd distributed-web-crawler
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure AWS credentials:
```bash
aws configure
```

### Configuration

1. Set up AWS resources:
   - Create S3 buckets for crawl data and index storage
   - Create DynamoDB tables for metadata and analytics
   - Create SQS queues for task distribution

2. Update configuration in `common/config.py`:
```python
AWS_REGION = 'your-region'
CRAWL_DATA_BUCKET = 'your-crawl-bucket'
INDEX_DATA_BUCKET = 'your-index-bucket'
```

## 🏃‍♂️ Running the System

1. Start the master node:
```bash
python src/master/master_node.py
```

2. Start crawler nodes (on different machines if desired):
```bash
python src/crawler/crawler_node.py
```

3. Start the indexer node:
```bash
python src/indexer/indexer_node.py
```

4. Launch the web interface:
```bash
python src/client/search_interface.py
```

## 🔍 Usage

### Web Interface

Access the web interface at `http://localhost:5000` to:
- Search through crawled content
- Submit new URLs for crawling
- View system statistics
- Monitor crawling progress

### API Endpoints

- `GET /search?q=<query>`: Search the index
- `POST /submit-url`: Submit a URL for crawling
- `GET /stats`: Get system statistics
- `GET /suggest`: Get search suggestions

## 📊 Monitoring

The system provides several monitoring capabilities:
- Real-time statistics in the web interface
- Detailed logs for each component
- AWS CloudWatch metrics
- DynamoDB analytics

## 🔧 Development

### Project Structure

```
src/
├── master/         # Master node implementation
├── crawler/        # Crawler node implementation
├── indexer/        # Indexer node implementation
├── client/         # Web interface and API
└── common/         # Shared utilities and configuration
```

### Adding New Features

1. Fork the repository
2. Create a feature branch
3. Implement your changes
4. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📫 Contact

For questions and support, please open an issue in the GitHub repository.

## 🙏 Acknowledgments

- [Scrapy](https://scrapy.org/) for the web crawling framework
- [Whoosh](https://whoosh.readthedocs.io/) for the search indexing
- [AWS](https://aws.amazon.com/) for cloud infrastructure
- [Flask](https://flask.palletsprojects.com/) for the web interface 