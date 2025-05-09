"""
Monitoring system for the distributed web crawler.
Tracks system metrics, crawler health, and performance statistics.
"""
import time
import json
import boto3
import psutil
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import threading
from typing import Dict, List, Any
import numpy as np
from dataclasses import dataclass, asdict
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from plotly.subplots import make_subplots

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Monitoring] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('monitoring.log')
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class CrawlerMetrics:
    """Metrics for a single crawler."""
    crawler_id: str
    status: str
    last_heartbeat: float
    urls_crawled: int
    urls_failed: int
    urls_timeout: int
    avg_response_time: float
    memory_usage: float
    cpu_usage: float
    active_tasks: int
    error_rate: float
    retry_rate: float
    bytes_downloaded: int
    requests_per_minute: float
    success_rate: float
    domain_distribution: Dict[str, int]
    http_status_codes: Dict[int, int]
    error_types: Dict[str, int]
    performance_metrics: Dict[str, float]

class MonitoringSystem:
    def __init__(self):
        """Initialize the monitoring system."""
        self.aws_region = 'us-east-1'
        self.sqs = boto3.client('sqs', region_name=self.aws_region)
        self.dynamodb = boto3.resource('dynamodb', region_name=self.aws_region)
        self.s3 = boto3.client('s3', region_name=self.aws_region)
        
        # Get queue URLs
        self.crawl_result_queue_url = self.sqs.get_queue_url(QueueName='crawl-result-queue')['QueueUrl']
        
        # Initialize metrics storage
        self.crawler_metrics: Dict[str, CrawlerMetrics] = {}
        self.system_metrics = {
            'total_urls_crawled': 0,
            'total_urls_failed': 0,
            'total_urls_timeout': 0,
            'total_bytes_downloaded': 0,
            'system_cpu_usage': 0,
            'system_memory_usage': 0,
            'system_disk_usage': 0,
            'system_network_io': {'bytes_sent': 0, 'bytes_recv': 0},
            'error_rates': defaultdict(list),
            'performance_metrics': defaultdict(list),
            'resource_utilization': defaultdict(list),
            'crawl_history': [],
            'domain_distribution': defaultdict(int),
            'http_status_codes': defaultdict(int),
            'error_types': defaultdict(int)
        }
        
        # Initialize locks for thread safety
        self.metrics_lock = threading.Lock()
        self.history_lock = threading.Lock()
        
        # Initialize performance tracking
        self.performance_window = 300  # 5 minutes
        self.performance_metrics = defaultdict(list)
        
        # Initialize resource monitoring
        self.resource_monitor = ResourceMonitor()
        
        # Initialize error tracking
        self.error_tracker = ErrorTracker()
        
        # Initialize the dashboard
        self.app = dash.Dash(__name__)
        self._setup_dashboard()
        
        logger.info("Monitoring system initialized")

    def _setup_dashboard(self):
        """Set up the monitoring dashboard."""
        self.app.layout = html.Div([
            html.H1('Distributed Web Crawler Monitoring Dashboard'),
            
            # System Overview
            html.Div([
                html.H2('System Overview'),
                html.Div([
                    html.Div([
                        html.H3('Total URLs Crawled'),
                        html.H2(id='total-urls-crawled')
                    ], className='metric-card'),
                    html.Div([
                        html.H3('Active Crawlers'),
                        html.H2(id='active-crawlers')
                    ], className='metric-card'),
                    html.Div([
                        html.H3('Error Rate'),
                        html.H2(id='error-rate')
                    ], className='metric-card'),
                    html.Div([
                        html.H3('Crawl Rate'),
                        html.H2(id='crawl-rate')
                    ], className='metric-card')
                ], className='metrics-grid'),
                
                # Resource Utilization
                html.Div([
                    html.H3('Resource Utilization'),
                    dcc.Graph(id='resource-utilization-graph')
                ]),
                
                # Performance Metrics
                html.Div([
                    html.H3('Performance Metrics'),
                    dcc.Graph(id='performance-metrics-graph')
                ])
            ]),
            
            # Crawler Status
            html.Div([
                html.H2('Crawler Status'),
                html.Div(id='crawler-status-table')
            ]),
            
            # Error Tracking
            html.Div([
                html.H2('Error Tracking'),
                html.Div([
                    html.Div([
                        html.H3('Error Distribution'),
                        dcc.Graph(id='error-distribution-graph')
                    ]),
                    html.Div([
                        html.H3('Error History'),
                        dcc.Graph(id='error-history-graph')
                    ])
                ], className='error-tracking-grid')
            ]),
            
            # Domain Distribution
            html.Div([
                html.H2('Domain Distribution'),
                dcc.Graph(id='domain-distribution-graph')
            ]),
            
            # Crawl History
            html.Div([
                html.H2('Crawl History'),
                dcc.Graph(id='crawl-history-graph')
            ]),
            
            # Auto-refresh interval
            dcc.Interval(
                id='interval-component',
                interval=5*1000,  # 5 seconds
                n_intervals=0
            )
        ])
        
        # Set up callbacks
        self._setup_callbacks()
        
        logger.info("Dashboard setup complete")

    def _setup_callbacks(self):
        """Set up dashboard callbacks."""
        @self.app.callback(
            [Output('total-urls-crawled', 'children'),
             Output('active-crawlers', 'children'),
             Output('error-rate', 'children'),
             Output('crawl-rate', 'children')],
            [Input('interval-component', 'n_intervals')]
        )
        def update_overview_metrics(n):
            with self.metrics_lock:
                total_urls = self.system_metrics['total_urls_crawled']
                active_crawlers = sum(1 for m in self.crawler_metrics.values() 
                                   if time.time() - m.last_heartbeat < 60)
                error_rate = self._calculate_error_rate()
                crawl_rate = self._calculate_crawl_rate()
                
                return [
                    f"{total_urls:,}",
                    f"{active_crawlers}",
                    f"{error_rate:.1f}%",
                    f"{crawl_rate:.1f} URLs/min"
                ]

        @self.app.callback(
            Output('resource-utilization-graph', 'figure'),
            [Input('interval-component', 'n_intervals')]
        )
        def update_resource_utilization(n):
            with self.metrics_lock:
                return self._create_resource_utilization_graph()

        @self.app.callback(
            Output('performance-metrics-graph', 'figure'),
            [Input('interval-component', 'n_intervals')]
        )
        def update_performance_metrics(n):
            with self.metrics_lock:
                return self._create_performance_metrics_graph()

        @self.app.callback(
            Output('crawler-status-table', 'children'),
            [Input('interval-component', 'n_intervals')]
        )
        def update_crawler_status(n):
            with self.metrics_lock:
                return self._create_crawler_status_table()

        @self.app.callback(
            [Output('error-distribution-graph', 'figure'),
             Output('error-history-graph', 'figure')],
            [Input('interval-component', 'n_intervals')]
        )
        def update_error_tracking(n):
            with self.metrics_lock:
                return [
                    self._create_error_distribution_graph(),
                    self._create_error_history_graph()
                ]

        @self.app.callback(
            Output('domain-distribution-graph', 'figure'),
            [Input('interval-component', 'n_intervals')]
        )
        def update_domain_distribution(n):
            with self.metrics_lock:
                return self._create_domain_distribution_graph()

        @self.app.callback(
            Output('crawl-history-graph', 'figure'),
            [Input('interval-component', 'n_intervals')]
        )
        def update_crawl_history(n):
            with self.metrics_lock:
                return self._create_crawl_history_graph()

    def _create_resource_utilization_graph(self):
        """Create resource utilization graph."""
        fig = make_subplots(rows=2, cols=2, subplot_titles=('CPU Usage', 'Memory Usage', 'Disk Usage', 'Network I/O'))
        
        # CPU Usage
        fig.add_trace(
            go.Scatter(
                y=self.system_metrics['resource_utilization']['cpu'],
                name='CPU Usage'
            ),
            row=1, col=1
        )
        
        # Memory Usage
        fig.add_trace(
            go.Scatter(
                y=self.system_metrics['resource_utilization']['memory'],
                name='Memory Usage'
            ),
            row=1, col=2
        )
        
        # Disk Usage
        fig.add_trace(
            go.Scatter(
                y=self.system_metrics['resource_utilization']['disk'],
                name='Disk Usage'
            ),
            row=2, col=1
        )
        
        # Network I/O
        fig.add_trace(
            go.Scatter(
                y=self.system_metrics['resource_utilization']['network_sent'],
                name='Network Sent'
            ),
            row=2, col=2
        )
        fig.add_trace(
            go.Scatter(
                y=self.system_metrics['resource_utilization']['network_recv'],
                name='Network Received'
            ),
            row=2, col=2
        )
        
        fig.update_layout(height=600, showlegend=True)
        return fig

    def _create_performance_metrics_graph(self):
        """Create performance metrics graph."""
        fig = make_subplots(rows=2, cols=2, subplot_titles=('Response Time', 'Crawl Rate', 'Success Rate', 'Error Rate'))
        
        # Response Time
        fig.add_trace(
            go.Scatter(
                y=self.system_metrics['performance_metrics']['response_time'],
                name='Response Time'
            ),
            row=1, col=1
        )
        
        # Crawl Rate
        fig.add_trace(
            go.Scatter(
                y=self.system_metrics['performance_metrics']['crawl_rate'],
                name='Crawl Rate'
            ),
            row=1, col=2
        )
        
        # Success Rate
        fig.add_trace(
            go.Scatter(
                y=self.system_metrics['performance_metrics']['success_rate'],
                name='Success Rate'
            ),
            row=2, col=1
        )
        
        # Error Rate
        fig.add_trace(
            go.Scatter(
                y=self.system_metrics['performance_metrics']['error_rate'],
                name='Error Rate'
            ),
            row=2, col=2
        )
        
        fig.update_layout(height=600, showlegend=True)
        return fig

    def _create_crawler_status_table(self):
        """Create crawler status table."""
        table = html.Table([
            html.Thead(
                html.Tr([
                    html.Th('Crawler ID'),
                    html.Th('Status'),
                    html.Th('URLs Crawled'),
                    html.Th('Error Rate'),
                    html.Th('Memory Usage'),
                    html.Th('CPU Usage'),
                    html.Th('Active Tasks')
                ])
            ),
            html.Tbody([
                html.Tr([
                    html.Td(m.crawler_id),
                    html.Td(m.status),
                    html.Td(f"{m.urls_crawled:,}"),
                    html.Td(f"{m.error_rate:.1f}%"),
                    html.Td(f"{m.memory_usage:.1f}MB"),
                    html.Td(f"{m.cpu_usage:.1f}%"),
                    html.Td(f"{m.active_tasks}")
                ]) for m in self.crawler_metrics.values()
            ])
        ])
        return table

    def _create_error_distribution_graph(self):
        """Create error distribution graph."""
        error_types = self.system_metrics['error_types']
        fig = go.Figure(data=[
            go.Pie(
                labels=list(error_types.keys()),
                values=list(error_types.values()),
                hole=.3
            )
        ])
        fig.update_layout(title='Error Distribution')
        return fig

    def _create_error_history_graph(self):
        """Create error history graph."""
        fig = go.Figure()
        for error_type, rates in self.system_metrics['error_rates'].items():
            fig.add_trace(go.Scatter(
                y=rates,
                name=error_type,
                mode='lines'
            ))
        fig.update_layout(title='Error History', yaxis_title='Error Rate (%)')
        return fig

    def _create_domain_distribution_graph(self):
        """Create domain distribution graph."""
        domains = self.system_metrics['domain_distribution']
        fig = go.Figure(data=[
            go.Bar(
                x=list(domains.keys()),
                y=list(domains.values())
            )
        ])
        fig.update_layout(title='Domain Distribution', xaxis_title='Domain', yaxis_title='URLs Crawled')
        return fig

    def _create_crawl_history_graph(self):
        """Create crawl history graph."""
        history = self.system_metrics['crawl_history']
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=[h['timestamp'] for h in history],
            y=[h['urls_crawled'] for h in history],
            name='URLs Crawled'
        ))
        fig.add_trace(go.Scatter(
            x=[h['timestamp'] for h in history],
            y=[h['error_rate'] for h in history],
            name='Error Rate'
        ))
        fig.update_layout(title='Crawl History', xaxis_title='Time', yaxis_title='Count')
        return fig

    def _calculate_error_rate(self) -> float:
        """Calculate the current error rate."""
        total = self.system_metrics['total_urls_crawled']
        if total == 0:
            return 0.0
        return (self.system_metrics['total_urls_failed'] / total) * 100

    def _calculate_crawl_rate(self) -> float:
        """Calculate the current crawl rate (URLs per minute)."""
        history = self.system_metrics['crawl_history']
        if len(history) < 2:
            return 0.0
        
        # Calculate rate over the last 5 minutes
        recent_history = [h for h in history if h['timestamp'] > time.time() - 300]
        if not recent_history:
            return 0.0
            
        urls_crawled = recent_history[-1]['urls_crawled'] - recent_history[0]['urls_crawled']
        time_diff = recent_history[-1]['timestamp'] - recent_history[0]['timestamp']
        return (urls_crawled / time_diff) * 60 if time_diff > 0 else 0.0

    def update_metrics(self, message: Dict[str, Any]):
        """Update metrics based on a message from the crawler."""
        with self.metrics_lock:
            try:
                message_type = message.get('type')
                if message_type == 'heartbeat':
                    self._update_crawler_metrics(message)
                elif message_type == 'result':
                    self._update_crawl_metrics(message)
                elif message_type == 'error':
                    self._update_error_metrics(message)
                
                # Update system metrics
                self._update_system_metrics()
                
                # Update performance metrics
                self._update_performance_metrics()
                
                # Update resource utilization
                self._update_resource_utilization()
                
                # Update crawl history
                self._update_crawl_history()
                
            except Exception as e:
                logger.error(f"Error updating metrics: {e}")
                logger.error(traceback.format_exc())

    def _update_crawler_metrics(self, message: Dict[str, Any]):
        """Update metrics for a specific crawler."""
        crawler_id = message['crawler_id']
        if crawler_id not in self.crawler_metrics:
            self.crawler_metrics[crawler_id] = CrawlerMetrics(
                crawler_id=crawler_id,
                status='active',
                last_heartbeat=time.time(),
                urls_crawled=0,
                urls_failed=0,
                urls_timeout=0,
                avg_response_time=0.0,
                memory_usage=0.0,
                cpu_usage=0.0,
                active_tasks=0,
                error_rate=0.0,
                retry_rate=0.0,
                bytes_downloaded=0,
                requests_per_minute=0.0,
                success_rate=100.0,
                domain_distribution={},
                http_status_codes={},
                error_types={},
                performance_metrics={}
            )
        
        metrics = self.crawler_metrics[crawler_id]
        metrics.last_heartbeat = time.time()
        metrics.status = message.get('status', 'active')
        metrics.active_tasks = message.get('active_tasks', 0)
        metrics.memory_usage = message.get('memory_usage', 0.0)
        metrics.cpu_usage = message.get('cpu_usage', 0.0)

    def _update_crawl_metrics(self, message: Dict[str, Any]):
        """Update metrics for a crawl result."""
        crawler_id = message['crawler_id']
        if crawler_id in self.crawler_metrics:
            metrics = self.crawler_metrics[crawler_id]
            metrics.urls_crawled += 1
            metrics.bytes_downloaded += message.get('bytes_downloaded', 0)
            
            # Update domain distribution
            domain = message.get('domain', 'unknown')
            metrics.domain_distribution[domain] = metrics.domain_distribution.get(domain, 0) + 1
            
            # Update HTTP status codes
            status_code = message.get('status_code', 0)
            if status_code:
                metrics.http_status_codes[status_code] = metrics.http_status_codes.get(status_code, 0) + 1
            
            # Update performance metrics
            response_time = message.get('response_time', 0)
            if response_time:
                metrics.performance_metrics['response_time'] = response_time
            
            # Update system metrics
            self.system_metrics['total_urls_crawled'] += 1
            self.system_metrics['total_bytes_downloaded'] += message.get('bytes_downloaded', 0)
            self.system_metrics['domain_distribution'][domain] = self.system_metrics['domain_distribution'].get(domain, 0) + 1

    def _update_error_metrics(self, message: Dict[str, Any]):
        """Update metrics for an error."""
        crawler_id = message['crawler_id']
        if crawler_id in self.crawler_metrics:
            metrics = self.crawler_metrics[crawler_id]
            metrics.urls_failed += 1
            
            # Update error types
            error_type = message.get('error_type', 'unknown')
            metrics.error_types[error_type] = metrics.error_types.get(error_type, 0) + 1
            
            # Update system metrics
            self.system_metrics['total_urls_failed'] += 1
            self.system_metrics['error_types'][error_type] = self.system_metrics['error_types'].get(error_type, 0) + 1

    def _update_system_metrics(self):
        """Update system-wide metrics."""
        # Calculate system-wide error rate
        total_urls = self.system_metrics['total_urls_crawled']
        if total_urls > 0:
            self.system_metrics['error_rates']['overall'].append(
                (self.system_metrics['total_urls_failed'] / total_urls) * 100
            )
        
        # Keep only last 1000 error rates
        for error_type in self.system_metrics['error_rates']:
            self.system_metrics['error_rates'][error_type] = self.system_metrics['error_rates'][error_type][-1000:]

    def _update_performance_metrics(self):
        """Update performance metrics."""
        # Calculate average response time
        response_times = []
        for metrics in self.crawler_metrics.values():
            if 'response_time' in metrics.performance_metrics:
                response_times.append(metrics.performance_metrics['response_time'])
        
        if response_times:
            self.system_metrics['performance_metrics']['response_time'].append(np.mean(response_times))
        
        # Calculate crawl rate
        self.system_metrics['performance_metrics']['crawl_rate'].append(
            self._calculate_crawl_rate()
        )
        
        # Calculate success rate
        total_urls = self.system_metrics['total_urls_crawled']
        if total_urls > 0:
            success_rate = ((total_urls - self.system_metrics['total_urls_failed']) / total_urls) * 100
            self.system_metrics['performance_metrics']['success_rate'].append(success_rate)
        
        # Keep only last 1000 performance metrics
        for metric in self.system_metrics['performance_metrics']:
            self.system_metrics['performance_metrics'][metric] = self.system_metrics['performance_metrics'][metric][-1000:]

    def _update_resource_utilization(self):
        """Update resource utilization metrics."""
        # Get system-wide resource usage
        cpu_percent = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        net_io = psutil.net_io_counters()
        
        self.system_metrics['resource_utilization']['cpu'].append(cpu_percent)
        self.system_metrics['resource_utilization']['memory'].append(memory.percent)
        self.system_metrics['resource_utilization']['disk'].append(disk.percent)
        self.system_metrics['resource_utilization']['network_sent'].append(net_io.bytes_sent)
        self.system_metrics['resource_utilization']['network_recv'].append(net_io.bytes_recv)
        
        # Keep only last 1000 resource metrics
        for resource in self.system_metrics['resource_utilization']:
            self.system_metrics['resource_utilization'][resource] = self.system_metrics['resource_utilization'][resource][-1000:]

    def _update_crawl_history(self):
        """Update crawl history."""
        history_entry = {
            'timestamp': time.time(),
            'urls_crawled': self.system_metrics['total_urls_crawled'],
            'urls_failed': self.system_metrics['total_urls_failed'],
            'error_rate': self._calculate_error_rate(),
            'crawl_rate': self._calculate_crawl_rate(),
            'active_crawlers': sum(1 for m in self.crawler_metrics.values() 
                                 if time.time() - m.last_heartbeat < 60)
        }
        
        with self.history_lock:
            self.system_metrics['crawl_history'].append(history_entry)
            # Keep only last 1000 history entries
            self.system_metrics['crawl_history'] = self.system_metrics['crawl_history'][-1000:]

    def run(self, port: int = 8050):
        """Run the monitoring dashboard."""
        logger.info(f"Starting monitoring dashboard on port {port}")
        self.app.run_server(debug=True, port=port)

class ResourceMonitor:
    """Monitor system resources."""
    def __init__(self):
        self.cpu_threshold = 80  # 80% CPU usage
        self.memory_threshold = 80  # 80% memory usage
        self.disk_threshold = 80  # 80% disk usage
        self.network_threshold = 1000000  # 1MB/s

    def check_resources(self) -> Dict[str, Any]:
        """Check system resources and return metrics."""
        return {
            'cpu': psutil.cpu_percent(),
            'memory': psutil.virtual_memory().percent,
            'disk': psutil.disk_usage('/').percent,
            'network': psutil.net_io_counters()
        }

    def is_resource_usage_high(self) -> bool:
        """Check if any resource usage is above threshold."""
        metrics = self.check_resources()
        return (
            metrics['cpu'] > self.cpu_threshold or
            metrics['memory'] > self.memory_threshold or
            metrics['disk'] > self.disk_threshold
        )

class ErrorTracker:
    """Track and analyze errors."""
    def __init__(self):
        self.errors = defaultdict(list)
        self.error_patterns = defaultdict(int)
        self.error_threshold = 10  # Number of similar errors before alerting

    def track_error(self, error_type: str, error_message: str):
        """Track an error occurrence."""
        timestamp = time.time()
        self.errors[error_type].append({
            'timestamp': timestamp,
            'message': error_message
        })
        
        # Update error patterns
        self.error_patterns[error_type] += 1
        
        # Check if we should alert
        if self.error_patterns[error_type] >= self.error_threshold:
            logger.warning(f"High frequency of {error_type} errors detected")

    def get_error_stats(self) -> Dict[str, Any]:
        """Get error statistics."""
        return {
            'error_counts': dict(self.error_patterns),
            'error_timeline': {
                error_type: [e['timestamp'] for e in errors]
                for error_type, errors in self.errors.items()
            }
        }

if __name__ == "__main__":
    monitoring = MonitoringSystem()
    monitoring.run() 