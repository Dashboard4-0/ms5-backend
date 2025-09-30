# MS5.0 Floor Dashboard - Phase 10.2 Monitoring and Alerting
# Application Metrics Collection and Business Metrics Implementation

import time
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass, asdict
from enum import Enum
import json

# Prometheus metrics (if available)
try:
    from prometheus_client import Counter, Histogram, Gauge, Info, CollectorRegistry, generate_latest
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    # Create dummy classes for when Prometheus is not available
    class Counter:
        def __init__(self, *args, **kwargs): pass
        def inc(self, *args, **kwargs): pass
        def labels(self, *args, **kwargs): return self
    
    class Histogram:
        def __init__(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def time(self): return self
        def __enter__(self): return self
        def __exit__(self, *args): pass
    
    class Gauge:
        def __init__(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass
        def inc(self, *args, **kwargs): pass
        def dec(self, *args, **kwargs): pass
        def labels(self, *args, **kwargs): return self
    
    class Info:
        def __init__(self, *args, **kwargs): pass
        def info(self, *args, **kwargs): pass

logger = logging.getLogger(__name__)

class MetricType(Enum):
    """Metric type enumeration."""
    COUNTER = "counter"
    HISTOGRAM = "histogram"
    GAUGE = "gauge"
    INFO = "info"

@dataclass
class MetricData:
    """Data class for metric information."""
    name: str
    type: MetricType
    value: float
    labels: Dict[str, str]
    timestamp: datetime
    description: str = ""

class ApplicationMetrics:
    """
    Comprehensive application metrics collection for MS5.0 Floor Dashboard.
    Collects both technical and business metrics for monitoring and alerting.
    """
    
    def __init__(self):
        """Initialize application metrics collector."""
        self.registry = CollectorRegistry() if PROMETHEUS_AVAILABLE else None
        self.metrics_data = {}
        self.custom_metrics = {}
        
        # Initialize Prometheus metrics
        if PROMETHEUS_AVAILABLE:
            self._initialize_prometheus_metrics()
        
        # Initialize custom metrics storage
        self._initialize_custom_metrics()
        
        logger.info("Application metrics initialized")
    
    def _initialize_prometheus_metrics(self):
        """Initialize Prometheus metrics."""
        # API Metrics
        self.api_requests_total = Counter(
            'ms5_api_requests_total',
            'Total number of API requests',
            ['method', 'endpoint', 'status_code'],
            registry=self.registry
        )
        
        self.api_request_duration = Histogram(
            'ms5_api_request_duration_seconds',
            'API request duration in seconds',
            ['method', 'endpoint'],
            buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
            registry=self.registry
        )
        
        # Database Metrics
        self.db_connections_active = Gauge(
            'ms5_db_connections_active',
            'Number of active database connections',
            registry=self.registry
        )
        
        self.db_query_duration = Histogram(
            'ms5_db_query_duration_seconds',
            'Database query duration in seconds',
            ['query_type', 'table'],
            buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
            registry=self.registry
        )
        
        # WebSocket Metrics
        self.websocket_connections_active = Gauge(
            'ms5_websocket_connections_active',
            'Number of active WebSocket connections',
            registry=self.registry
        )
        
        self.websocket_messages_total = Counter(
            'ms5_websocket_messages_total',
            'Total number of WebSocket messages',
            ['message_type'],
            registry=self.registry
        )
        
        # Production Metrics
        self.production_lines_active = Gauge(
            'ms5_production_lines_active',
            'Number of active production lines',
            registry=self.registry
        )
        
        self.production_oee = Gauge(
            'ms5_production_oee',
            'Overall Equipment Effectiveness',
            ['line_id', 'equipment_code'],
            registry=self.registry
        )
        
        self.production_throughput = Gauge(
            'ms5_production_throughput',
            'Production throughput (units per hour)',
            ['line_id', 'product_type'],
            registry=self.registry
        )
        
        # Andon Metrics
        self.andon_events_active = Gauge(
            'ms5_andon_events_active',
            'Number of active Andon events',
            ['priority', 'status'],
            registry=self.registry
        )
        
        self.andon_response_time = Histogram(
            'ms5_andon_response_time_seconds',
            'Andon event response time in seconds',
            ['priority'],
            buckets=[30, 60, 120, 300, 600, 1800],
            registry=self.registry
        )
        
        # Quality Metrics
        self.quality_checks_total = Counter(
            'ms5_quality_checks_total',
            'Total number of quality checks',
            ['result', 'check_type'],
            registry=self.registry
        )
        
        self.quality_defect_rate = Gauge(
            'ms5_quality_defect_rate',
            'Quality defect rate (defects per 1000 units)',
            ['line_id', 'product_type'],
            registry=self.registry
        )
        
        # Maintenance Metrics
        self.maintenance_work_orders_active = Gauge(
            'ms5_maintenance_work_orders_active',
            'Number of active maintenance work orders',
            ['priority', 'work_type'],
            registry=self.registry
        )
        
        self.maintenance_downtime_minutes = Gauge(
            'ms5_maintenance_downtime_minutes',
            'Maintenance downtime in minutes',
            ['equipment_code', 'maintenance_type'],
            registry=self.registry
        )
        
        # System Metrics
        self.system_memory_usage = Gauge(
            'ms5_system_memory_usage_bytes',
            'System memory usage in bytes',
            ['component'],
            registry=self.registry
        )
        
        self.system_cpu_usage = Gauge(
            'ms5_system_cpu_usage_percent',
            'System CPU usage percentage',
            ['component'],
            registry=self.registry
        )
        
        # Cache Metrics
        self.cache_hits_total = Counter(
            'ms5_cache_hits_total',
            'Total number of cache hits',
            ['cache_type'],
            registry=self.registry
        )
        
        self.cache_misses_total = Counter(
            'ms5_cache_misses_total',
            'Total number of cache misses',
            ['cache_type'],
            registry=self.registry
        )
        
        # Business Metrics
        self.production_efficiency = Gauge(
            'ms5_production_efficiency_percent',
            'Production efficiency percentage',
            ['line_id'],
            registry=self.registry
        )
        
        self.downtime_percentage = Gauge(
            'ms5_downtime_percentage',
            'Downtime percentage',
            ['line_id', 'category'],
            registry=self.registry
        )
        
        self.energy_consumption_kwh = Gauge(
            'ms5_energy_consumption_kwh',
            'Energy consumption in kWh',
            ['equipment_code'],
            registry=self.registry
        )
    
    def _initialize_custom_metrics(self):
        """Initialize custom metrics storage."""
        self.custom_metrics = {
            'business_kpis': {},
            'production_trends': {},
            'quality_trends': {},
            'maintenance_trends': {},
            'system_health': {}
        }
    
    # API Metrics Methods
    def record_api_request(self, method: str, endpoint: str, status_code: int, duration: float):
        """Record API request metrics."""
        if PROMETHEUS_AVAILABLE:
            self.api_requests_total.labels(
                method=method,
                endpoint=endpoint,
                status_code=str(status_code)
            ).inc()
            
            self.api_request_duration.labels(
                method=method,
                endpoint=endpoint
            ).observe(duration)
        
        # Store in custom metrics
        metric_key = f"api_requests_{method}_{endpoint}"
        if metric_key not in self.custom_metrics['system_health']:
            self.custom_metrics['system_health'][metric_key] = {
                'total_requests': 0,
                'total_duration': 0.0,
                'avg_duration': 0.0,
                'error_count': 0,
                'last_updated': datetime.now()
            }
        
        metric = self.custom_metrics['system_health'][metric_key]
        metric['total_requests'] += 1
        metric['total_duration'] += duration
        metric['avg_duration'] = metric['total_duration'] / metric['total_requests']
        metric['last_updated'] = datetime.now()
        
        if status_code >= 400:
            metric['error_count'] += 1
    
    # Database Metrics Methods
    def record_db_connection(self, active_connections: int):
        """Record database connection metrics."""
        if PROMETHEUS_AVAILABLE:
            self.db_connections_active.set(active_connections)
    
    def record_db_query(self, query_type: str, table: str, duration: float):
        """Record database query metrics."""
        if PROMETHEUS_AVAILABLE:
            self.db_query_duration.labels(
                query_type=query_type,
                table=table
            ).observe(duration)
    
    # WebSocket Metrics Methods
    def record_websocket_connection(self, active_connections: int):
        """Record WebSocket connection metrics."""
        if PROMETHEUS_AVAILABLE:
            self.websocket_connections_active.set(active_connections)
    
    def record_websocket_message(self, message_type: str):
        """Record WebSocket message metrics."""
        if PROMETHEUS_AVAILABLE:
            self.websocket_messages_total.labels(message_type=message_type).inc()
    
    # Production Metrics Methods
    def record_production_line_status(self, line_id: str, is_active: bool):
        """Record production line status."""
        # This would be called periodically to update active line count
        pass
    
    def record_oee(self, line_id: str, equipment_code: str, oee: float):
        """Record OEE metrics."""
        if PROMETHEUS_AVAILABLE:
            self.production_oee.labels(
                line_id=line_id,
                equipment_code=equipment_code
            ).set(oee)
        
        # Store in custom metrics
        if 'oee_data' not in self.custom_metrics['business_kpis']:
            self.custom_metrics['business_kpis']['oee_data'] = {}
        
        self.custom_metrics['business_kpis']['oee_data'][f"{line_id}_{equipment_code}"] = {
            'oee': oee,
            'timestamp': datetime.now(),
            'line_id': line_id,
            'equipment_code': equipment_code
        }
    
    def record_production_throughput(self, line_id: str, product_type: str, throughput: float):
        """Record production throughput metrics."""
        if PROMETHEUS_AVAILABLE:
            self.production_throughput.labels(
                line_id=line_id,
                product_type=product_type
            ).set(throughput)
    
    # Andon Metrics Methods
    def record_andon_event(self, priority: str, status: str, response_time: float = None):
        """Record Andon event metrics."""
        if PROMETHEUS_AVAILABLE:
            self.andon_events_active.labels(
                priority=priority,
                status=status
            ).inc()
            
            if response_time is not None:
                self.andon_response_time.labels(priority=priority).observe(response_time)
    
    # Quality Metrics Methods
    def record_quality_check(self, result: str, check_type: str):
        """Record quality check metrics."""
        if PROMETHEUS_AVAILABLE:
            self.quality_checks_total.labels(
                result=result,
                check_type=check_type
            ).inc()
    
    def record_defect_rate(self, line_id: str, product_type: str, defect_rate: float):
        """Record quality defect rate."""
        if PROMETHEUS_AVAILABLE:
            self.quality_defect_rate.labels(
                line_id=line_id,
                product_type=product_type
            ).set(defect_rate)
    
    # Maintenance Metrics Methods
    def record_maintenance_work_order(self, priority: str, work_type: str, is_active: bool):
        """Record maintenance work order metrics."""
        if PROMETHEUS_AVAILABLE:
            if is_active:
                self.maintenance_work_orders_active.labels(
                    priority=priority,
                    work_type=work_type
                ).inc()
            else:
                self.maintenance_work_orders_active.labels(
                    priority=priority,
                    work_type=work_type
                ).dec()
    
    def record_maintenance_downtime(self, equipment_code: str, maintenance_type: str, downtime_minutes: float):
        """Record maintenance downtime."""
        if PROMETHEUS_AVAILABLE:
            self.maintenance_downtime_minutes.labels(
                equipment_code=equipment_code,
                maintenance_type=maintenance_type
            ).set(downtime_minutes)
    
    # System Metrics Methods
    def record_memory_usage(self, component: str, memory_bytes: int):
        """Record memory usage metrics."""
        if PROMETHEUS_AVAILABLE:
            self.system_memory_usage.labels(component=component).set(memory_bytes)
    
    def record_cpu_usage(self, component: str, cpu_percent: float):
        """Record CPU usage metrics."""
        if PROMETHEUS_AVAILABLE:
            self.system_cpu_usage.labels(component=component).set(cpu_percent)
    
    # Cache Metrics Methods
    def record_cache_hit(self, cache_type: str):
        """Record cache hit."""
        if PROMETHEUS_AVAILABLE:
            self.cache_hits_total.labels(cache_type=cache_type).inc()
    
    def record_cache_miss(self, cache_type: str):
        """Record cache miss."""
        if PROMETHEUS_AVAILABLE:
            self.cache_misses_total.labels(cache_type=cache_type).inc()
    
    # Business Metrics Methods
    def record_production_efficiency(self, line_id: str, efficiency_percent: float):
        """Record production efficiency metrics."""
        if PROMETHEUS_AVAILABLE:
            self.production_efficiency.labels(line_id=line_id).set(efficiency_percent)
        
        # Store in custom metrics
        if 'efficiency_data' not in self.custom_metrics['business_kpis']:
            self.custom_metrics['business_kpis']['efficiency_data'] = {}
        
        self.custom_metrics['business_kpis']['efficiency_data'][line_id] = {
            'efficiency': efficiency_percent,
            'timestamp': datetime.now(),
            'line_id': line_id
        }
    
    def record_downtime_percentage(self, line_id: str, category: str, downtime_percent: float):
        """Record downtime percentage."""
        if PROMETHEUS_AVAILABLE:
            self.downtime_percentage.labels(
                line_id=line_id,
                category=category
            ).set(downtime_percent)
    
    def record_energy_consumption(self, equipment_code: str, consumption_kwh: float):
        """Record energy consumption."""
        if PROMETHEUS_AVAILABLE:
            self.energy_consumption_kwh.labels(equipment_code=equipment_code).set(consumption_kwh)
    
    # Business KPI Calculations
    async def calculate_production_kpis(self, line_id: str, time_period_hours: int = 24) -> Dict[str, Any]:
        """Calculate production KPIs for a line."""
        # This would integrate with the database to calculate actual KPIs
        # For now, return sample data structure
        
        kpis = {
            'line_id': line_id,
            'time_period_hours': time_period_hours,
            'oee': 0.85,
            'availability': 0.92,
            'performance': 0.88,
            'quality': 0.96,
            'throughput': 150.5,
            'downtime_percentage': 8.5,
            'efficiency': 87.3,
            'energy_consumption': 1250.8,
            'timestamp': datetime.now()
        }
        
        # Store in custom metrics
        if 'production_kpis' not in self.custom_metrics['business_kpis']:
            self.custom_metrics['business_kpis']['production_kpis'] = {}
        
        self.custom_metrics['business_kpis']['production_kpis'][line_id] = kpis
        
        return kpis
    
    async def calculate_quality_metrics(self, line_id: str, time_period_hours: int = 24) -> Dict[str, Any]:
        """Calculate quality metrics for a line."""
        quality_metrics = {
            'line_id': line_id,
            'time_period_hours': time_period_hours,
            'first_pass_yield': 0.94,
            'defect_rate': 2.3,
            'rework_rate': 1.8,
            'scrap_rate': 0.5,
            'total_checks': 1250,
            'passed_checks': 1180,
            'failed_checks': 70,
            'timestamp': datetime.now()
        }
        
        # Store in custom metrics
        if 'quality_metrics' not in self.custom_metrics['quality_trends']:
            self.custom_metrics['quality_trends']['quality_metrics'] = {}
        
        self.custom_metrics['quality_trends']['quality_metrics'][line_id] = quality_metrics
        
        return quality_metrics
    
    async def calculate_maintenance_metrics(self, equipment_code: str, time_period_days: int = 7) -> Dict[str, Any]:
        """Calculate maintenance metrics for equipment."""
        maintenance_metrics = {
            'equipment_code': equipment_code,
            'time_period_days': time_period_days,
            'total_downtime_hours': 12.5,
            'planned_downtime_hours': 8.0,
            'unplanned_downtime_hours': 4.5,
            'maintenance_events': 3,
            'mean_time_to_repair': 2.1,
            'mean_time_between_failures': 168.5,
            'preventive_maintenance_ratio': 0.75,
            'timestamp': datetime.now()
        }
        
        # Store in custom metrics
        if 'maintenance_metrics' not in self.custom_metrics['maintenance_trends']:
            self.custom_metrics['maintenance_trends']['maintenance_metrics'] = {}
        
        self.custom_metrics['maintenance_trends']['maintenance_metrics'][equipment_code] = maintenance_metrics
        
        return maintenance_metrics
    
    # Metrics Export Methods
    def get_prometheus_metrics(self) -> str:
        """Get Prometheus-formatted metrics."""
        if PROMETHEUS_AVAILABLE and self.registry:
            return generate_latest(self.registry).decode('utf-8')
        return ""
    
    def get_custom_metrics(self) -> Dict[str, Any]:
        """Get custom metrics data."""
        return self.custom_metrics
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get metrics summary for monitoring dashboard."""
        summary = {
            'timestamp': datetime.now(),
            'prometheus_available': PROMETHEUS_AVAILABLE,
            'custom_metrics_count': sum(len(category) for category in self.custom_metrics.values()),
            'business_kpis': {
                'production_kpis_count': len(self.custom_metrics['business_kpis'].get('production_kpis', {})),
                'efficiency_data_count': len(self.custom_metrics['business_kpis'].get('efficiency_data', {})),
                'oee_data_count': len(self.custom_metrics['business_kpis'].get('oee_data', {}))
            },
            'quality_trends_count': len(self.custom_metrics['quality_trends'].get('quality_metrics', {})),
            'maintenance_trends_count': len(self.custom_metrics['maintenance_trends'].get('maintenance_metrics', {})),
            'system_health_count': len(self.custom_metrics['system_health'])
        }
        
        return summary
    
    # Alerting Thresholds
    def check_alerting_thresholds(self) -> List[Dict[str, Any]]:
        """Check metrics against alerting thresholds and return alerts."""
        alerts = []
        
        # Check OEE thresholds
        oee_data = self.custom_metrics['business_kpis'].get('oee_data', {})
        for key, data in oee_data.items():
            if data['oee'] < 0.75:  # OEE below 75%
                alerts.append({
                    'type': 'warning',
                    'metric': 'oee',
                    'line_id': data['line_id'],
                    'equipment_code': data['equipment_code'],
                    'value': data['oee'],
                    'threshold': 0.75,
                    'message': f"OEE below threshold: {data['oee']:.2%} < 75%",
                    'timestamp': data['timestamp']
                })
            elif data['oee'] < 0.60:  # OEE below 60%
                alerts.append({
                    'type': 'critical',
                    'metric': 'oee',
                    'line_id': data['line_id'],
                    'equipment_code': data['equipment_code'],
                    'value': data['oee'],
                    'threshold': 0.60,
                    'message': f"OEE critically low: {data['oee']:.2%} < 60%",
                    'timestamp': data['timestamp']
                })
        
        # Check efficiency thresholds
        efficiency_data = self.custom_metrics['business_kpis'].get('efficiency_data', {})
        for line_id, data in efficiency_data.items():
            if data['efficiency'] < 80:  # Efficiency below 80%
                alerts.append({
                    'type': 'warning',
                    'metric': 'efficiency',
                    'line_id': line_id,
                    'value': data['efficiency'],
                    'threshold': 80,
                    'message': f"Production efficiency below threshold: {data['efficiency']:.1f}% < 80%",
                    'timestamp': data['timestamp']
                })
        
        # Check API performance thresholds
        system_health = self.custom_metrics['system_health']
        for endpoint, data in system_health.items():
            if data['avg_duration'] > 2.0:  # API response time > 2 seconds
                alerts.append({
                    'type': 'warning',
                    'metric': 'api_performance',
                    'endpoint': endpoint,
                    'value': data['avg_duration'],
                    'threshold': 2.0,
                    'message': f"API response time slow: {data['avg_duration']:.2f}s > 2.0s",
                    'timestamp': data['last_updated']
                })
            
            error_rate = (data['error_count'] / data['total_requests']) * 100 if data['total_requests'] > 0 else 0
            if error_rate > 5:  # Error rate > 5%
                alerts.append({
                    'type': 'critical',
                    'metric': 'api_error_rate',
                    'endpoint': endpoint,
                    'value': error_rate,
                    'threshold': 5.0,
                    'message': f"API error rate high: {error_rate:.1f}% > 5%",
                    'timestamp': data['last_updated']
                })
        
        return alerts


# Global metrics instance
_application_metrics = None

def get_application_metrics() -> ApplicationMetrics:
    """Get global application metrics instance."""
    global _application_metrics
    if _application_metrics is None:
        _application_metrics = ApplicationMetrics()
    return _application_metrics


# Decorator for automatic metrics collection
def collect_metrics(metric_type: str, **metric_labels):
    """
    Decorator for automatic metrics collection.
    
    Args:
        metric_type: Type of metric (api_request, db_query, etc.)
        **metric_labels: Labels for the metric
        
    Returns:
        Decorated function
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            
            try:
                result = await func(*args, **kwargs)
                
                # Record successful execution
                metrics = get_application_metrics()
                if metric_type == 'api_request':
                    method = metric_labels.get('method', 'UNKNOWN')
                    endpoint = metric_labels.get('endpoint', func.__name__)
                    metrics.record_api_request(method, endpoint, 200, time.time() - start_time)
                elif metric_type == 'db_query':
                    query_type = metric_labels.get('query_type', 'SELECT')
                    table = metric_labels.get('table', 'unknown')
                    metrics.record_db_query(query_type, table, time.time() - start_time)
                
                return result
                
            except Exception as e:
                # Record failed execution
                metrics = get_application_metrics()
                if metric_type == 'api_request':
                    method = metric_labels.get('method', 'UNKNOWN')
                    endpoint = metric_labels.get('endpoint', func.__name__)
                    metrics.record_api_request(method, endpoint, 500, time.time() - start_time)
                
                raise
        
        return wrapper
    return decorator
