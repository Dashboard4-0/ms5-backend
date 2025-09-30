# MS5.0 Floor Dashboard - Phase 6B: Enhanced Application Metrics for AKS
# Advanced monitoring with Azure Monitor integration and distributed tracing

import time
import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
from contextlib import asynccontextmanager
import os

# Azure Monitor integration
try:
    from opencensus.ext.azure import metrics_exporter
    from opencensus.stats import measure, view, stats
    from opencensus.tags import tag_map, tag_key, tag_value
    AZURE_MONITOR_AVAILABLE = True
except ImportError:
    AZURE_MONITOR_AVAILABLE = False

# Prometheus metrics
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
    """Metric type enumeration for comprehensive monitoring."""
    COUNTER = "counter"
    HISTOGRAM = "histogram"
    GAUGE = "gauge"
    INFO = "info"
    SUMMARY = "summary"

@dataclass
class MetricData:
    """Enhanced data class for metric information with Azure Monitor compatibility."""
    name: str
    type: MetricType
    value: Union[float, int, str]
    labels: Dict[str, str]
    timestamp: datetime
    description: str = ""
    azure_dimensions: Optional[Dict[str, str]] = None

@dataclass
class SLIDefinition:
    """Service Level Indicator definition for SLO monitoring."""
    name: str
    metric_name: str
    target_value: float
    comparison_operator: str  # '>', '<', '>=', '<=', '=='
    window_duration: str  # e.g., '5m', '1h', '24h'
    description: str = ""

@dataclass
class SLODefinition:
    """Service Level Objective definition with error budget tracking."""
    name: str
    sli_name: str
    target_percentage: float
    error_budget_percentage: float
    window_duration: str
    description: str = ""

class AKSApplicationMetrics:
    """
    Enhanced application metrics collection for MS5.0 Floor Dashboard AKS deployment.
    
    This class provides comprehensive monitoring capabilities including:
    - Azure Monitor integration for cloud-native metrics
    - Enhanced Prometheus metrics with Kubernetes-specific dimensions
    - Business KPI tracking with real-time calculations
    - SLI/SLO monitoring with error budget tracking
    - Distributed tracing correlation
    """
    
    def __init__(self, azure_connection_string: Optional[str] = None):
        """
        Initialize enhanced application metrics collector.
        
        Args:
            azure_connection_string: Azure Monitor connection string for telemetry
        """
        self.registry = CollectorRegistry() if PROMETHEUS_AVAILABLE else None
        self.metrics_data = {}
        self.custom_metrics = {}
        self.sli_definitions = {}
        self.slo_definitions = {}
        self.error_budgets = {}
        
        # Azure Monitor configuration
        self.azure_connection_string = azure_connection_string or os.getenv('APPLICATIONINSIGHTS_CONNECTION_STRING')
        self.azure_exporter = None
        
        # Initialize metrics systems
        if PROMETHEUS_AVAILABLE:
            self._initialize_prometheus_metrics()
        
        if AZURE_MONITOR_AVAILABLE and self.azure_connection_string:
            self._initialize_azure_monitor()
        
        self._initialize_custom_metrics()
        self._initialize_sli_slo_definitions()
        
        logger.info("Enhanced AKS application metrics initialized")
    
    def _initialize_prometheus_metrics(self):
        """Initialize enhanced Prometheus metrics with Kubernetes-specific dimensions."""
        # Kubernetes-specific metrics
        self.pod_restarts = Counter(
            'ms5_pod_restarts_total',
            'Total number of pod restarts',
            ['namespace', 'pod_name', 'container_name', 'node_name'],
            registry=self.registry
        )
        
        self.node_utilization = Gauge(
            'ms5_node_utilization_percent',
            'Node resource utilization percentage',
            ['node_name', 'resource_type', 'zone'],
            registry=self.registry
        )
        
        self.cluster_health = Gauge(
            'ms5_cluster_health_score',
            'Overall cluster health score',
            ['cluster_name', 'environment'],
            registry=self.registry
        )
        
        # Enhanced API metrics with Kubernetes dimensions
        self.api_requests_total = Counter(
            'ms5_api_requests_total',
            'Total number of API requests',
            ['method', 'endpoint', 'status_code', 'namespace', 'pod_name'],
            registry=self.registry
        )
        
        self.api_request_duration = Histogram(
            'ms5_api_request_duration_seconds',
            'API request duration in seconds',
            ['method', 'endpoint', 'namespace'],
            buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
            registry=self.registry
        )
        
        # Enhanced database metrics
        self.db_connections_active = Gauge(
            'ms5_db_connections_active',
            'Number of active database connections',
            ['database', 'namespace', 'pod_name'],
            registry=self.registry
        )
        
        self.db_query_duration = Histogram(
            'ms5_db_query_duration_seconds',
            'Database query duration in seconds',
            ['query_type', 'table', 'namespace'],
            buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
            registry=self.registry
        )
        
        # Enhanced WebSocket metrics
        self.websocket_connections_active = Gauge(
            'ms5_websocket_connections_active',
            'Number of active WebSocket connections',
            ['namespace', 'pod_name'],
            registry=self.registry
        )
        
        self.websocket_messages_total = Counter(
            'ms5_websocket_messages_total',
            'Total number of WebSocket messages',
            ['message_type', 'namespace', 'pod_name'],
            registry=self.registry
        )
        
        # Enhanced production metrics with real-time calculations
        self.production_oee = Gauge(
            'ms5_production_oee',
            'Overall Equipment Effectiveness',
            ['line_id', 'equipment_code', 'shift', 'namespace'],
            registry=self.registry
        )
        
        self.production_throughput = Gauge(
            'ms5_production_throughput',
            'Production throughput (units per hour)',
            ['line_id', 'product_type', 'shift', 'namespace'],
            registry=self.registry
        )
        
        # Enhanced Andon metrics
        self.andon_events_active = Gauge(
            'ms5_andon_events_active',
            'Number of active Andon events',
            ['priority', 'status', 'line_id', 'namespace'],
            registry=self.registry
        )
        
        self.andon_response_time = Histogram(
            'ms5_andon_response_time_seconds',
            'Andon event response time in seconds',
            ['priority', 'line_id'],
            buckets=[30, 60, 120, 300, 600, 1800, 3600],
            registry=self.registry
        )
        
        # Enhanced quality metrics
        self.quality_defect_rate = Gauge(
            'ms5_quality_defect_rate',
            'Quality defect rate (defects per 1000 units)',
            ['line_id', 'product_type', 'shift', 'namespace'],
            registry=self.registry
        )
        
        # Enhanced maintenance metrics
        self.maintenance_downtime_minutes = Gauge(
            'ms5_maintenance_downtime_minutes',
            'Maintenance downtime in minutes',
            ['equipment_code', 'maintenance_type', 'namespace'],
            registry=self.registry
        )
        
        # System metrics with Kubernetes dimensions
        self.system_memory_usage = Gauge(
            'ms5_system_memory_usage_bytes',
            'System memory usage in bytes',
            ['component', 'namespace', 'pod_name'],
            registry=self.registry
        )
        
        self.system_cpu_usage = Gauge(
            'ms5_system_cpu_usage_percent',
            'System CPU usage percentage',
            ['component', 'namespace', 'pod_name'],
            registry=self.registry
        )
        
        # Business metrics with enhanced dimensions
        self.production_efficiency = Gauge(
            'ms5_production_efficiency_percent',
            'Production efficiency percentage',
            ['line_id', 'shift', 'namespace'],
            registry=self.registry
        )
        
        self.energy_consumption_kwh = Gauge(
            'ms5_energy_consumption_kwh',
            'Energy consumption in kWh',
            ['equipment_code', 'namespace'],
            registry=self.registry
        )
        
        # Cost monitoring metrics
        self.resource_cost_usd = Gauge(
            'ms5_resource_cost_usd',
            'Resource cost in USD',
            ['resource_type', 'namespace', 'node_name'],
            registry=self.registry
        )
        
        # SLI/SLO metrics
        self.sli_value = Gauge(
            'ms5_sli_value',
            'Service Level Indicator value',
            ['sli_name', 'service', 'namespace'],
            registry=self.registry
        )
        
        self.slo_error_budget = Gauge(
            'ms5_slo_error_budget_percent',
            'Service Level Objective error budget percentage',
            ['slo_name', 'service', 'namespace'],
            registry=self.registry
        )
    
    def _initialize_azure_monitor(self):
        """Initialize Azure Monitor integration."""
        try:
            # Configure Azure Monitor metrics exporter
            self.azure_exporter = metrics_exporter.new_metrics_exporter(
                connection_string=self.azure_connection_string
            )
            
            # Register Azure Monitor exporter
            stats.register_view(self.azure_exporter)
            
            logger.info("Azure Monitor integration initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Azure Monitor: {e}")
            self.azure_exporter = None
    
    def _initialize_custom_metrics(self):
        """Initialize enhanced custom metrics storage."""
        self.custom_metrics = {
            'business_kpis': {},
            'production_trends': {},
            'quality_trends': {},
            'maintenance_trends': {},
            'system_health': {},
            'cost_metrics': {},
            'sli_slo_data': {}
        }
    
    def _initialize_sli_slo_definitions(self):
        """Initialize SLI/SLO definitions for comprehensive monitoring."""
        # API Service SLIs
        self.sli_definitions['api_availability'] = SLIDefinition(
            name='api_availability',
            metric_name='ms5_api_requests_total',
            target_value=99.9,
            comparison_operator='>=',
            window_duration='5m',
            description='API availability percentage'
        )
        
        self.sli_definitions['api_latency'] = SLIDefinition(
            name='api_latency',
            metric_name='ms5_api_request_duration_seconds',
            target_value=0.2,  # 200ms
            comparison_operator='<=',
            window_duration='5m',
            description='API response time p95'
        )
        
        self.sli_definitions['api_error_rate'] = SLIDefinition(
            name='api_error_rate',
            metric_name='ms5_api_requests_total',
            target_value=0.1,  # 0.1%
            comparison_operator='<=',
            window_duration='5m',
            description='API error rate percentage'
        )
        
        # Database Service SLIs
        self.sli_definitions['db_availability'] = SLIDefinition(
            name='db_availability',
            metric_name='ms5_db_connections_active',
            target_value=99.95,
            comparison_operator='>=',
            window_duration='5m',
            description='Database availability percentage'
        )
        
        self.sli_definitions['db_query_latency'] = SLIDefinition(
            name='db_query_latency',
            metric_name='ms5_db_query_duration_seconds',
            target_value=0.1,  # 100ms
            comparison_operator='<=',
            window_duration='5m',
            description='Database query latency p95'
        )
        
        # Production SLIs
        self.sli_definitions['production_oee'] = SLIDefinition(
            name='production_oee',
            metric_name='ms5_production_oee',
            target_value=85.0,  # 85%
            comparison_operator='>=',
            window_duration='1h',
            description='Production OEE percentage'
        )
        
        self.sli_definitions['andon_response_time'] = SLIDefinition(
            name='andon_response_time',
            metric_name='ms5_andon_response_time_seconds',
            target_value=300.0,  # 5 minutes
            comparison_operator='<=',
            window_duration='1h',
            description='Andon response time p95'
        )
        
        # Initialize SLOs
        self.slo_definitions['api_availability_slo'] = SLODefinition(
            name='api_availability_slo',
            sli_name='api_availability',
            target_percentage=99.9,
            error_budget_percentage=0.1,
            window_duration='30d',
            description='API availability SLO'
        )
        
        self.slo_definitions['api_latency_slo'] = SLODefinition(
            name='api_latency_slo',
            sli_name='api_latency',
            target_percentage=95.0,
            error_budget_percentage=5.0,
            window_duration='7d',
            description='API latency SLO'
        )
        
        self.slo_definitions['production_oee_slo'] = SLODefinition(
            name='production_oee_slo',
            sli_name='production_oee',
            target_percentage=85.0,
            error_budget_percentage=15.0,
            window_duration='30d',
            description='Production OEE SLO'
        )
    
    # Enhanced API Metrics Methods
    def record_api_request(self, method: str, endpoint: str, status_code: int, duration: float, 
                          namespace: str = "ms5-production", pod_name: str = "unknown"):
        """Record enhanced API request metrics with Kubernetes dimensions."""
        if PROMETHEUS_AVAILABLE:
            self.api_requests_total.labels(
                method=method,
                endpoint=endpoint,
                status_code=str(status_code),
                namespace=namespace,
                pod_name=pod_name
            ).inc()
            
            self.api_request_duration.labels(
                method=method,
                endpoint=endpoint,
                namespace=namespace
            ).observe(duration)
        
        # Azure Monitor integration
        if self.azure_exporter:
            self._send_azure_metric('api_request', {
                'method': method,
                'endpoint': endpoint,
                'status_code': str(status_code),
                'namespace': namespace,
                'pod_name': pod_name
            }, duration)
        
        # Store in custom metrics
        metric_key = f"api_requests_{method}_{endpoint}"
        if metric_key not in self.custom_metrics['system_health']:
            self.custom_metrics['system_health'][metric_key] = {
                'total_requests': 0,
                'total_duration': 0.0,
                'avg_duration': 0.0,
                'error_count': 0,
                'last_updated': datetime.now(),
                'namespace': namespace,
                'pod_name': pod_name
            }
        
        metric = self.custom_metrics['system_health'][metric_key]
        metric['total_requests'] += 1
        metric['total_duration'] += duration
        metric['avg_duration'] = metric['total_duration'] / metric['total_requests']
        metric['last_updated'] = datetime.now()
        
        if status_code >= 400:
            metric['error_count'] += 1
    
    # Enhanced Database Metrics Methods
    def record_db_connection(self, active_connections: int, database: str = "postgresql", 
                           namespace: str = "ms5-production", pod_name: str = "unknown"):
        """Record enhanced database connection metrics."""
        if PROMETHEUS_AVAILABLE:
            self.db_connections_active.labels(
                database=database,
                namespace=namespace,
                pod_name=pod_name
            ).set(active_connections)
        
        # Azure Monitor integration
        if self.azure_exporter:
            self._send_azure_metric('db_connections', {
                'database': database,
                'namespace': namespace,
                'pod_name': pod_name
            }, active_connections)
    
    def record_db_query(self, query_type: str, table: str, duration: float, 
                       namespace: str = "ms5-production"):
        """Record enhanced database query metrics."""
        if PROMETHEUS_AVAILABLE:
            self.db_query_duration.labels(
                query_type=query_type,
                table=table,
                namespace=namespace
            ).observe(duration)
        
        # Azure Monitor integration
        if self.azure_exporter:
            self._send_azure_metric('db_query_duration', {
                'query_type': query_type,
                'table': table,
                'namespace': namespace
            }, duration)
    
    # Enhanced Production Metrics Methods
    def record_oee(self, line_id: str, equipment_code: str, oee: float, 
                   shift: str = "day", namespace: str = "ms5-production"):
        """Record enhanced OEE metrics with shift and namespace dimensions."""
        if PROMETHEUS_AVAILABLE:
            self.production_oee.labels(
                line_id=line_id,
                equipment_code=equipment_code,
                shift=shift,
                namespace=namespace
            ).set(oee)
        
        # Azure Monitor integration
        if self.azure_exporter:
            self._send_azure_metric('production_oee', {
                'line_id': line_id,
                'equipment_code': equipment_code,
                'shift': shift,
                'namespace': namespace
            }, oee)
        
        # Store in custom metrics
        if 'oee_data' not in self.custom_metrics['business_kpis']:
            self.custom_metrics['business_kpis']['oee_data'] = {}
        
        self.custom_metrics['business_kpis']['oee_data'][f"{line_id}_{equipment_code}"] = {
            'oee': oee,
            'timestamp': datetime.now(),
            'line_id': line_id,
            'equipment_code': equipment_code,
            'shift': shift,
            'namespace': namespace
        }
    
    def record_production_throughput(self, line_id: str, product_type: str, throughput: float, 
                                   shift: str = "day", namespace: str = "ms5-production"):
        """Record enhanced production throughput metrics."""
        if PROMETHEUS_AVAILABLE:
            self.production_throughput.labels(
                line_id=line_id,
                product_type=product_type,
                shift=shift,
                namespace=namespace
            ).set(throughput)
        
        # Azure Monitor integration
        if self.azure_exporter:
            self._send_azure_metric('production_throughput', {
                'line_id': line_id,
                'product_type': product_type,
                'shift': shift,
                'namespace': namespace
            }, throughput)
    
    # Enhanced Andon Metrics Methods
    def record_andon_event(self, priority: str, status: str, line_id: str, 
                          response_time: float = None, namespace: str = "ms5-production"):
        """Record enhanced Andon event metrics."""
        if PROMETHEUS_AVAILABLE:
            self.andon_events_active.labels(
                priority=priority,
                status=status,
                line_id=line_id,
                namespace=namespace
            ).inc()
            
            if response_time is not None:
                self.andon_response_time.labels(
                    priority=priority,
                    line_id=line_id
                ).observe(response_time)
        
        # Azure Monitor integration
        if self.azure_exporter:
            self._send_azure_metric('andon_event', {
                'priority': priority,
                'status': status,
                'line_id': line_id,
                'namespace': namespace
            }, 1)
            
            if response_time is not None:
                self._send_azure_metric('andon_response_time', {
                    'priority': priority,
                    'line_id': line_id,
                    'namespace': namespace
                }, response_time)
    
    # Enhanced Quality Metrics Methods
    def record_defect_rate(self, line_id: str, product_type: str, defect_rate: float, 
                          shift: str = "day", namespace: str = "ms5-production"):
        """Record enhanced quality defect rate metrics."""
        if PROMETHEUS_AVAILABLE:
            self.quality_defect_rate.labels(
                line_id=line_id,
                product_type=product_type,
                shift=shift,
                namespace=namespace
            ).set(defect_rate)
        
        # Azure Monitor integration
        if self.azure_exporter:
            self._send_azure_metric('quality_defect_rate', {
                'line_id': line_id,
                'product_type': product_type,
                'shift': shift,
                'namespace': namespace
            }, defect_rate)
    
    # Enhanced Maintenance Metrics Methods
    def record_maintenance_downtime(self, equipment_code: str, maintenance_type: str, 
                                  downtime_minutes: float, namespace: str = "ms5-production"):
        """Record enhanced maintenance downtime metrics."""
        if PROMETHEUS_AVAILABLE:
            self.maintenance_downtime_minutes.labels(
                equipment_code=equipment_code,
                maintenance_type=maintenance_type,
                namespace=namespace
            ).set(downtime_minutes)
        
        # Azure Monitor integration
        if self.azure_exporter:
            self._send_azure_metric('maintenance_downtime', {
                'equipment_code': equipment_code,
                'maintenance_type': maintenance_type,
                'namespace': namespace
            }, downtime_minutes)
    
    # Enhanced System Metrics Methods
    def record_memory_usage(self, component: str, memory_bytes: int, 
                           namespace: str = "ms5-production", pod_name: str = "unknown"):
        """Record enhanced memory usage metrics."""
        if PROMETHEUS_AVAILABLE:
            self.system_memory_usage.labels(
                component=component,
                namespace=namespace,
                pod_name=pod_name
            ).set(memory_bytes)
        
        # Azure Monitor integration
        if self.azure_exporter:
            self._send_azure_metric('memory_usage', {
                'component': component,
                'namespace': namespace,
                'pod_name': pod_name
            }, memory_bytes)
    
    def record_cpu_usage(self, component: str, cpu_percent: float, 
                        namespace: str = "ms5-production", pod_name: str = "unknown"):
        """Record enhanced CPU usage metrics."""
        if PROMETHEUS_AVAILABLE:
            self.system_cpu_usage.labels(
                component=component,
                namespace=namespace,
                pod_name=pod_name
            ).set(cpu_percent)
        
        # Azure Monitor integration
        if self.azure_exporter:
            self._send_azure_metric('cpu_usage', {
                'component': component,
                'namespace': namespace,
                'pod_name': pod_name
            }, cpu_percent)
    
    # Enhanced Business Metrics Methods
    def record_production_efficiency(self, line_id: str, efficiency_percent: float, 
                                   shift: str = "day", namespace: str = "ms5-production"):
        """Record enhanced production efficiency metrics."""
        if PROMETHEUS_AVAILABLE:
            self.production_efficiency.labels(
                line_id=line_id,
                shift=shift,
                namespace=namespace
            ).set(efficiency_percent)
        
        # Azure Monitor integration
        if self.azure_exporter:
            self._send_azure_metric('production_efficiency', {
                'line_id': line_id,
                'shift': shift,
                'namespace': namespace
            }, efficiency_percent)
        
        # Store in custom metrics
        if 'efficiency_data' not in self.custom_metrics['business_kpis']:
            self.custom_metrics['business_kpis']['efficiency_data'] = {}
        
        self.custom_metrics['business_kpis']['efficiency_data'][line_id] = {
            'efficiency': efficiency_percent,
            'timestamp': datetime.now(),
            'line_id': line_id,
            'shift': shift,
            'namespace': namespace
        }
    
    def record_energy_consumption(self, equipment_code: str, consumption_kwh: float, 
                                 namespace: str = "ms5-production"):
        """Record enhanced energy consumption metrics."""
        if PROMETHEUS_AVAILABLE:
            self.energy_consumption_kwh.labels(
                equipment_code=equipment_code,
                namespace=namespace
            ).set(consumption_kwh)
        
        # Azure Monitor integration
        if self.azure_exporter:
            self._send_azure_metric('energy_consumption', {
                'equipment_code': equipment_code,
                'namespace': namespace
            }, consumption_kwh)
    
    # Cost Monitoring Methods
    def record_resource_cost(self, resource_type: str, cost_usd: float, 
                           namespace: str = "ms5-production", node_name: str = "unknown"):
        """Record resource cost metrics."""
        if PROMETHEUS_AVAILABLE:
            self.resource_cost_usd.labels(
                resource_type=resource_type,
                namespace=namespace,
                node_name=node_name
            ).set(cost_usd)
        
        # Azure Monitor integration
        if self.azure_exporter:
            self._send_azure_metric('resource_cost', {
                'resource_type': resource_type,
                'namespace': namespace,
                'node_name': node_name
            }, cost_usd)
        
        # Store in custom metrics
        if 'cost_data' not in self.custom_metrics['cost_metrics']:
            self.custom_metrics['cost_metrics']['cost_data'] = {}
        
        self.custom_metrics['cost_metrics']['cost_data'][f"{resource_type}_{namespace}"] = {
            'cost_usd': cost_usd,
            'timestamp': datetime.now(),
            'resource_type': resource_type,
            'namespace': namespace,
            'node_name': node_name
        }
    
    # SLI/SLO Monitoring Methods
    def record_sli_value(self, sli_name: str, value: float, service: str, 
                        namespace: str = "ms5-production"):
        """Record Service Level Indicator value."""
        if PROMETHEUS_AVAILABLE:
            self.sli_value.labels(
                sli_name=sli_name,
                service=service,
                namespace=namespace
            ).set(value)
        
        # Azure Monitor integration
        if self.azure_exporter:
            self._send_azure_metric('sli_value', {
                'sli_name': sli_name,
                'service': service,
                'namespace': namespace
            }, value)
        
        # Store in custom metrics
        if 'sli_data' not in self.custom_metrics['sli_slo_data']:
            self.custom_metrics['sli_slo_data']['sli_data'] = {}
        
        self.custom_metrics['sli_slo_data']['sli_data'][sli_name] = {
            'value': value,
            'timestamp': datetime.now(),
            'service': service,
            'namespace': namespace
        }
    
    def record_slo_error_budget(self, slo_name: str, error_budget_percent: float, 
                               service: str, namespace: str = "ms5-production"):
        """Record Service Level Objective error budget."""
        if PROMETHEUS_AVAILABLE:
            self.slo_error_budget.labels(
                slo_name=slo_name,
                service=service,
                namespace=namespace
            ).set(error_budget_percent)
        
        # Azure Monitor integration
        if self.azure_exporter:
            self._send_azure_metric('slo_error_budget', {
                'slo_name': slo_name,
                'service': service,
                'namespace': namespace
            }, error_budget_percent)
        
        # Store in custom metrics
        if 'slo_data' not in self.custom_metrics['sli_slo_data']:
            self.custom_metrics['sli_slo_data']['slo_data'] = {}
        
        self.custom_metrics['sli_slo_data']['slo_data'][slo_name] = {
            'error_budget_percent': error_budget_percent,
            'timestamp': datetime.now(),
            'service': service,
            'namespace': namespace
        }
    
    # Azure Monitor Helper Methods
    def _send_azure_metric(self, metric_name: str, dimensions: Dict[str, str], value: float):
        """Send metric to Azure Monitor."""
        if not self.azure_exporter:
            return
        
        try:
            # Create custom metric for Azure Monitor
            custom_metric = {
                'name': f'MS5.0.{metric_name}',
                'value': value,
                'dimensions': dimensions,
                'timestamp': datetime.now().isoformat()
            }
            
            # Send to Azure Monitor (implementation depends on Azure SDK)
            # This is a placeholder for the actual Azure Monitor integration
            logger.debug(f"Sending metric to Azure Monitor: {custom_metric}")
            
        except Exception as e:
            logger.error(f"Failed to send metric to Azure Monitor: {e}")
    
    # Enhanced Business KPI Calculations
    async def calculate_real_time_oee(self, line_id: str, shift: str = "day") -> Dict[str, Any]:
        """Calculate real-time OEE with enhanced metrics and Azure Monitor integration."""
        # Get current production data (this would integrate with the database)
        production_data = await self._get_production_data(line_id, shift)
        
        # Calculate OEE components
        availability = production_data['actual_runtime'] / production_data['planned_runtime']
        performance = production_data['actual_output'] / production_data['theoretical_output']
        quality = production_data['good_output'] / production_data['actual_output']
        
        oee = availability * performance * quality
        
        # Record metrics
        self.record_oee(line_id, production_data['equipment_code'], oee, shift)
        
        # Record individual components
        self.record_production_efficiency(line_id, availability * 100, shift)
        
        return {
            'line_id': line_id,
            'shift': shift,
            'oee': oee,
            'availability': availability,
            'performance': performance,
            'quality': quality,
            'timestamp': datetime.now(),
            'namespace': 'ms5-production'
        }
    
    async def _get_production_data(self, line_id: str, shift: str) -> Dict[str, Any]:
        """Get production data for OEE calculation."""
        # This would integrate with the actual database
        # For now, return sample data
        return {
            'line_id': line_id,
            'shift': shift,
            'equipment_code': f'EQ_{line_id}',
            'actual_runtime': 7.5,  # hours
            'planned_runtime': 8.0,  # hours
            'actual_output': 150,  # units
            'theoretical_output': 200,  # units
            'good_output': 144  # units
        }
    
    # Enhanced Metrics Export Methods
    def get_prometheus_metrics(self) -> str:
        """Get Prometheus-formatted metrics."""
        if PROMETHEUS_AVAILABLE and self.registry:
            return generate_latest(self.registry).decode('utf-8')
        return ""
    
    def get_custom_metrics(self) -> Dict[str, Any]:
        """Get enhanced custom metrics data."""
        return self.custom_metrics
    
    def get_sli_slo_data(self) -> Dict[str, Any]:
        """Get SLI/SLO data for monitoring dashboards."""
        return {
            'sli_definitions': {name: asdict(sli) for name, sli in self.sli_definitions.items()},
            'slo_definitions': {name: asdict(slo) for name, slo in self.slo_definitions.items()},
            'sli_data': self.custom_metrics['sli_slo_data'].get('sli_data', {}),
            'slo_data': self.custom_metrics['sli_slo_data'].get('slo_data', {})
        }
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get enhanced metrics summary for monitoring dashboard."""
        summary = {
            'timestamp': datetime.now(),
            'prometheus_available': PROMETHEUS_AVAILABLE,
            'azure_monitor_available': self.azure_exporter is not None,
            'custom_metrics_count': sum(len(category) for category in self.custom_metrics.values()),
            'business_kpis': {
                'production_kpis_count': len(self.custom_metrics['business_kpis'].get('production_kpis', {})),
                'efficiency_data_count': len(self.custom_metrics['business_kpis'].get('efficiency_data', {})),
                'oee_data_count': len(self.custom_metrics['business_kpis'].get('oee_data', {}))
            },
            'quality_trends_count': len(self.custom_metrics['quality_trends'].get('quality_metrics', {})),
            'maintenance_trends_count': len(self.custom_metrics['maintenance_trends'].get('maintenance_metrics', {})),
            'system_health_count': len(self.custom_metrics['system_health']),
            'cost_metrics_count': len(self.custom_metrics['cost_metrics'].get('cost_data', {})),
            'sli_slo_count': len(self.custom_metrics['sli_slo_data'].get('sli_data', {})) + 
                           len(self.custom_metrics['sli_slo_data'].get('slo_data', {}))
        }
        
        return summary
    
    # Enhanced Alerting Thresholds
    def check_enhanced_alerting_thresholds(self) -> List[Dict[str, Any]]:
        """Check metrics against enhanced alerting thresholds and return alerts."""
        alerts = []
        
        # Check OEE thresholds with enhanced dimensions
        oee_data = self.custom_metrics['business_kpis'].get('oee_data', {})
        for key, data in oee_data.items():
            if data['oee'] < 0.75:  # OEE below 75%
                alerts.append({
                    'type': 'warning',
                    'metric': 'oee',
                    'line_id': data['line_id'],
                    'equipment_code': data['equipment_code'],
                    'shift': data.get('shift', 'unknown'),
                    'namespace': data.get('namespace', 'unknown'),
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
                    'shift': data.get('shift', 'unknown'),
                    'namespace': data.get('namespace', 'unknown'),
                    'value': data['oee'],
                    'threshold': 0.60,
                    'message': f"OEE critically low: {data['oee']:.2%} < 60%",
                    'timestamp': data['timestamp']
                })
        
        # Check efficiency thresholds with enhanced dimensions
        efficiency_data = self.custom_metrics['business_kpis'].get('efficiency_data', {})
        for line_id, data in efficiency_data.items():
            if data['efficiency'] < 80:  # Efficiency below 80%
                alerts.append({
                    'type': 'warning',
                    'metric': 'efficiency',
                    'line_id': line_id,
                    'shift': data.get('shift', 'unknown'),
                    'namespace': data.get('namespace', 'unknown'),
                    'value': data['efficiency'],
                    'threshold': 80,
                    'message': f"Production efficiency below threshold: {data['efficiency']:.1f}% < 80%",
                    'timestamp': data['timestamp']
                })
        
        # Check API performance thresholds with enhanced dimensions
        system_health = self.custom_metrics['system_health']
        for endpoint, data in system_health.items():
            if data['avg_duration'] > 2.0:  # API response time > 2 seconds
                alerts.append({
                    'type': 'warning',
                    'metric': 'api_performance',
                    'endpoint': endpoint,
                    'namespace': data.get('namespace', 'unknown'),
                    'pod_name': data.get('pod_name', 'unknown'),
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
                    'namespace': data.get('namespace', 'unknown'),
                    'pod_name': data.get('pod_name', 'unknown'),
                    'value': error_rate,
                    'threshold': 5.0,
                    'message': f"API error rate high: {error_rate:.1f}% > 5%",
                    'timestamp': data['last_updated']
                })
        
        # Check cost thresholds
        cost_data = self.custom_metrics['cost_metrics'].get('cost_data', {})
        for resource_key, data in cost_data.items():
            if data['cost_usd'] > 1000:  # Cost > $1000
                alerts.append({
                    'type': 'warning',
                    'metric': 'resource_cost',
                    'resource_type': data['resource_type'],
                    'namespace': data['namespace'],
                    'node_name': data.get('node_name', 'unknown'),
                    'value': data['cost_usd'],
                    'threshold': 1000,
                    'message': f"Resource cost high: ${data['cost_usd']:.2f} > $1000",
                    'timestamp': data['timestamp']
                })
        
        return alerts


# Global enhanced metrics instance
_aks_application_metrics = None

def get_aks_application_metrics() -> AKSApplicationMetrics:
    """Get global enhanced AKS application metrics instance."""
    global _aks_application_metrics
    if _aks_application_metrics is None:
        _aks_application_metrics = AKSApplicationMetrics()
    return _aks_application_metrics


# Enhanced decorator for automatic metrics collection
def collect_aks_metrics(metric_type: str, **metric_labels):
    """
    Enhanced decorator for automatic metrics collection with Kubernetes dimensions.
    
    Args:
        metric_type: Type of metric (api_request, db_query, etc.)
        **metric_labels: Labels for the metric including Kubernetes dimensions
        
    Returns:
        Decorated function
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            namespace = metric_labels.get('namespace', 'ms5-production')
            pod_name = metric_labels.get('pod_name', os.getenv('HOSTNAME', 'unknown'))
            
            try:
                result = await func(*args, **kwargs)
                
                # Record successful execution
                metrics = get_aks_application_metrics()
                if metric_type == 'api_request':
                    method = metric_labels.get('method', 'UNKNOWN')
                    endpoint = metric_labels.get('endpoint', func.__name__)
                    metrics.record_api_request(method, endpoint, 200, time.time() - start_time, namespace, pod_name)
                elif metric_type == 'db_query':
                    query_type = metric_labels.get('query_type', 'SELECT')
                    table = metric_labels.get('table', 'unknown')
                    metrics.record_db_query(query_type, table, time.time() - start_time, namespace)
                
                return result
                
            except Exception as e:
                # Record failed execution
                metrics = get_aks_application_metrics()
                if metric_type == 'api_request':
                    method = metric_labels.get('method', 'UNKNOWN')
                    endpoint = metric_labels.get('endpoint', func.__name__)
                    metrics.record_api_request(method, endpoint, 500, time.time() - start_time, namespace, pod_name)
                
                raise
        
        return wrapper
    return decorator


# Context manager for distributed tracing correlation
@asynccontextmanager
async def trace_correlation(trace_id: str, span_id: str):
    """
    Context manager for distributed tracing correlation.
    
    Args:
        trace_id: Distributed trace ID
        span_id: Span ID for correlation
        
    Yields:
        Correlation context
    """
    # Set correlation context for Azure Monitor
    correlation_context = {
        'trace_id': trace_id,
        'span_id': span_id,
        'timestamp': datetime.now()
    }
    
    try:
        yield correlation_context
    finally:
        # Clean up correlation context
        pass
