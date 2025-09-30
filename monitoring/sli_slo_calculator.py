# MS5.0 Floor Dashboard - Phase 6B: SLI/SLO Calculator
# Service Level Indicators and Objectives calculation service

import asyncio
import logging
import time
import yaml
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
import os
import json
from pathlib import Path

import aiohttp
from aiohttp import web
from prometheus_client import Counter, Gauge, start_http_server

logger = logging.getLogger(__name__)

@dataclass
class SLIDefinition:
    """Service Level Indicator definition."""
    name: str
    metric_name: str
    target_value: float
    comparison_operator: str
    window_duration: str
    description: str
    calculation: str

@dataclass
class SLODefinition:
    """Service Level Objective definition."""
    name: str
    sli_name: str
    target_percentage: float
    error_budget_percentage: float
    window_duration: str
    description: str
    alert_thresholds: Dict[str, float]

@dataclass
class SLIResult:
    """SLI calculation result."""
    name: str
    value: float
    target_value: float
    meets_target: bool
    timestamp: datetime
    calculation: str

@dataclass
class SLOResult:
    """SLO calculation result."""
    name: str
    sli_name: str
    target_percentage: float
    error_budget_percentage: float
    current_error_budget: float
    meets_target: bool
    timestamp: datetime

class SLISLOCalculator:
    """
    Service Level Indicators and Objectives calculator for MS5.0 Floor Dashboard.
    
    This service calculates SLI and SLO values in real-time, tracks error budgets,
    and provides comprehensive monitoring of service level objectives.
    """
    
    def __init__(self, prometheus_url: str, namespace: str = "ms5-production"):
        """
        Initialize SLI/SLO calculator.
        
        Args:
            prometheus_url: Prometheus server URL
            namespace: Kubernetes namespace
        """
        self.prometheus_url = prometheus_url
        self.namespace = namespace
        self.sli_definitions: Dict[str, SLIDefinition] = {}
        self.slo_definitions: Dict[str, SLODefinition] = {}
        self.sli_results: Dict[str, SLIResult] = {}
        self.slo_results: Dict[str, SLOResult] = {}
        self.error_budgets: Dict[str, float] = {}
        
        # Prometheus metrics
        self.sli_calculations_total = Counter(
            'ms5_sli_calculations_total',
            'Total number of SLI calculations',
            ['sli_name', 'result']
        )
        
        self.slo_calculations_total = Counter(
            'ms5_slo_calculations_total',
            'Total number of SLO calculations',
            ['slo_name', 'result']
        )
        
        self.sli_value = Gauge(
            'ms5_sli_value',
            'Service Level Indicator value',
            ['sli_name', 'service', 'namespace']
        )
        
        self.slo_error_budget = Gauge(
            'ms5_slo_error_budget_percent',
            'Service Level Objective error budget percentage',
            ['slo_name', 'service', 'namespace']
        )
        
        self.calculation_duration = Gauge(
            'ms5_sli_slo_calculation_duration_seconds',
            'SLI/SLO calculation duration in seconds',
            ['calculation_type']
        )
        
        logger.info("SLI/SLO calculator initialized")
    
    async def load_configurations(self, config_path: str = "/etc/sli-slo"):
        """Load SLI/SLO definitions from configuration files."""
        try:
            # Load SLI definitions
            sli_config_path = Path(config_path) / "sli-definitions.yaml"
            if sli_config_path.exists():
                with open(sli_config_path, 'r') as f:
                    sli_config = yaml.safe_load(f)
                    await self._load_sli_definitions(sli_config)
            
            # Load SLO definitions
            slo_config_path = Path(config_path) / "slo-definitions.yaml"
            if slo_config_path.exists():
                with open(slo_config_path, 'r') as f:
                    slo_config = yaml.safe_load(f)
                    await self._load_slo_definitions(slo_config)
            
            logger.info(f"Loaded {len(self.sli_definitions)} SLI definitions and {len(self.slo_definitions)} SLO definitions")
            
        except Exception as e:
            logger.error(f"Failed to load configurations: {e}")
            raise
    
    async def _load_sli_definitions(self, config: Dict[str, Any]):
        """Load SLI definitions from configuration."""
        for sli_config in config.get('slis', []):
            sli_def = SLIDefinition(
                name=sli_config['name'],
                metric_name=sli_config['metric_name'],
                target_value=sli_config['target_value'],
                comparison_operator=sli_config['comparison_operator'],
                window_duration=sli_config['window_duration'],
                description=sli_config['description'],
                calculation=sli_config['calculation']
            )
            self.sli_definitions[sli_def.name] = sli_def
    
    async def _load_slo_definitions(self, config: Dict[str, Any]):
        """Load SLO definitions from configuration."""
        for slo_config in config.get('slos', []):
            slo_def = SLODefinition(
                name=slo_config['name'],
                sli_name=slo_config['sli_name'],
                target_percentage=slo_config['target_percentage'],
                error_budget_percentage=slo_config['error_budget_percentage'],
                window_duration=slo_config['window_duration'],
                description=slo_config['description'],
                alert_thresholds=slo_config.get('alert_thresholds', {})
            )
            self.slo_definitions[slo_def.name] = slo_def
    
    async def calculate_sli(self, sli_name: str) -> Optional[SLIResult]:
        """
        Calculate SLI value for a specific indicator.
        
        Args:
            sli_name: Name of the SLI to calculate
            
        Returns:
            SLI calculation result or None if calculation fails
        """
        if sli_name not in self.sli_definitions:
            logger.warning(f"SLI definition not found: {sli_name}")
            return None
        
        sli_def = self.sli_definitions[sli_name]
        start_time = time.time()
        
        try:
            # Query Prometheus for the metric value
            value = await self._query_prometheus(sli_def.calculation)
            
            # Determine if the target is met
            meets_target = self._evaluate_target(value, sli_def.target_value, sli_def.comparison_operator)
            
            # Create result
            result = SLIResult(
                name=sli_name,
                value=value,
                target_value=sli_def.target_value,
                meets_target=meets_target,
                timestamp=datetime.now(),
                calculation=sli_def.calculation
            )
            
            # Store result
            self.sli_results[sli_name] = result
            
            # Update Prometheus metrics
            self.sli_value.labels(
                sli_name=sli_name,
                service=self._extract_service_from_sli(sli_name),
                namespace=self.namespace
            ).set(value)
            
            self.sli_calculations_total.labels(
                sli_name=sli_name,
                result="success" if meets_target else "failure"
            ).inc()
            
            # Record calculation duration
            duration = time.time() - start_time
            self.calculation_duration.labels(calculation_type="sli").set(duration)
            
            logger.debug(f"Calculated SLI {sli_name}: {value} (target: {sli_def.target_value}, meets: {meets_target})")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to calculate SLI {sli_name}: {e}")
            self.sli_calculations_total.labels(
                sli_name=sli_name,
                result="error"
            ).inc()
            return None
    
    async def calculate_slo(self, slo_name: str) -> Optional[SLOResult]:
        """
        Calculate SLO and error budget for a specific objective.
        
        Args:
            slo_name: Name of the SLO to calculate
            
        Returns:
            SLO calculation result or None if calculation fails
        """
        if slo_name not in self.slo_definitions:
            logger.warning(f"SLO definition not found: {slo_name}")
            return None
        
        slo_def = self.slo_definitions[slo_name]
        start_time = time.time()
        
        try:
            # Get the corresponding SLI result
            if slo_def.sli_name not in self.sli_results:
                logger.warning(f"SLI result not found for SLO {slo_name}: {slo_def.sli_name}")
                return None
            
            sli_result = self.sli_results[slo_def.sli_name]
            
            # Calculate error budget
            error_budget = self._calculate_error_budget(sli_result, slo_def)
            
            # Determine if SLO target is met
            meets_target = sli_result.meets_target
            
            # Create result
            result = SLOResult(
                name=slo_name,
                sli_name=slo_def.sli_name,
                target_percentage=slo_def.target_percentage,
                error_budget_percentage=slo_def.error_budget_percentage,
                current_error_budget=error_budget,
                meets_target=meets_target,
                timestamp=datetime.now()
            )
            
            # Store result
            self.slo_results[slo_name] = result
            self.error_budgets[slo_name] = error_budget
            
            # Update Prometheus metrics
            self.slo_error_budget.labels(
                slo_name=slo_name,
                service=self._extract_service_from_slo(slo_name),
                namespace=self.namespace
            ).set(error_budget)
            
            self.slo_calculations_total.labels(
                slo_name=slo_name,
                result="success" if meets_target else "failure"
            ).inc()
            
            # Record calculation duration
            duration = time.time() - start_time
            self.calculation_duration.labels(calculation_type="slo").set(duration)
            
            logger.debug(f"Calculated SLO {slo_name}: error_budget={error_budget}%, meets_target={meets_target}")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to calculate SLO {slo_name}: {e}")
            self.slo_calculations_total.labels(
                slo_name=slo_name,
                result="error"
            ).inc()
            return None
    
    async def _query_prometheus(self, query: str) -> float:
        """
        Query Prometheus for metric value.
        
        Args:
            query: Prometheus query expression
            
        Returns:
            Metric value
        """
        async with aiohttp.ClientSession() as session:
            params = {
                'query': query,
                'time': int(time.time())
            }
            
            async with session.get(f"{self.prometheus_url}/api/v1/query", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data['status'] == 'success' and data['data']['result']:
                        return float(data['data']['result'][0]['value'][1])
                    else:
                        raise ValueError(f"Prometheus query returned no data: {query}")
                else:
                    raise ValueError(f"Prometheus query failed with status {response.status}")
    
    def _evaluate_target(self, value: float, target: float, operator: str) -> bool:
        """
        Evaluate if a value meets the target based on the comparison operator.
        
        Args:
            value: Actual value
            target: Target value
            operator: Comparison operator (>, <, >=, <=, ==)
            
        Returns:
            True if target is met, False otherwise
        """
        if operator == '>':
            return value > target
        elif operator == '<':
            return value < target
        elif operator == '>=':
            return value >= target
        elif operator == '<=':
            return value <= target
        elif operator == '==':
            return value == target
        else:
            raise ValueError(f"Unknown comparison operator: {operator}")
    
    def _calculate_error_budget(self, sli_result: SLIResult, slo_def: SLODefinition) -> float:
        """
        Calculate error budget percentage for an SLO.
        
        Args:
            sli_result: SLI calculation result
            slo_def: SLO definition
            
        Returns:
            Error budget percentage
        """
        # Simple error budget calculation based on SLI performance
        # In a real implementation, this would be more sophisticated
        if sli_result.meets_target:
            return slo_def.error_budget_percentage
        else:
            # Calculate how much error budget is consumed
            error_rate = abs(sli_result.value - sli_result.target_value) / sli_result.target_value
            consumed_budget = min(error_rate * 100, slo_def.error_budget_percentage)
            return max(0, slo_def.error_budget_percentage - consumed_budget)
    
    def _extract_service_from_sli(self, sli_name: str) -> str:
        """Extract service name from SLI name."""
        if 'api' in sli_name:
            return 'api'
        elif 'db' in sli_name:
            return 'database'
        elif 'production' in sli_name:
            return 'production'
        elif 'andon' in sli_name:
            return 'andon'
        elif 'quality' in sli_name:
            return 'quality'
        elif 'system' in sli_name:
            return 'system'
        else:
            return 'unknown'
    
    def _extract_service_from_slo(self, slo_name: str) -> str:
        """Extract service name from SLO name."""
        return self._extract_service_from_sli(slo_name)
    
    async def calculate_all_slis(self) -> Dict[str, SLIResult]:
        """Calculate all defined SLIs."""
        results = {}
        for sli_name in self.sli_definitions:
            result = await self.calculate_sli(sli_name)
            if result:
                results[sli_name] = result
        return results
    
    async def calculate_all_slos(self) -> Dict[str, SLOResult]:
        """Calculate all defined SLOs."""
        results = {}
        for slo_name in self.slo_definitions:
            result = await self.calculate_slo(slo_name)
            if result:
                results[slo_name] = result
        return results
    
    async def run_calculation_loop(self, interval: int = 30):
        """
        Run continuous SLI/SLO calculation loop.
        
        Args:
            interval: Calculation interval in seconds
        """
        logger.info(f"Starting SLI/SLO calculation loop with {interval}s interval")
        
        while True:
            try:
                # Calculate all SLIs first
                await self.calculate_all_slis()
                
                # Then calculate all SLOs
                await self.calculate_all_slos()
                
                logger.debug(f"Completed SLI/SLO calculation cycle")
                
            except Exception as e:
                logger.error(f"Error in calculation loop: {e}")
            
            await asyncio.sleep(interval)
    
    def get_sli_results(self) -> Dict[str, Dict[str, Any]]:
        """Get all SLI results as dictionaries."""
        return {
            name: {
                'name': result.name,
                'value': result.value,
                'target_value': result.target_value,
                'meets_target': result.meets_target,
                'timestamp': result.timestamp.isoformat(),
                'calculation': result.calculation
            }
            for name, result in self.sli_results.items()
        }
    
    def get_slo_results(self) -> Dict[str, Dict[str, Any]]:
        """Get all SLO results as dictionaries."""
        return {
            name: {
                'name': result.name,
                'sli_name': result.sli_name,
                'target_percentage': result.target_percentage,
                'error_budget_percentage': result.error_budget_percentage,
                'current_error_budget': result.current_error_budget,
                'meets_target': result.meets_target,
                'timestamp': result.timestamp.isoformat()
            }
            for name, result in self.slo_results.items()
        }
    
    def get_summary(self) -> Dict[str, Any]:
        """Get calculation summary."""
        total_slis = len(self.sli_definitions)
        total_slos = len(self.slo_definitions)
        calculated_slis = len(self.sli_results)
        calculated_slos = len(self.slo_results)
        
        sli_success_rate = sum(1 for r in self.sli_results.values() if r.meets_target) / calculated_slis if calculated_slis > 0 else 0
        slo_success_rate = sum(1 for r in self.slo_results.values() if r.meets_target) / calculated_slos if calculated_slos > 0 else 0
        
        return {
            'timestamp': datetime.now().isoformat(),
            'total_slis': total_slis,
            'total_slos': total_slos,
            'calculated_slis': calculated_slis,
            'calculated_slos': calculated_slos,
            'sli_success_rate': sli_success_rate,
            'slo_success_rate': slo_success_rate,
            'error_budgets': self.error_budgets
        }


# Web server for health checks and API endpoints
class SLISLOServer:
    """Web server for SLI/SLO calculator service."""
    
    def __init__(self, calculator: SLISLOCalculator):
        self.calculator = calculator
        self.app = web.Application()
        self._setup_routes()
    
    def _setup_routes(self):
        """Setup web server routes."""
        self.app.router.add_get('/health', self.health_check)
        self.app.router.add_get('/ready', self.ready_check)
        self.app.router.add_get('/metrics', self.metrics_endpoint)
        self.app.router.add_get('/slis', self.get_slis)
        self.app.router.add_get('/slos', self.get_slos)
        self.app.router.add_get('/summary', self.get_summary)
        self.app.router.add_post('/calculate/sli/{sli_name}', self.calculate_sli_endpoint)
        self.app.router.add_post('/calculate/slo/{slo_name}', self.calculate_slo_endpoint)
    
    async def health_check(self, request):
        """Health check endpoint."""
        return web.json_response({'status': 'healthy', 'timestamp': datetime.now().isoformat()})
    
    async def ready_check(self, request):
        """Readiness check endpoint."""
        if len(self.calculator.sli_definitions) > 0 and len(self.calculator.slo_definitions) > 0:
            return web.json_response({'status': 'ready', 'timestamp': datetime.now().isoformat()})
        else:
            return web.json_response({'status': 'not_ready', 'timestamp': datetime.now().isoformat()}, status=503)
    
    async def metrics_endpoint(self, request):
        """Prometheus metrics endpoint."""
        from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
        return web.Response(
            body=generate_latest(),
            content_type=CONTENT_TYPE_LATEST
        )
    
    async def get_slis(self, request):
        """Get all SLI results."""
        return web.json_response(self.calculator.get_sli_results())
    
    async def get_slos(self, request):
        """Get all SLO results."""
        return web.json_response(self.calculator.get_slo_results())
    
    async def get_summary(self, request):
        """Get calculation summary."""
        return web.json_response(self.calculator.get_summary())
    
    async def calculate_sli_endpoint(self, request):
        """Calculate specific SLI."""
        sli_name = request.match_info['sli_name']
        result = await self.calculator.calculate_sli(sli_name)
        if result:
            return web.json_response({
                'name': result.name,
                'value': result.value,
                'target_value': result.target_value,
                'meets_target': result.meets_target,
                'timestamp': result.timestamp.isoformat()
            })
        else:
            return web.json_response({'error': f'Failed to calculate SLI: {sli_name}'}, status=500)
    
    async def calculate_slo_endpoint(self, request):
        """Calculate specific SLO."""
        slo_name = request.match_info['slo_name']
        result = await self.calculator.calculate_slo(slo_name)
        if result:
            return web.json_response({
                'name': result.name,
                'sli_name': result.sli_name,
                'target_percentage': result.target_percentage,
                'error_budget_percentage': result.error_budget_percentage,
                'current_error_budget': result.current_error_budget,
                'meets_target': result.meets_target,
                'timestamp': result.timestamp.isoformat()
            })
        else:
            return web.json_response({'error': f'Failed to calculate SLO: {slo_name}'}, status=500)


async def main():
    """Main function to run the SLI/SLO calculator service."""
    # Configuration
    prometheus_url = os.getenv('PROMETHEUS_URL', 'http://prometheus.ms5-production.svc.cluster.local:9090')
    namespace = os.getenv('NAMESPACE', 'ms5-production')
    calculation_interval = int(os.getenv('CALCULATION_INTERVAL', '30'))
    config_path = os.getenv('CONFIG_PATH', '/etc/sli-slo')
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize calculator
    calculator = SLISLOCalculator(prometheus_url, namespace)
    
    # Load configurations
    await calculator.load_configurations(config_path)
    
    # Start Prometheus metrics server
    start_http_server(8080)
    
    # Initialize web server
    server = SLISLOServer(calculator)
    
    # Start web server
    runner = web.AppRunner(server.app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    
    logger.info("SLI/SLO calculator service started")
    
    # Run calculation loop
    await calculator.run_calculation_loop(calculation_interval)


if __name__ == '__main__':
    asyncio.run(main())
