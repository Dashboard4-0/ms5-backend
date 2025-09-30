"""
MS5.0 Floor Dashboard - Real-time Integration Service

This module provides real-time integration between production services
and WebSocket broadcasting for live updates.
"""

import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime
import structlog

from app.services.enhanced_websocket_manager import EnhancedWebSocketManager
from app.services.enhanced_metric_transformer import EnhancedMetricTransformer
from app.services.enhanced_telemetry_poller import EnhancedTelemetryPoller
from app.services.equipment_job_mapper import EquipmentJobMapper
from app.services.plc_integrated_oee_calculator import PLCIntegratedOEECalculator
from app.services.plc_integrated_downtime_tracker import PLCIntegratedDowntimeTracker
from app.services.plc_integrated_andon_service import PLCIntegratedAndonService
from app.services.production_service import ProductionService
from app.services.andon_service import AndonService
from app.services.notification_service import NotificationService, EnhancedNotificationService
from app.services.production_service import ProductionStatisticsService
from app.services.oee_calculator import OEECalculator

logger = structlog.get_logger()


class RealTimeIntegrationService:
    """Service for real-time integration between production services and WebSocket broadcasting."""
    
    def __init__(self, websocket_manager: EnhancedWebSocketManager):
        self.websocket_manager = websocket_manager
        self.production_service = None
        self.andon_service = None
        self.notification_service = None
        self.enhanced_notification_service = None
        self.production_statistics_service = None
        self.oee_calculator = None
        self.equipment_job_mapper = None
        self.downtime_tracker = None
        self.andon_service_plc = None
        self.enhanced_poller = None
        self.is_running = False
        self.background_tasks = []
    
    async def initialize(self):
        """Initialize the real-time integration service."""
        try:
            # Initialize production services
            self.production_service = ProductionService()
            self.andon_service = AndonService()
            self.notification_service = NotificationService()
            self.enhanced_notification_service = EnhancedNotificationService()
            self.production_statistics_service = ProductionStatisticsService()
            self.oee_calculator = OEECalculator()
            self.equipment_job_mapper = EquipmentJobMapper(self.production_service)
            self.downtime_tracker = PLCIntegratedDowntimeTracker()
            self.andon_service_plc = PLCIntegratedAndonService()
            
            # Initialize enhanced poller
            self.enhanced_poller = EnhancedTelemetryPoller()
            await self.enhanced_poller.initialize()
            
            logger.info("Real-time integration service initialized successfully")
            
        except Exception as e:
            logger.error("Failed to initialize real-time integration service", error=str(e))
            raise
    
    async def start(self):
        """Start the real-time integration service."""
        if self.is_running:
            logger.warning("Real-time integration service is already running")
            return
        
        try:
            self.is_running = True
            
            # Start background tasks
            self.background_tasks = [
                asyncio.create_task(self._production_event_processor()),
                asyncio.create_task(self._oee_update_processor()),
                asyncio.create_task(self._downtime_event_processor()),
                asyncio.create_task(self._andon_event_processor()),
                asyncio.create_task(self._job_progress_processor()),
                asyncio.create_task(self._quality_alert_processor()),
                asyncio.create_task(self._changeover_event_processor()),
                # Phase 3 Background Processors
                asyncio.create_task(self._production_statistics_processor()),
                asyncio.create_task(self._oee_analytics_processor()),
                asyncio.create_task(self._andon_analytics_processor()),
                asyncio.create_task(self._notification_processor()),
                asyncio.create_task(self._dashboard_update_processor())
            ]
            
            logger.info("Real-time integration service started")
            
        except Exception as e:
            logger.error("Failed to start real-time integration service", error=str(e))
            self.is_running = False
            raise
    
    async def stop(self):
        """Stop the real-time integration service."""
        if not self.is_running:
            return
        
        try:
            self.is_running = False
            
            # Cancel all background tasks
            for task in self.background_tasks:
                task.cancel()
            
            # Wait for tasks to complete
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
            
            self.background_tasks = []
            
            logger.info("Real-time integration service stopped")
            
        except Exception as e:
            logger.error("Error stopping real-time integration service", error=str(e))
    
    async def _production_event_processor(self):
        """Process production events and broadcast updates."""
        while self.is_running:
            try:
                # Get production updates from the enhanced poller
                if self.enhanced_poller and hasattr(self.enhanced_poller, 'get_production_updates'):
                    updates = await self.enhanced_poller.get_production_updates()
                    
                    for update in updates:
                        line_id = update.get("line_id")
                        if line_id:
                            await self.websocket_manager.broadcast_production_update(line_id, update)
                
                await asyncio.sleep(1)  # Process every second
                
            except Exception as e:
                logger.error("Error in production event processor", error=str(e))
                await asyncio.sleep(5)  # Wait before retrying
    
    async def _oee_update_processor(self):
        """Process OEE updates and broadcast them."""
        while self.is_running:
            try:
                # Get OEE updates from the OEE calculator
                if self.oee_calculator and hasattr(self.oee_calculator, 'get_latest_oee_updates'):
                    oee_updates = await self.oee_calculator.get_latest_oee_updates()
                    
                    for update in oee_updates:
                        line_id = update.get("line_id")
                        if line_id:
                            await self.websocket_manager.broadcast_oee_update(line_id, update)
                
                await asyncio.sleep(5)  # Process every 5 seconds
                
            except Exception as e:
                logger.error("Error in OEE update processor", error=str(e))
                await asyncio.sleep(10)  # Wait before retrying
    
    async def _downtime_event_processor(self):
        """Process downtime events and broadcast them."""
        while self.is_running:
            try:
                # Get downtime events from the downtime tracker
                if self.downtime_tracker and hasattr(self.downtime_tracker, 'get_latest_downtime_events'):
                    downtime_events = await self.downtime_tracker.get_latest_downtime_events()
                    
                    for event in downtime_events:
                        await self.websocket_manager.broadcast_downtime_event(event)
                
                await asyncio.sleep(2)  # Process every 2 seconds
                
            except Exception as e:
                logger.error("Error in downtime event processor", error=str(e))
                await asyncio.sleep(5)  # Wait before retrying
    
    async def _andon_event_processor(self):
        """Process Andon events and broadcast them."""
        while self.is_running:
            try:
                # Get Andon events from the Andon service
                if self.andon_service_plc and hasattr(self.andon_service_plc, 'get_latest_andon_events'):
                    andon_events = await self.andon_service_plc.get_latest_andon_events()
                    
                    for event in andon_events:
                        await self.websocket_manager.broadcast_andon_event(event)
                
                await asyncio.sleep(1)  # Process every second
                
            except Exception as e:
                logger.error("Error in Andon event processor", error=str(e))
                await asyncio.sleep(5)  # Wait before retrying
    
    async def _job_progress_processor(self):
        """Process job progress updates and broadcast them."""
        while self.is_running:
            try:
                # Get job progress updates from the job mapper
                if self.equipment_job_mapper and hasattr(self.equipment_job_mapper, 'get_job_progress_updates'):
                    job_updates = await self.equipment_job_mapper.get_job_progress_updates()
                    
                    for update in job_updates:
                        event_type = update.get("event_type")
                        job_data = update.get("job_data", {})
                        
                        if event_type == "job_assigned":
                            await self.websocket_manager.broadcast_job_assigned(job_data)
                        elif event_type == "job_started":
                            await self.websocket_manager.broadcast_job_started(job_data)
                        elif event_type == "job_completed":
                            await self.websocket_manager.broadcast_job_completed(job_data)
                        elif event_type == "job_cancelled":
                            await self.websocket_manager.broadcast_job_cancelled(job_data)
                
                await asyncio.sleep(3)  # Process every 3 seconds
                
            except Exception as e:
                logger.error("Error in job progress processor", error=str(e))
                await asyncio.sleep(5)  # Wait before retrying
    
    async def _quality_alert_processor(self):
        """Process quality alerts and broadcast them."""
        while self.is_running:
            try:
                # Get quality alerts from production service
                if self.production_service and hasattr(self.production_service, 'get_quality_alerts'):
                    quality_alerts = await self.production_service.get_quality_alerts()
                    
                    for alert in quality_alerts:
                        await self.websocket_manager.broadcast_quality_alert(alert)
                
                await asyncio.sleep(5)  # Process every 5 seconds
                
            except Exception as e:
                logger.error("Error in quality alert processor", error=str(e))
                await asyncio.sleep(10)  # Wait before retrying
    
    async def _changeover_event_processor(self):
        """Process changeover events and broadcast them."""
        while self.is_running:
            try:
                # Get changeover events from production service
                if self.production_service and hasattr(self.production_service, 'get_changeover_events'):
                    changeover_events = await self.production_service.get_changeover_events()
                    
                    for event in changeover_events:
                        event_type = event.get("event_type")
                        changeover_data = event.get("changeover_data", {})
                        
                        if event_type == "changeover_started":
                            await self.websocket_manager.broadcast_changeover_started(changeover_data)
                        elif event_type == "changeover_completed":
                            await self.websocket_manager.broadcast_changeover_completed(changeover_data)
                
                await asyncio.sleep(2)  # Process every 2 seconds
                
            except Exception as e:
                logger.error("Error in changeover event processor", error=str(e))
                await asyncio.sleep(5)  # Wait before retrying
    
    # Manual broadcasting methods for direct service integration
    async def broadcast_production_metrics(self, line_id: str, metrics: Dict[str, Any]):
        """Broadcast production metrics update."""
        await self.websocket_manager.broadcast_production_update(line_id, metrics)
    
    async def broadcast_job_event(self, event_type: str, job_data: Dict[str, Any]):
        """Broadcast job-related event."""
        if event_type == "job_assigned":
            await self.websocket_manager.broadcast_job_assigned(job_data)
        elif event_type == "job_started":
            await self.websocket_manager.broadcast_job_started(job_data)
        elif event_type == "job_completed":
            await self.websocket_manager.broadcast_job_completed(job_data)
        elif event_type == "job_cancelled":
            await self.websocket_manager.broadcast_job_cancelled(job_data)
        else:
            logger.warning(f"Unknown job event type: {event_type}")
    
    async def broadcast_oee_metrics(self, line_id: str, oee_data: Dict[str, Any]):
        """Broadcast OEE metrics update."""
        await self.websocket_manager.broadcast_oee_update(line_id, oee_data)
    
    async def broadcast_downtime_event(self, downtime_data: Dict[str, Any]):
        """Broadcast downtime event."""
        await self.websocket_manager.broadcast_downtime_event(downtime_data)
    
    async def broadcast_andon_event(self, andon_data: Dict[str, Any]):
        """Broadcast Andon event."""
        await self.websocket_manager.broadcast_andon_event(andon_data)
    
    async def broadcast_escalation_update(self, escalation_data: Dict[str, Any]):
        """Broadcast escalation update."""
        await self.websocket_manager.broadcast_escalation_update(escalation_data)
    
    async def broadcast_quality_alert(self, quality_data: Dict[str, Any]):
        """Broadcast quality alert."""
        await self.websocket_manager.broadcast_quality_alert(quality_data)
    
    async def broadcast_changeover_event(self, event_type: str, changeover_data: Dict[str, Any]):
        """Broadcast changeover event."""
        if event_type == "changeover_started":
            await self.websocket_manager.broadcast_changeover_started(changeover_data)
        elif event_type == "changeover_completed":
            await self.websocket_manager.broadcast_changeover_completed(changeover_data)
        else:
            logger.warning(f"Unknown changeover event type: {event_type}")
    
    # Integration with existing services
    async def integrate_with_enhanced_poller(self, poller: EnhancedTelemetryPoller):
        """Integrate with enhanced telemetry poller for real-time updates."""
        self.enhanced_poller = poller
        
        # Set up callbacks for real-time broadcasting
        if hasattr(poller, 'set_production_callback'):
            poller.set_production_callback(self.broadcast_production_metrics)
        
        if hasattr(poller, 'set_oee_callback'):
            poller.set_oee_callback(self.broadcast_oee_metrics)
        
        if hasattr(poller, 'set_downtime_callback'):
            poller.set_downtime_callback(self.broadcast_downtime_event)
        
        if hasattr(poller, 'set_andon_callback'):
            poller.set_andon_callback(self.broadcast_andon_event)
        
        logger.info("Integrated with enhanced telemetry poller")
    
    async def integrate_with_production_service(self, production_service: ProductionService):
        """Integrate with production service for real-time updates."""
        self.production_service = production_service
        
        # Set up callbacks for real-time broadcasting
        if hasattr(production_service, 'set_job_callback'):
            production_service.set_job_callback(self.broadcast_job_event)
        
        if hasattr(production_service, 'set_quality_callback'):
            production_service.set_quality_callback(self.broadcast_quality_alert)
        
        if hasattr(production_service, 'set_changeover_callback'):
            production_service.set_changeover_callback(self.broadcast_changeover_event)
        
        logger.info("Integrated with production service")
    
    async def integrate_with_andon_service(self, andon_service: AndonService):
        """Integrate with Andon service for real-time updates."""
        self.andon_service = andon_service
        
        # Set up callbacks for real-time broadcasting
        if hasattr(andon_service, 'set_andon_callback'):
            andon_service.set_andon_callback(self.broadcast_andon_event)
        
        if hasattr(andon_service, 'set_escalation_callback'):
            andon_service.set_escalation_callback(self.broadcast_escalation_update)
        
        logger.info("Integrated with Andon service")
    
    def get_status(self) -> Dict[str, Any]:
        """Get the status of the real-time integration service."""
        return {
            "is_running": self.is_running,
            "background_tasks": len(self.background_tasks),
            "active_connections": self.websocket_manager.get_connection_stats()["active_connections"],
            "services_initialized": {
                "production_service": self.production_service is not None,
                "andon_service": self.andon_service is not None,
                "notification_service": self.notification_service is not None,
                "enhanced_notification_service": self.enhanced_notification_service is not None,
                "production_statistics_service": self.production_statistics_service is not None,
                "oee_calculator": self.oee_calculator is not None,
                "equipment_job_mapper": self.equipment_job_mapper is not None,
                "downtime_tracker": self.downtime_tracker is not None,
                "andon_service_plc": self.andon_service_plc is not None,
                "enhanced_poller": self.enhanced_poller is not None
            }
        }
    
    # Phase 3 Implementation - Enhanced Background Processors
    
    async def _production_statistics_processor(self):
        """Process production statistics updates and broadcast them."""
        while self.is_running:
            try:
                # Get production statistics updates
                if self.production_statistics_service:
                    # This would typically get statistics updates from a queue or database trigger
                    # For now, we'll simulate periodic statistics updates
                    pass
                
                await asyncio.sleep(30)  # Process every 30 seconds
                
            except Exception as e:
                logger.error("Error in production statistics processor", error=str(e))
                await asyncio.sleep(10)  # Wait before retrying
    
    async def _oee_analytics_processor(self):
        """Process OEE analytics updates and broadcast them."""
        while self.is_running:
            try:
                # Get OEE analytics updates
                if self.oee_calculator:
                    # This would typically get analytics updates from calculation results
                    # For now, we'll simulate periodic analytics updates
                    pass
                
                await asyncio.sleep(60)  # Process every minute
                
            except Exception as e:
                logger.error("Error in OEE analytics processor", error=str(e))
                await asyncio.sleep(15)  # Wait before retrying
    
    async def _andon_analytics_processor(self):
        """Process Andon analytics updates and broadcast them."""
        while self.is_running:
            try:
                # Get Andon analytics updates
                if self.andon_service:
                    # This would typically get analytics updates from event processing
                    # For now, we'll simulate periodic analytics updates
                    pass
                
                await asyncio.sleep(45)  # Process every 45 seconds
                
            except Exception as e:
                logger.error("Error in Andon analytics processor", error=str(e))
                await asyncio.sleep(10)  # Wait before retrying
    
    async def _notification_processor(self):
        """Process notification events and broadcast them."""
        while self.is_running:
            try:
                # Get notification events
                if self.enhanced_notification_service:
                    # This would typically get notification events from a queue
                    # For now, we'll simulate periodic notification processing
                    pass
                
                await asyncio.sleep(10)  # Process every 10 seconds
                
            except Exception as e:
                logger.error("Error in notification processor", error=str(e))
                await asyncio.sleep(5)  # Wait before retrying
    
    async def _dashboard_update_processor(self):
        """Process dashboard updates and broadcast them."""
        while self.is_running:
            try:
                # Get dashboard updates from all services
                dashboard_updates = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "production_stats": {},
                    "oee_metrics": {},
                    "andon_events": {},
                    "notifications": {}
                }
                
                # Collect updates from all services
                if self.production_statistics_service:
                    dashboard_updates["production_stats"] = {
                        "status": "updated",
                        "last_update": datetime.utcnow().isoformat()
                    }
                
                if self.oee_calculator:
                    dashboard_updates["oee_metrics"] = {
                        "status": "updated",
                        "last_update": datetime.utcnow().isoformat()
                    }
                
                if self.andon_service:
                    dashboard_updates["andon_events"] = {
                        "status": "updated",
                        "last_update": datetime.utcnow().isoformat()
                    }
                
                # Broadcast dashboard updates to all connected clients
                await self.websocket_manager.broadcast_dashboard_update(dashboard_updates)
                
                await asyncio.sleep(15)  # Process every 15 seconds
                
            except Exception as e:
                logger.error("Error in dashboard update processor", error=str(e))
                await asyncio.sleep(5)  # Wait before retrying
    
    # Phase 3 Integration Methods
    
    async def integrate_with_enhanced_notification_service(self, notification_service: EnhancedNotificationService):
        """Integrate with enhanced notification service."""
        self.enhanced_notification_service = notification_service
        
        # Set up callbacks for real-time broadcasting
        if hasattr(notification_service, 'set_notification_callback'):
            notification_service.set_notification_callback(self.broadcast_notification_event)
        
        logger.info("Integrated with enhanced notification service")
    
    async def integrate_with_production_statistics_service(self, statistics_service: ProductionStatisticsService):
        """Integrate with production statistics service."""
        self.production_statistics_service = statistics_service
        
        # Set up callbacks for real-time broadcasting
        if hasattr(statistics_service, 'set_statistics_callback'):
            statistics_service.set_statistics_callback(self.broadcast_production_statistics)
        
        logger.info("Integrated with production statistics service")
    
    async def integrate_with_oee_calculator(self, oee_calculator: OEECalculator):
        """Integrate with OEE calculator service."""
        self.oee_calculator = oee_calculator
        
        # Set up callbacks for real-time broadcasting
        if hasattr(oee_calculator, 'set_analytics_callback'):
            oee_calculator.set_analytics_callback(self.broadcast_oee_analytics)
        
        logger.info("Integrated with OEE calculator service")
    
    # Phase 3 Broadcasting Methods
    
    async def broadcast_notification_event(self, notification_data: Dict[str, Any]):
        """Broadcast notification event."""
        await self.websocket_manager.broadcast_notification_event(notification_data)
    
    async def broadcast_production_statistics(self, statistics_data: Dict[str, Any]):
        """Broadcast production statistics update."""
        await self.websocket_manager.broadcast_production_statistics(statistics_data)
    
    async def broadcast_oee_analytics(self, analytics_data: Dict[str, Any]):
        """Broadcast OEE analytics update."""
        await self.websocket_manager.broadcast_oee_analytics(analytics_data)
    
    async def broadcast_andon_analytics(self, analytics_data: Dict[str, Any]):
        """Broadcast Andon analytics update."""
        await self.websocket_manager.broadcast_andon_analytics(analytics_data)
    
    async def broadcast_dashboard_update(self, dashboard_data: Dict[str, Any]):
        """Broadcast dashboard update."""
        await self.websocket_manager.broadcast_dashboard_update(dashboard_data)
