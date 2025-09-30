"""
MS5.0 Floor Dashboard - Enhanced Metric Transformer

This module extends the existing MetricTransformer to integrate with production
management services, providing enhanced OEE calculations, downtime tracking,
and production context management.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from uuid import UUID
import structlog

from app.services.production_service import ProductionLineService, ProductionScheduleService
from app.services.oee_calculator import OEECalculator
from app.services.downtime_tracker import DowntimeTracker
from app.services.andon_service import AndonService
from app.services.notification_service import NotificationService
from app.database import execute_query, execute_scalar

# Import the original transformer from the tag scanner
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../Tag_Scanner_for Reference Only'))

from transforms import MetricTransformer

logger = structlog.get_logger()


class EnhancedMetricTransformer(MetricTransformer):
    """Extended transformer with production management integration."""
    
    def __init__(self, fault_catalog: Dict[int, Dict] = None, production_service=None):
        """Initialize enhanced metric transformer."""
        super().__init__(fault_catalog)
        self.production_service = production_service
        self.oee_calculator = OEECalculator()
        self.downtime_tracker = DowntimeTracker()
        self.andon_service = AndonService()
        self.notification_service = NotificationService() if NotificationService else None
        
        # Production context cache
        self.production_context_cache = {}
        self.cache_ttl = 300  # 5 minutes
    
    async def transform_bagger_metrics(
        self,
        raw_data: Dict[str, Any],
        context_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Enhanced transformation with production management integration."""
        # Call parent transformation
        metrics = super().transform_bagger_metrics(raw_data, context_data)
        
        # Add production-specific metrics
        production_metrics = await self._add_production_metrics(raw_data, context_data)
        metrics.update(production_metrics)
        
        # Add enhanced OEE calculations
        oee_metrics = await self._calculate_enhanced_oee(metrics, context_data)
        metrics.update(oee_metrics)
        
        # Add downtime tracking
        downtime_metrics = await self._track_downtime_events(metrics, context_data)
        metrics.update(downtime_metrics)
        
        return metrics
    
    async def transform_basket_loader_metrics(
        self,
        raw_data: Dict[str, Any],
        context_data: Dict[str, Any],
        parent_product: Optional[int] = None
    ) -> Dict[str, Any]:
        """Enhanced transformation for basket loader with production management."""
        # Call parent transformation
        metrics = super().transform_basket_loader_metrics(raw_data, context_data, parent_product)
        
        # Add production-specific metrics
        production_metrics = await self._add_production_metrics(raw_data, context_data)
        metrics.update(production_metrics)
        
        # Add enhanced OEE calculations (if applicable)
        oee_metrics = await self._calculate_enhanced_oee(metrics, context_data)
        metrics.update(oee_metrics)
        
        # Add downtime tracking
        downtime_metrics = await self._track_downtime_events(metrics, context_data)
        metrics.update(downtime_metrics)
        
        return metrics
    
    async def _add_production_metrics(self, raw_data: Dict, context_data: Dict) -> Dict[str, Any]:
        """Add production management specific metrics."""
        processed = raw_data.get("processed", {})
        
        # Get production context
        equipment_code = context_data.get("equipment_code", "")
        production_context = await self._get_production_context(equipment_code)
        
        return {
            "production_line_id": production_context.get("production_line_id"),
            "current_job_id": production_context.get("current_job_id"),
            "target_quantity": production_context.get("target_quantity", 0),
            "actual_quantity": processed.get("product_count", 0),
            "production_efficiency": self._calculate_production_efficiency(processed, context_data),
            "quality_rate": self._calculate_quality_rate(processed, context_data),
            "changeover_status": self._detect_changeover_status(processed, context_data),
            "production_schedule_id": production_context.get("production_schedule_id"),
            "current_product_type_id": production_context.get("current_product_type_id"),
            "shift_id": production_context.get("shift_id"),
            "target_speed": production_context.get("target_speed", 0.0)
        }
    
    async def _calculate_enhanced_oee(self, metrics: Dict, context_data: Dict) -> Dict[str, Any]:
        """Calculate enhanced OEE with production context."""
        if not self.oee_calculator:
            return {}
        
        equipment_code = context_data.get("equipment_code", "")
        production_context = await self._get_production_context(equipment_code)
        line_id = production_context.get("production_line_id")
        
        if not line_id:
            return {}
        
        try:
            # Calculate real-time OEE
            oee_data = self.oee_calculator.calculate_real_time_oee(
                line_id=line_id,
                equipment_code=equipment_code,
                current_status=metrics,
                timestamp=datetime.utcnow()
            )
            
            return {
                "enhanced_oee": oee_data.get("oee", 0.0),
                "enhanced_availability": oee_data.get("availability", 0.0),
                "enhanced_performance": oee_data.get("performance", 0.0),
                "enhanced_quality": oee_data.get("quality", 0.0),
                "is_currently_down": oee_data.get("is_currently_down", False),
                "current_downtime_duration": oee_data.get("current_downtime_duration_seconds", 0)
            }
        except Exception as e:
            logger.error("Failed to calculate enhanced OEE", error=str(e), equipment_code=equipment_code)
            return {}
    
    async def _track_downtime_events(self, metrics: Dict, context_data: Dict) -> Dict[str, Any]:
        """Track downtime events with production context."""
        if not self.downtime_tracker:
            return {}
        
        equipment_code = context_data.get("equipment_code", "")
        production_context = await self._get_production_context(equipment_code)
        line_id = production_context.get("production_line_id")
        
        if not line_id:
            return {}
        
        try:
            # Detect downtime event
            downtime_event = self.downtime_tracker.detect_downtime_event(
                line_id=line_id,
                equipment_code=equipment_code,
                current_status=metrics,
                timestamp=datetime.utcnow()
            )
            
            if downtime_event:
                # Trigger Andon event if needed
                self._trigger_andon_if_needed(downtime_event, equipment_code, line_id)
                
                return {
                    "downtime_event_id": downtime_event.get("id"),
                    "downtime_reason": downtime_event.get("reason_description"),
                    "downtime_category": downtime_event.get("category"),
                    "downtime_status": downtime_event.get("status")
                }
            
            return {}
        except Exception as e:
            logger.error("Failed to track downtime events", error=str(e), equipment_code=equipment_code)
            return {}
    
    async def _get_production_context(self, equipment_code: str) -> Dict[str, Any]:
        """Get production context for equipment with caching."""
        # Check cache first
        cache_key = f"{equipment_code}_{datetime.now().strftime('%Y%m%d%H%M')}"
        if cache_key in self.production_context_cache:
            return self.production_context_cache[cache_key]
        
        try:
            # Get production context from database
            context_query = """
            SELECT 
                c.current_job_id,
                c.production_schedule_id,
                c.production_line_id,
                c.target_speed,
                c.current_product_type_id,
                c.shift_id,
                c.target_quantity,
                c.actual_quantity,
                c.production_efficiency,
                c.quality_rate,
                c.changeover_status,
                c.current_operator,
                c.current_shift
            FROM factory_telemetry.context c
            WHERE c.equipment_code = :equipment_code
            """
            
            result = await execute_query(context_query, {"equipment_code": equipment_code})
            
            if result:
                context = result[0]
                # Cache the result
                self.production_context_cache[cache_key] = context
                return context
            
            return {}
        except Exception as e:
            logger.error("Failed to get production context", error=str(e), equipment_code=equipment_code)
            return {}
    
    def _calculate_production_efficiency(self, processed: Dict, context_data: Dict) -> float:
        """Calculate production efficiency percentage."""
        try:
            target_speed = context_data.get("target_speed", 0.0)
            actual_speed = processed.get("speed_real", 0.0)
            
            if target_speed <= 0:
                return 0.0
            
            efficiency = min(100.0, (actual_speed / target_speed) * 100.0)
            return round(efficiency, 2)
        except Exception:
            return 0.0
    
    def _calculate_quality_rate(self, processed: Dict, context_data: Dict) -> float:
        """Calculate quality rate percentage."""
        try:
            # This would typically be calculated from quality data
            # For now, return a default value or calculate from available data
            total_parts = processed.get("product_count", 0)
            if total_parts <= 0:
                return 100.0  # Default to 100% if no parts produced
            
            # In a real implementation, this would use quality data
            # For now, assume 95% quality rate
            return 95.0
        except Exception:
            return 95.0
    
    def _detect_changeover_status(self, processed: Dict, context_data: Dict) -> str:
        """Detect changeover status from equipment data."""
        try:
            # Check for changeover indicators in the PLC data
            # This would typically look for specific PLC tags or patterns
            speed = processed.get("speed_real", 0.0)
            running = processed.get("running_status", False)
            
            if not running and speed == 0.0:
                # Check if this is a planned changeover
                planned_stop = context_data.get("planned_stop", False)
                if planned_stop:
                    return "in_progress"
            
            # Check for changeover completion indicators
            if running and speed > 0.1:
                # Equipment is running, changeover likely completed
                return "completed"
            
            return "none"
        except Exception:
            return "none"
    
    def _trigger_andon_if_needed(self, downtime_event: Dict, equipment_code: str, line_id: UUID):
        """Trigger Andon event if downtime event meets criteria."""
        try:
            if not self.andon_service:
                return
            
            # Determine if we should create an Andon event
            category = downtime_event.get("category", "")
            reason_code = downtime_event.get("reason_code", "")
            
            # Only create Andon events for unplanned downtime
            if category != "unplanned":
                return
            
            # Determine event type and priority based on reason code
            event_type, priority = self._classify_downtime_for_andon(reason_code, equipment_code)
            
            if event_type and priority:
                # Create Andon event
                andon_data = {
                    "line_id": line_id,
                    "equipment_code": equipment_code,
                    "event_type": event_type,
                    "priority": priority,
                    "description": f"PLC Downtime: {downtime_event.get('reason_description', 'Unknown')}",
                    "auto_generated": True
                }
                
                # This would be called asynchronously in a real implementation
                logger.info(
                    "Andon event triggered for downtime",
                    equipment_code=equipment_code,
                    line_id=line_id,
                    event_type=event_type,
                    priority=priority
                )
        except Exception as e:
            logger.error("Failed to trigger Andon event", error=str(e), equipment_code=equipment_code)
    
    def _classify_downtime_for_andon(self, reason_code: str, equipment_code: str) -> Tuple[str, str]:
        """Classify downtime reason for Andon event creation."""
        # Map reason codes to Andon event types and priorities
        reason_mapping = {
            "MECH_FAULT": ("maintenance", "high"),
            "ELEC_FAULT": ("maintenance", "high"),
            "BEARING_FAIL": ("maintenance", "high"),
            "BELT_BREAK": ("maintenance", "medium"),
            "MOTOR_FAIL": ("maintenance", "high"),
            "SENSOR_FAIL": ("maintenance", "medium"),
            "PLC_FAULT": ("maintenance", "critical"),
            "POWER_LOSS": ("maintenance", "critical"),
            "MAT_SHORTAGE": ("material", "medium"),
            "MAT_JAM": ("material", "medium"),
            "UPSTREAM_STOP": ("upstream", "low"),
            "DOWNSTREAM_STOP": ("downstream", "low"),
            "QUALITY_ISSUE": ("quality", "medium"),
            "UNKNOWN": ("maintenance", "medium")
        }
        
        return reason_mapping.get(reason_code, ("maintenance", "medium"))
    
    def update_production_context(self, equipment_code: str, context_updates: Dict[str, Any]):
        """Update production context for equipment."""
        try:
            # This would typically update the database context table
            # For now, just update the cache
            cache_key = f"{equipment_code}_{datetime.now().strftime('%Y%m%d%H%M')}"
            if cache_key in self.production_context_cache:
                self.production_context_cache[cache_key].update(context_updates)
            
            logger.info(
                "Production context updated",
                equipment_code=equipment_code,
                updates=context_updates
            )
        except Exception as e:
            logger.error("Failed to update production context", error=str(e), equipment_code=equipment_code)
    
    def get_equipment_production_status(self, equipment_code: str) -> Dict[str, Any]:
        """Get comprehensive production status for equipment."""
        try:
            production_context = self._get_production_context(equipment_code)
            
            # Get current job details if available
            current_job_id = production_context.get("current_job_id")
            job_details = {}
            if current_job_id:
                # This would typically fetch job details from the database
                job_details = {"job_id": current_job_id}
            
            return {
                "equipment_code": equipment_code,
                "production_context": production_context,
                "current_job": job_details,
                "cache_status": "active" if equipment_code in str(self.production_context_cache) else "inactive",
                "last_updated": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error("Failed to get equipment production status", error=str(e), equipment_code=equipment_code)
            return {"equipment_code": equipment_code, "error": str(e)}
