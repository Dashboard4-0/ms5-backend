"""
MS5.0 Floor Dashboard - Enhanced Telemetry Poller

This module extends the existing TelemetryPoller to integrate with production
management services, providing enhanced polling capabilities with production
context management, real-time OEE calculations, and automated workflow triggers.
"""

import asyncio
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any
from uuid import UUID
import structlog

from app.services.enhanced_metric_transformer import EnhancedMetricTransformer
from app.services.production_service import ProductionLineService, ProductionScheduleService
from app.services.oee_calculator import OEECalculator
from app.services.downtime_tracker import DowntimeTracker
from app.services.andon_service import AndonService
from app.services.notification_service import NotificationService
from app.database import execute_query, execute_scalar, execute_update

# Import the original poller from the tag scanner
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../Tag_Scanner_for Reference Only'))

from poller import TelemetryPoller

logger = structlog.get_logger()


class EnhancedTelemetryPoller(TelemetryPoller):
    """Enhanced poller with production management integration."""
    
    def __init__(self):
        """Initialize enhanced telemetry poller."""
        super().__init__()
        self.production_service = None
        self.andon_service = None
        self.notification_service = None
        self.production_context_manager = None
        
        # Enhanced services
        self.enhanced_oee_calculator = None
        self.enhanced_downtime_tracker = None
        
        # Production event processing
        self.production_events_queue = asyncio.Queue()
        self.andon_events_queue = asyncio.Queue()
        
        # Performance monitoring
        self.poll_cycle_times = []
        self.max_cycle_time_history = 100
    
    async def initialize(self) -> None:
        """Initialize with production services."""
        await super().initialize()
        
        try:
            # Initialize production services
            self.production_service = ProductionLineService()
            self.andon_service = AndonService()
            self.notification_service = NotificationService() if NotificationService else None
            
            # Initialize enhanced transformer
            self.transformer = EnhancedMetricTransformer(
                fault_catalog=self.fault_catalog,
                production_service=self.production_service
            )
            
            # Initialize enhanced services
            self.enhanced_oee_calculator = OEECalculator()
            self.enhanced_downtime_tracker = DowntimeTracker()
            
            # Initialize production context manager
            self.production_context_manager = ProductionContextManager(self.production_service)
            
            logger.info("Enhanced telemetry poller initialized with production services")
            
        except Exception as e:
            logger.error("Failed to initialize enhanced telemetry poller", error=str(e))
            raise
    
    async def run(self) -> None:
        """Run enhanced polling loop with production management."""
        self.running = True
        logger.info(
            "starting_enhanced_poll_loop",
            interval_s=1.0,  # 1Hz polling
        )
        
        # Start background task processors
        production_task = asyncio.create_task(self._process_production_events_worker())
        andon_task = asyncio.create_task(self._process_andon_events_worker())
        
        try:
            while self.running:
                cycle_start = time.time()
                
                try:
                    await self._enhanced_poll_cycle()
                except Exception as e:
                    logger.error("enhanced_poll_cycle_error", error=str(e))
                
                # Track cycle time for performance monitoring
                cycle_duration = time.time() - cycle_start
                self._track_cycle_time(cycle_duration)
                
                # Calculate sleep time to maintain 1Hz
                sleep_time = max(0, 1.0 - cycle_duration)
                
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                else:
                    logger.warning(
                        "enhanced_poll_cycle_slow",
                        duration=cycle_duration,
                        target=1.0,
                    )
            
        finally:
            # Cancel background tasks
            production_task.cancel()
            andon_task.cancel()
            
            try:
                await asyncio.gather(production_task, andon_task, return_exceptions=True)
            except Exception as e:
                logger.error("Error cancelling background tasks", error=str(e))
    
    async def _enhanced_poll_cycle(self) -> None:
        """Execute enhanced polling cycle with production management."""
        ts = datetime.utcnow()
        
        # Get database session
        with self._get_db_session() as session:
            # Get metric bindings
            bagger_bindings = self._get_metric_bindings(session, "BP01.PACK.BAG1")
            basket_bindings = self._get_metric_bindings(session, "BP01.PACK.BAG1.BL")
            
            # Get enhanced context data
            context_bagger = await self._get_enhanced_context(session, "BP01.PACK.BAG1")
            context_basket = await self._get_enhanced_context(session, "BP01.PACK.BAG1.BL")
            
            # Poll Bagger 1 with enhanced processing
            bagger_metrics = await self._enhanced_poll_bagger(context_bagger)
            if bagger_metrics:
                self.last_bagger_product = bagger_metrics.get("current_product")
                await self._store_enhanced_metrics(
                    session,
                    "BP01.PACK.BAG1",
                    bagger_metrics,
                    bagger_bindings,
                    ts
                )
                
                # Process production events
                await self._process_equipment_production_events("BP01.PACK.BAG1", bagger_metrics)
            
            # Poll Basket Loader 1 with enhanced processing
            basket_metrics = await self._enhanced_poll_basket_loader(
                context_basket,
                self.last_bagger_product
            )
            if basket_metrics:
                await self._store_enhanced_metrics(
                    session,
                    "BP01.PACK.BAG1.BL",
                    basket_metrics,
                    basket_bindings,
                    ts
                )
                
                # Process production events
                await self._process_equipment_production_events("BP01.PACK.BAG1.BL", basket_metrics)
            
            session.commit()
    
    async def _enhanced_poll_bagger(self, context_data: Dict) -> Optional[Dict]:
        """Enhanced Bagger 1 polling with production management."""
        try:
            # Read PLC tags
            raw_data = self.bagger_mapper.read_all_tags()
            
            # Transform to enhanced metrics
            metrics = await self.transformer.transform_bagger_metrics(raw_data, context_data)
            
            # Update production context
            await self._update_production_context("BP01.PACK.BAG1", metrics, context_data)
            
            # Detect fault edges
            fault_bits = raw_data["processed"].get("fault_bits", [False] * 64)
            edges = self.fault_detector.detect_edges("BP01.PACK.BAG1", fault_bits)
            
            # Process fault edges with enhanced handling
            if edges:
                await self._enhanced_process_fault_edges("BP01.PACK.BAG1", edges, metrics)
            
            return metrics
            
        except Exception as e:
            logger.error("enhanced_bagger_poll_failed", error=str(e))
            return None
    
    async def _enhanced_poll_basket_loader(
        self,
        context_data: Dict,
        parent_product: Optional[int]
    ) -> Optional[Dict]:
        """Enhanced Basket Loader 1 polling with production management."""
        try:
            # Read PLC tags
            raw_data = self.basket_loader_mapper.read_all_tags()
            
            # Transform to enhanced metrics
            metrics = await self.transformer.transform_basket_loader_metrics(
                raw_data,
                context_data,
                parent_product
            )
            
            # Update production context
            await self._update_production_context("BP01.PACK.BAG1.BL", metrics, context_data)
            
            return metrics
            
        except Exception as e:
            logger.error("enhanced_basket_loader_poll_failed", error=str(e))
            return None
    
    async def _get_enhanced_context(self, session, equipment_code: str) -> Dict:
        """Get enhanced context data including production information."""
        try:
            # Get basic context
            basic_context = self._get_context(session, equipment_code)
            
            # Get production context
            production_context = await self.production_context_manager.get_production_context(equipment_code)
            
            # Merge contexts
            enhanced_context = {
                **basic_context,
                **production_context,
                "equipment_code": equipment_code
            }
            
            return enhanced_context
            
        except Exception as e:
            logger.error("Failed to get enhanced context", error=str(e), equipment_code=equipment_code)
            return self._get_context(session, equipment_code)
    
    async def _update_production_context(self, equipment_code: str, metrics: Dict, context_data: Dict):
        """Update production context based on current metrics."""
        try:
            # Extract production metrics
            production_metrics = {
                "actual_quantity": metrics.get("product_count", 0),
                "production_efficiency": metrics.get("production_efficiency", 0.0),
                "quality_rate": metrics.get("quality_rate", 0.0),
                "changeover_status": metrics.get("changeover_status", "none"),
                "last_production_update": datetime.utcnow()
            }
            
            # Update context in database
            await self.production_context_manager.update_equipment_context(
                equipment_code, production_metrics
            )
            
        except Exception as e:
            logger.error("Failed to update production context", error=str(e), equipment_code=equipment_code)
    
    async def _store_enhanced_metrics(
        self,
        session,
        equipment_code: str,
        metrics: Dict,
        bindings: Dict,
        ts: datetime
    ) -> None:
        """Store enhanced metrics in database."""
        try:
            # Store basic metrics using parent method
            await self._store_metrics(session, equipment_code, metrics, bindings, ts)
            
            # Store enhanced metrics in production context
            enhanced_metrics = {
                "production_line_id": metrics.get("production_line_id"),
                "current_job_id": metrics.get("current_job_id"),
                "production_schedule_id": metrics.get("production_schedule_id"),
                "target_quantity": metrics.get("target_quantity"),
                "actual_quantity": metrics.get("actual_quantity"),
                "production_efficiency": metrics.get("production_efficiency"),
                "quality_rate": metrics.get("quality_rate"),
                "changeover_status": metrics.get("changeover_status"),
                "enhanced_oee": metrics.get("enhanced_oee"),
                "enhanced_availability": metrics.get("enhanced_availability"),
                "enhanced_performance": metrics.get("enhanced_performance"),
                "enhanced_quality": metrics.get("enhanced_quality"),
                "is_currently_down": metrics.get("is_currently_down"),
                "current_downtime_duration": metrics.get("current_downtime_duration"),
                "downtime_event_id": metrics.get("downtime_event_id"),
                "downtime_reason": metrics.get("downtime_reason"),
                "downtime_category": metrics.get("downtime_category"),
                "downtime_status": metrics.get("downtime_status")
            }
            
            # Update production context table
            await self._update_production_context_table(session, equipment_code, enhanced_metrics)
            
        except Exception as e:
            logger.error(
                "enhanced_metrics_storage_failed",
                equipment_code=equipment_code,
                error=str(e),
            )
            raise
    
    async def _update_production_context_table(self, session, equipment_code: str, metrics: Dict):
        """Update production context table with enhanced metrics."""
        try:
            update_query = """
            UPDATE factory_telemetry.context 
            SET 
                production_line_id = :production_line_id,
                current_job_id = :current_job_id,
                production_schedule_id = :production_schedule_id,
                target_quantity = :target_quantity,
                actual_quantity = :actual_quantity,
                production_efficiency = :production_efficiency,
                quality_rate = :quality_rate,
                changeover_status = :changeover_status,
                last_production_update = :last_production_update
            WHERE equipment_code = :equipment_code
            """
            
            await execute_update(update_query, {
                "equipment_code": equipment_code,
                "production_line_id": metrics.get("production_line_id"),
                "current_job_id": metrics.get("current_job_id"),
                "production_schedule_id": metrics.get("production_schedule_id"),
                "target_quantity": metrics.get("target_quantity"),
                "actual_quantity": metrics.get("actual_quantity"),
                "production_efficiency": metrics.get("production_efficiency"),
                "quality_rate": metrics.get("quality_rate"),
                "changeover_status": metrics.get("changeover_status"),
                "last_production_update": metrics.get("last_production_update")
            })
            
        except Exception as e:
            logger.error("Failed to update production context table", error=str(e))
    
    async def _enhanced_process_fault_edges(self, equipment_code: str, edges: List, metrics: Dict):
        """Enhanced fault edge processing with production context."""
        try:
            # Process basic fault edges
            await self._process_fault_edges(equipment_code, edges)
            
            # Enhanced processing for production events
            for edge in edges:
                if edge["edge_type"] == "rising" and edge["is_active"]:
                    # New fault detected - trigger production events
                    await self._handle_fault_detected(equipment_code, edge, metrics)
                elif edge["edge_type"] == "falling" and not edge["is_active"]:
                    # Fault cleared - trigger recovery events
                    await self._handle_fault_cleared(equipment_code, edge, metrics)
            
        except Exception as e:
            logger.error("Enhanced fault edge processing failed", error=str(e))
    
    async def _handle_fault_detected(self, equipment_code: str, edge: Dict, metrics: Dict):
        """Handle fault detection with production context."""
        try:
            # Add to production events queue
            production_event = {
                "type": "fault_detected",
                "equipment_code": equipment_code,
                "fault_bit": edge["bit_index"],
                "timestamp": edge["timestamp"],
                "metrics": metrics
            }
            
            await self.production_events_queue.put(production_event)
            
            logger.info(
                "Fault detected with production context",
                equipment_code=equipment_code,
                fault_bit=edge["bit_index"]
            )
            
        except Exception as e:
            logger.error("Failed to handle fault detection", error=str(e))
    
    async def _handle_fault_cleared(self, equipment_code: str, edge: Dict, metrics: Dict):
        """Handle fault clearing with production context."""
        try:
            # Add to production events queue
            production_event = {
                "type": "fault_cleared",
                "equipment_code": equipment_code,
                "fault_bit": edge["bit_index"],
                "timestamp": edge["timestamp"],
                "metrics": metrics
            }
            
            await self.production_events_queue.put(production_event)
            
            logger.info(
                "Fault cleared with production context",
                equipment_code=equipment_code,
                fault_bit=edge["bit_index"]
            )
            
        except Exception as e:
            logger.error("Failed to handle fault clearing", error=str(e))
    
    async def _process_equipment_production_events(self, equipment_code: str, metrics: Dict):
        """Process production events for equipment."""
        try:
            # Check for job completion
            await self._check_job_completion(equipment_code, metrics)
            
            # Check for quality issues
            await self._check_quality_issues(equipment_code, metrics)
            
            # Check for changeover events
            await self._check_changeover_events(equipment_code, metrics)
            
        except Exception as e:
            logger.error("Failed to process production events", error=str(e), equipment_code=equipment_code)
    
    async def _check_job_completion(self, equipment_code: str, metrics: Dict):
        """Check for job completion based on metrics."""
        try:
            target_quantity = metrics.get("target_quantity", 0)
            actual_quantity = metrics.get("actual_quantity", 0)
            
            if target_quantity > 0 and actual_quantity >= target_quantity:
                # Job completed
                completion_event = {
                    "type": "job_completed",
                    "equipment_code": equipment_code,
                    "target_quantity": target_quantity,
                    "actual_quantity": actual_quantity,
                    "timestamp": datetime.utcnow()
                }
                
                await self.production_events_queue.put(completion_event)
                
                logger.info(
                    "Job completed detected",
                    equipment_code=equipment_code,
                    target_quantity=target_quantity,
                    actual_quantity=actual_quantity
                )
                
        except Exception as e:
            logger.error("Failed to check job completion", error=str(e))
    
    async def _check_quality_issues(self, equipment_code: str, metrics: Dict):
        """Check for quality issues based on metrics."""
        try:
            quality_rate = metrics.get("quality_rate", 100.0)
            quality_threshold = 95.0  # Configurable threshold
            
            if quality_rate < quality_threshold:
                # Quality issue detected
                quality_event = {
                    "type": "quality_issue",
                    "equipment_code": equipment_code,
                    "quality_rate": quality_rate,
                    "threshold": quality_threshold,
                    "timestamp": datetime.utcnow()
                }
                
                await self.production_events_queue.put(quality_event)
                
                logger.warning(
                    "Quality issue detected",
                    equipment_code=equipment_code,
                    quality_rate=quality_rate,
                    threshold=quality_threshold
                )
                
        except Exception as e:
            logger.error("Failed to check quality issues", error=str(e))
    
    async def _check_changeover_events(self, equipment_code: str, metrics: Dict):
        """Check for changeover events based on metrics."""
        try:
            changeover_status = metrics.get("changeover_status", "none")
            current_job_id = metrics.get("current_job_id")
            
            if changeover_status == "in_progress" and current_job_id:
                # Changeover started
                changeover_event = {
                    "type": "changeover_started",
                    "equipment_code": equipment_code,
                    "job_id": current_job_id,
                    "timestamp": datetime.utcnow()
                }
                
                await self.production_events_queue.put(changeover_event)
                
            elif changeover_status == "completed" and current_job_id:
                # Changeover completed
                changeover_event = {
                    "type": "changeover_completed",
                    "equipment_code": equipment_code,
                    "job_id": current_job_id,
                    "timestamp": datetime.utcnow()
                }
                
                await self.production_events_queue.put(changeover_event)
                
        except Exception as e:
            logger.error("Failed to check changeover events", error=str(e))
    
    async def _process_production_events_worker(self):
        """Background worker for processing production events."""
        while self.running:
            try:
                # Wait for events with timeout
                event = await asyncio.wait_for(
                    self.production_events_queue.get(),
                    timeout=1.0
                )
                
                await self._process_production_event(event)
                
            except asyncio.TimeoutError:
                # No events, continue
                continue
            except Exception as e:
                logger.error("Error processing production event", error=str(e))
    
    async def _process_andon_events_worker(self):
        """Background worker for processing Andon events."""
        while self.running:
            try:
                # Wait for events with timeout
                event = await asyncio.wait_for(
                    self.andon_events_queue.get(),
                    timeout=1.0
                )
                
                await self._process_andon_event(event)
                
            except asyncio.TimeoutError:
                # No events, continue
                continue
            except Exception as e:
                logger.error("Error processing Andon event", error=str(e))
    
    async def _process_production_event(self, event: Dict):
        """Process a production event."""
        try:
            event_type = event.get("type")
            equipment_code = event.get("equipment_code")
            
            if event_type == "job_completed":
                await self._handle_job_completion(event)
            elif event_type == "quality_issue":
                await self._handle_quality_issue(event)
            elif event_type == "changeover_started":
                await self._handle_changeover_started(event)
            elif event_type == "changeover_completed":
                await self._handle_changeover_completed(event)
            elif event_type == "fault_detected":
                await self._handle_fault_detected_event(event)
            elif event_type == "fault_cleared":
                await self._handle_fault_cleared_event(event)
            
            logger.info("Production event processed", event_type=event_type, equipment_code=equipment_code)
            
        except Exception as e:
            logger.error("Failed to process production event", error=str(e), event=event)
    
    async def _process_andon_event(self, event: Dict):
        """Process an Andon event."""
        try:
            # Process Andon events (escalation, notifications, etc.)
            logger.info("Andon event processed", event=event)
            
        except Exception as e:
            logger.error("Failed to process Andon event", error=str(e), event=event)
    
    async def _handle_job_completion(self, event: Dict):
        """Handle job completion event."""
        try:
            equipment_code = event.get("equipment_code")
            target_quantity = event.get("target_quantity")
            actual_quantity = event.get("actual_quantity")
            
            if not equipment_code:
                return
            
            # Get production context to find current job
            production_context = await self.production_context_manager.get_production_context(equipment_code)
            current_job_id = production_context.get("current_job_id")
            
            if not current_job_id:
                logger.warning("No current job found for job completion", equipment_code=equipment_code)
                return
            
            # Update job status to completed
            if self.production_service:
                await self.production_service.complete_job_assignment(current_job_id, {
                    "actual_quantity": actual_quantity,
                    "completion_notes": f"Auto-completed: Target {target_quantity}, Actual {actual_quantity}",
                    "completed_at": datetime.utcnow()
                })
            
            # Clear production context
            await self.production_context_manager.update_equipment_context(equipment_code, {
                "current_job_id": None,
                "target_quantity": 0,
                "actual_quantity": 0,
                "production_efficiency": 0.0,
                "changeover_status": "none"
            })
            
            # Send notification
            if self.notification_service:
                await self.notification_service.send_push_notification(
                    user_id=production_context.get("current_operator", ""),
                    title="Job Completed",
                    body=f"Job {current_job_id} completed on {equipment_code}",
                    notification_type="job_completion"
                )
            
            logger.info(
                "Job completion handled",
                equipment_code=equipment_code,
                job_id=current_job_id,
                target_quantity=target_quantity,
                actual_quantity=actual_quantity
            )
            
        except Exception as e:
            logger.error("Failed to handle job completion", error=str(e), event=event)
    
    async def _handle_quality_issue(self, event: Dict):
        """Handle quality issue event."""
        try:
            equipment_code = event.get("equipment_code")
            quality_rate = event.get("quality_rate")
            threshold = event.get("threshold")
            
            if not equipment_code:
                return
            
            # Get production context
            production_context = await self.production_context_manager.get_production_context(equipment_code)
            line_id = production_context.get("production_line_id")
            
            # Create Andon event for quality issue
            if self.andon_service and line_id:
                andon_data = {
                    "line_id": line_id,
                    "equipment_code": equipment_code,
                    "event_type": "quality",
                    "priority": "medium",
                    "description": f"Quality rate {quality_rate:.1f}% below threshold {threshold:.1f}%",
                    "auto_generated": True
                }
                
                await self.andon_service.create_andon_event(andon_data)
            
            # Send notification to quality team
            if self.notification_service:
                await self.notification_service.send_push_notification(
                    user_id="quality_team",  # This would be a group or specific user
                    title="Quality Issue Detected",
                    body=f"Quality rate {quality_rate:.1f}% on {equipment_code}",
                    notification_type="quality_alert"
                )
            
            # Update production context
            await self.production_context_manager.update_equipment_context(equipment_code, {
                "quality_rate": quality_rate,
                "last_quality_issue": datetime.utcnow(),
                "quality_status": "below_threshold"
            })
            
            logger.warning(
                "Quality issue handled",
                equipment_code=equipment_code,
                quality_rate=quality_rate,
                threshold=threshold
            )
            
        except Exception as e:
            logger.error("Failed to handle quality issue", error=str(e), event=event)
    
    async def _handle_changeover_started(self, event: Dict):
        """Handle changeover started event."""
        try:
            equipment_code = event.get("equipment_code")
            job_id = event.get("job_id")
            
            if not equipment_code:
                return
            
            # Get production context
            production_context = await self.production_context_manager.get_production_context(equipment_code)
            line_id = production_context.get("production_line_id")
            
            # Update production context
            await self.production_context_manager.update_equipment_context(equipment_code, {
                "changeover_status": "in_progress",
                "changeover_started_at": datetime.utcnow(),
                "current_job_id": job_id
            })
            
            # Create Andon event for changeover
            if self.andon_service and line_id:
                andon_data = {
                    "line_id": line_id,
                    "equipment_code": equipment_code,
                    "event_type": "maintenance",
                    "priority": "low",
                    "description": f"Changeover started for job {job_id}",
                    "auto_generated": True
                }
                
                await self.andon_service.create_andon_event(andon_data)
            
            # Send notification
            if self.notification_service:
                await self.notification_service.send_push_notification(
                    user_id=production_context.get("current_operator", ""),
                    title="Changeover Started",
                    body=f"Changeover started on {equipment_code} for job {job_id}",
                    notification_type="changeover"
                )
            
            logger.info(
                "Changeover started handled",
                equipment_code=equipment_code,
                job_id=job_id
            )
            
        except Exception as e:
            logger.error("Failed to handle changeover started", error=str(e), event=event)
    
    async def _handle_changeover_completed(self, event: Dict):
        """Handle changeover completed event."""
        try:
            equipment_code = event.get("equipment_code")
            job_id = event.get("job_id")
            
            if not equipment_code:
                return
            
            # Get production context
            production_context = await self.production_context_manager.get_production_context(equipment_code)
            line_id = production_context.get("production_line_id")
            
            # Calculate changeover duration
            changeover_started_at = production_context.get("changeover_started_at")
            changeover_duration = None
            if changeover_started_at:
                changeover_duration = (datetime.utcnow() - changeover_started_at).total_seconds()
            
            # Update production context
            await self.production_context_manager.update_equipment_context(equipment_code, {
                "changeover_status": "completed",
                "changeover_completed_at": datetime.utcnow(),
                "changeover_duration": changeover_duration
            })
            
            # Create Andon event for changeover completion
            if self.andon_service and line_id:
                andon_data = {
                    "line_id": line_id,
                    "equipment_code": equipment_code,
                    "event_type": "maintenance",
                    "priority": "low",
                    "description": f"Changeover completed for job {job_id}",
                    "auto_generated": True
                }
                
                await self.andon_service.create_andon_event(andon_data)
            
            # Send notification
            if self.notification_service:
                duration_text = f" (Duration: {changeover_duration:.1f}s)" if changeover_duration else ""
                await self.notification_service.send_push_notification(
                    user_id=production_context.get("current_operator", ""),
                    title="Changeover Completed",
                    body=f"Changeover completed on {equipment_code} for job {job_id}{duration_text}",
                    notification_type="changeover"
                )
            
            logger.info(
                "Changeover completed handled",
                equipment_code=equipment_code,
                job_id=job_id,
                duration=changeover_duration
            )
            
        except Exception as e:
            logger.error("Failed to handle changeover completed", error=str(e), event=event)
    
    async def _handle_fault_detected_event(self, event: Dict):
        """Handle fault detected event."""
        try:
            equipment_code = event.get("equipment_code")
            fault_bit = event.get("fault_bit")
            metrics = event.get("metrics", {})
            
            if not equipment_code:
                return
            
            # Get production context
            production_context = await self.production_context_manager.get_production_context(equipment_code)
            line_id = production_context.get("production_line_id")
            
            # Get fault information from catalog
            fault_info = self.fault_catalog.get(equipment_code, {}).get(fault_bit, {})
            fault_name = fault_info.get("name", f"Fault {fault_bit}")
            fault_description = fault_info.get("description", "Unknown fault")
            
            # Create Andon event for fault
            if self.andon_service and line_id:
                andon_data = {
                    "line_id": line_id,
                    "equipment_code": equipment_code,
                    "event_type": "maintenance",
                    "priority": "high",
                    "description": f"Fault detected: {fault_name} - {fault_description}",
                    "auto_generated": True
                }
                
                await self.andon_service.create_andon_event(andon_data)
            
            # Send notification to maintenance team
            if self.notification_service:
                await self.notification_service.send_push_notification(
                    user_id="maintenance_team",  # This would be a group or specific user
                    title="Fault Detected",
                    body=f"Fault {fault_name} detected on {equipment_code}",
                    notification_type="fault_alert"
                )
            
            # Update production context
            await self.production_context_manager.update_equipment_context(equipment_code, {
                "fault_status": "active",
                "active_fault_bit": fault_bit,
                "fault_name": fault_name,
                "fault_detected_at": datetime.utcnow()
            })
            
            logger.warning(
                "Fault detected handled",
                equipment_code=equipment_code,
                fault_bit=fault_bit,
                fault_name=fault_name
            )
            
        except Exception as e:
            logger.error("Failed to handle fault detected event", error=str(e), event=event)
    
    async def _handle_fault_cleared_event(self, event: Dict):
        """Handle fault cleared event."""
        try:
            equipment_code = event.get("equipment_code")
            fault_bit = event.get("fault_bit")
            metrics = event.get("metrics", {})
            
            if not equipment_code:
                return
            
            # Get production context
            production_context = await self.production_context_manager.get_production_context(equipment_code)
            line_id = production_context.get("production_line_id")
            
            # Get fault information from catalog
            fault_info = self.fault_catalog.get(equipment_code, {}).get(fault_bit, {})
            fault_name = fault_info.get("name", f"Fault {fault_bit}")
            
            # Calculate fault duration
            fault_detected_at = production_context.get("fault_detected_at")
            fault_duration = None
            if fault_detected_at:
                fault_duration = (datetime.utcnow() - fault_detected_at).total_seconds()
            
            # Create Andon event for fault cleared
            if self.andon_service and line_id:
                andon_data = {
                    "line_id": line_id,
                    "equipment_code": equipment_code,
                    "event_type": "maintenance",
                    "priority": "low",
                    "description": f"Fault cleared: {fault_name}",
                    "auto_generated": True
                }
                
                await self.andon_service.create_andon_event(andon_data)
            
            # Send notification
            if self.notification_service:
                duration_text = f" (Duration: {fault_duration:.1f}s)" if fault_duration else ""
                await self.notification_service.send_push_notification(
                    user_id="maintenance_team",
                    title="Fault Cleared",
                    body=f"Fault {fault_name} cleared on {equipment_code}{duration_text}",
                    notification_type="fault_cleared"
                )
            
            # Update production context
            await self.production_context_manager.update_equipment_context(equipment_code, {
                "fault_status": "cleared",
                "active_fault_bit": None,
                "fault_name": None,
                "fault_cleared_at": datetime.utcnow(),
                "fault_duration": fault_duration
            })
            
            logger.info(
                "Fault cleared handled",
                equipment_code=equipment_code,
                fault_bit=fault_bit,
                fault_name=fault_name,
                duration=fault_duration
            )
            
        except Exception as e:
            logger.error("Failed to handle fault cleared event", error=str(e), event=event)
    
    def _track_cycle_time(self, cycle_duration: float):
        """Track poll cycle times for performance monitoring."""
        self.poll_cycle_times.append(cycle_duration)
        
        # Keep only recent cycle times
        if len(self.poll_cycle_times) > self.max_cycle_time_history:
            self.poll_cycle_times.pop(0)
        
        # Log performance warnings
        if cycle_duration > 0.8:  # 80% of 1Hz target
            logger.warning(
                "Poll cycle performance warning",
                duration=cycle_duration,
                avg_duration=sum(self.poll_cycle_times) / len(self.poll_cycle_times)
            )
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get polling performance statistics."""
        if not self.poll_cycle_times:
            return {"error": "No cycle time data available"}
        
        return {
            "total_cycles": len(self.poll_cycle_times),
            "avg_cycle_time": round(sum(self.poll_cycle_times) / len(self.poll_cycle_times), 3),
            "min_cycle_time": round(min(self.poll_cycle_times), 3),
            "max_cycle_time": round(max(self.poll_cycle_times), 3),
            "current_cycle_time": round(self.poll_cycle_times[-1], 3) if self.poll_cycle_times else 0,
            "performance_status": "good" if self.poll_cycle_times[-1] < 0.8 else "degraded" if self.poll_cycle_times[-1] < 1.0 else "poor"
        }
    
    async def shutdown(self) -> None:
        """Gracefully shutdown the enhanced poller."""
        logger.info("shutting_down_enhanced_poller")
        self.running = False
        
        # Call parent shutdown
        await super().shutdown()
        
        # Cleanup enhanced resources
        if self.production_context_manager:
            # Cleanup production context manager
            pass
        
        logger.info("enhanced_poller_shutdown_complete")


class ProductionContextManager:
    """Manage production context data for PLC integration."""
    
    def __init__(self, production_service: ProductionLineService):
        self.production_service = production_service
        self.context_cache = {}
        self.cache_ttl = 300  # 5 minutes
    
    async def get_production_context(self, equipment_code: str) -> Dict:
        """Get production context for equipment."""
        try:
            # Check cache first
            cache_key = f"{equipment_code}_{datetime.now().strftime('%Y%m%d%H%M')}"
            if cache_key in self.context_cache:
                return self.context_cache[cache_key]
            
            # Get from database
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
                self.context_cache[cache_key] = context
                return context
            
            return {}
            
        except Exception as e:
            logger.error("Failed to get production context", error=str(e), equipment_code=equipment_code)
            return {}
    
    async def update_equipment_context(self, equipment_code: str, context_data: Dict):
        """Update equipment context with production information."""
        try:
            # Update context table
            context_update = {
                "equipment_code": equipment_code,
                **context_data,
                "updated_at": datetime.utcnow()
            }
            
            # This would typically call a database function
            logger.info("Production context updated", equipment_code=equipment_code, updates=context_data)
            
        except Exception as e:
            logger.error("Failed to update equipment context", error=str(e), equipment_code=equipment_code)
