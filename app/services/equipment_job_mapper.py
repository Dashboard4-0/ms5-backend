"""
MS5.0 Floor Dashboard - Equipment Job Mapper

This module provides mapping between equipment and production jobs/schedules,
enabling the PLC integration to understand current production context and
automatically track job progress based on equipment metrics.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from uuid import UUID
import structlog

from app.services.production_service import ProductionLineService, ProductionScheduleService
from app.database import execute_query, execute_scalar, execute_update
from app.utils.exceptions import NotFoundError, BusinessLogicError

logger = structlog.get_logger()


class EquipmentJobMapper:
    """Map equipment to production jobs and schedules."""
    
    def __init__(self, production_service: ProductionLineService):
        self.production_service = production_service
        self.schedule_service = ProductionScheduleService()
        self.equipment_job_cache = {}
        self.cache_ttl = 300  # 5 minutes
    
    async def get_current_job(self, equipment_code: str) -> Optional[Dict]:
        """Get current job assignment for equipment."""
        try:
            # Check cache first
            cache_key = f"{equipment_code}_{datetime.now().strftime('%Y%m%d%H%M')}"
            if cache_key in self.equipment_job_cache:
                return self.equipment_job_cache[cache_key]
            
            # Get current job from context
            context_query = """
            SELECT 
                c.current_job_id,
                c.production_schedule_id,
                c.production_line_id,
                c.target_quantity,
                c.actual_quantity,
                c.target_speed,
                c.current_product_type_id,
                c.shift_id,
                c.production_efficiency,
                c.quality_rate,
                c.changeover_status,
                c.current_operator,
                c.current_shift
            FROM factory_telemetry.context c
            WHERE c.equipment_code = :equipment_code
            """
            
            result = await execute_query(context_query, {"equipment_code": equipment_code})
            
            if not result:
                return None
            
            context = result[0]
            current_job_id = context.get("current_job_id")
            
            if not current_job_id:
                return None
            
            # Get job details
            job_details = await self._get_job_details(current_job_id)
            if not job_details:
                return None
            
            # Get schedule details if available
            schedule_id = context.get("production_schedule_id")
            schedule_details = None
            if schedule_id:
                schedule_details = await self._get_schedule_details(schedule_id)
            
            # Get product type details
            product_type_id = context.get("current_product_type_id")
            product_details = None
            if product_type_id:
                product_details = await self._get_product_type_details(product_type_id)
            
            # Compile job information
            job_info = {
                "job_id": current_job_id,
                "schedule_id": schedule_id,
                "line_id": context.get("production_line_id"),
                "product_type_id": product_type_id,
                "target_quantity": context.get("target_quantity", 0),
                "actual_quantity": context.get("actual_quantity", 0),
                "target_speed": context.get("target_speed", 0.0),
                "production_efficiency": context.get("production_efficiency", 0.0),
                "quality_rate": context.get("quality_rate", 0.0),
                "changeover_status": context.get("changeover_status", "none"),
                "current_operator": context.get("current_operator"),
                "current_shift": context.get("current_shift"),
                "shift_id": context.get("shift_id"),
                "job_details": job_details,
                "schedule_details": schedule_details,
                "product_details": product_details,
                "progress_percentage": self._calculate_progress_percentage(
                    context.get("actual_quantity", 0),
                    context.get("target_quantity", 0)
                ),
                "estimated_completion": self._estimate_completion_time(
                    context.get("actual_quantity", 0),
                    context.get("target_quantity", 0),
                    context.get("target_speed", 0.0)
                )
            }
            
            # Cache the result
            self.equipment_job_cache[cache_key] = job_info
            
            return job_info
            
        except Exception as e:
            logger.error("Failed to get current job", error=str(e), equipment_code=equipment_code)
            return None
    
    async def update_job_progress(self, equipment_code: str, metrics: Dict) -> Optional[Dict]:
        """Update job progress based on equipment metrics."""
        try:
            # Get current job
            current_job = await self.get_current_job(equipment_code)
            if not current_job:
                return None
            
            # Extract progress metrics
            actual_quantity = metrics.get("product_count", 0)
            target_quantity = current_job.get("target_quantity", 0)
            production_efficiency = metrics.get("production_efficiency", 0.0)
            quality_rate = metrics.get("quality_rate", 0.0)
            
            # Update context with new metrics
            update_query = """
            UPDATE factory_telemetry.context 
            SET 
                actual_quantity = :actual_quantity,
                production_efficiency = :production_efficiency,
                quality_rate = :quality_rate,
                last_production_update = :last_update
            WHERE equipment_code = :equipment_code
            """
            
            await execute_update(update_query, {
                "equipment_code": equipment_code,
                "actual_quantity": actual_quantity,
                "production_efficiency": production_efficiency,
                "quality_rate": quality_rate,
                "last_update": datetime.utcnow()
            })
            
            # Check for job completion
            if target_quantity > 0 and actual_quantity >= target_quantity:
                await self._handle_job_completion(current_job, equipment_code, metrics)
            
            # Update progress information
            updated_job = current_job.copy()
            updated_job.update({
                "actual_quantity": actual_quantity,
                "production_efficiency": production_efficiency,
                "quality_rate": quality_rate,
                "progress_percentage": self._calculate_progress_percentage(actual_quantity, target_quantity),
                "estimated_completion": self._estimate_completion_time(
                    actual_quantity, target_quantity, current_job.get("target_speed", 0.0)
                ),
                "last_updated": datetime.utcnow().isoformat()
            })
            
            # Clear cache to force refresh
            self._clear_job_cache(equipment_code)
            
            logger.info(
                "Job progress updated",
                equipment_code=equipment_code,
                job_id=current_job.get("job_id"),
                actual_quantity=actual_quantity,
                target_quantity=target_quantity,
                progress_percentage=updated_job["progress_percentage"]
            )
            
            return updated_job
            
        except Exception as e:
            logger.error("Failed to update job progress", error=str(e), equipment_code=equipment_code)
            return None
    
    async def assign_job_to_equipment(
        self, 
        equipment_code: str, 
        job_id: UUID, 
        assigned_by: UUID,
        target_quantity: int = None,
        target_speed: float = None
    ) -> Dict:
        """Assign a job to equipment."""
        try:
            # Validate job exists
            job_details = await self._get_job_details(job_id)
            if not job_details:
                raise NotFoundError("Job assignment", str(job_id))
            
            # Get schedule details if available
            schedule_id = job_details.get("schedule_id")
            schedule_details = None
            if schedule_id:
                schedule_details = await self._get_schedule_details(schedule_id)
            
            # Update context with job assignment
            update_query = """
            UPDATE factory_telemetry.context 
            SET 
                current_job_id = :job_id,
                production_schedule_id = :schedule_id,
                production_line_id = :line_id,
                target_quantity = :target_quantity,
                target_speed = :target_speed,
                current_product_type_id = :product_type_id,
                actual_quantity = 0,
                production_efficiency = 0.0,
                quality_rate = 0.0,
                changeover_status = 'none',
                last_production_update = :last_update
            WHERE equipment_code = :equipment_code
            """
            
            await execute_update(update_query, {
                "equipment_code": equipment_code,
                "job_id": job_id,
                "schedule_id": schedule_id,
                "line_id": schedule_details.get("line_id") if schedule_details else None,
                "target_quantity": target_quantity or schedule_details.get("target_quantity") if schedule_details else 0,
                "target_speed": target_speed or 0.0,
                "product_type_id": schedule_details.get("product_type_id") if schedule_details else None,
                "last_update": datetime.utcnow()
            })
            
            # Clear cache
            self._clear_job_cache(equipment_code)
            
            # Log job assignment
            logger.info(
                "Job assigned to equipment",
                equipment_code=equipment_code,
                job_id=job_id,
                assigned_by=assigned_by,
                target_quantity=target_quantity,
                target_speed=target_speed
            )
            
            # Return updated job information
            return await self.get_current_job(equipment_code)
            
        except Exception as e:
            logger.error("Failed to assign job to equipment", error=str(e), equipment_code=equipment_code, job_id=job_id)
            raise BusinessLogicError("Failed to assign job to equipment")
    
    async def unassign_job_from_equipment(self, equipment_code: str, unassigned_by: UUID) -> bool:
        """Unassign current job from equipment."""
        try:
            # Get current job
            current_job = await self.get_current_job(equipment_code)
            if not current_job:
                return False
            
            # Update context to clear job assignment
            update_query = """
            UPDATE factory_telemetry.context 
            SET 
                current_job_id = NULL,
                production_schedule_id = NULL,
                target_quantity = 0,
                actual_quantity = 0,
                target_speed = 0.0,
                current_product_type_id = NULL,
                production_efficiency = 0.0,
                quality_rate = 0.0,
                changeover_status = 'none',
                last_production_update = :last_update
            WHERE equipment_code = :equipment_code
            """
            
            await execute_update(update_query, {
                "equipment_code": equipment_code,
                "last_update": datetime.utcnow()
            })
            
            # Clear cache
            self._clear_job_cache(equipment_code)
            
            logger.info(
                "Job unassigned from equipment",
                equipment_code=equipment_code,
                job_id=current_job.get("job_id"),
                unassigned_by=unassigned_by
            )
            
            return True
            
        except Exception as e:
            logger.error("Failed to unassign job from equipment", error=str(e), equipment_code=equipment_code)
            return False
    
    async def get_equipment_job_history(
        self, 
        equipment_code: str, 
        start_date: datetime = None,
        end_date: datetime = None,
        limit: int = 100
    ) -> List[Dict]:
        """Get job assignment history for equipment."""
        try:
            if not start_date:
                start_date = datetime.utcnow() - timedelta(days=30)
            if not end_date:
                end_date = datetime.utcnow()
            
            # Get job history from context history table
            history_query = """
            SELECT 
                pch.equipment_code,
                pch.context_data,
                pch.change_reason,
                pch.changed_by,
                pch.changed_at
            FROM factory_telemetry.production_context_history pch
            WHERE pch.equipment_code = :equipment_code
            AND pch.changed_at >= :start_date
            AND pch.changed_at <= :end_date
            ORDER BY pch.changed_at DESC
            LIMIT :limit
            """
            
            result = await execute_query(history_query, {
                "equipment_code": equipment_code,
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit
            })
            
            history = []
            for row in result:
                context_data = row["context_data"]
                history.append({
                    "equipment_code": row["equipment_code"],
                    "job_id": context_data.get("current_job_id"),
                    "schedule_id": context_data.get("production_schedule_id"),
                    "line_id": context_data.get("production_line_id"),
                    "target_quantity": context_data.get("target_quantity"),
                    "actual_quantity": context_data.get("actual_quantity"),
                    "target_speed": context_data.get("target_speed"),
                    "product_type_id": context_data.get("current_product_type_id"),
                    "changeover_status": context_data.get("changeover_status"),
                    "production_efficiency": context_data.get("production_efficiency"),
                    "quality_rate": context_data.get("quality_rate"),
                    "change_reason": row["change_reason"],
                    "changed_by": row["changed_by"],
                    "changed_at": row["changed_at"]
                })
            
            return history
            
        except Exception as e:
            logger.error("Failed to get equipment job history", error=str(e), equipment_code=equipment_code)
            return []
    
    async def get_equipment_production_summary(
        self, 
        equipment_code: str, 
        start_date: datetime = None,
        end_date: datetime = None
    ) -> Dict:
        """Get production summary for equipment over a period."""
        try:
            if not start_date:
                start_date = datetime.utcnow() - timedelta(days=7)
            if not end_date:
                end_date = datetime.utcnow()
            
            # Get production metrics from context history
            summary_query = """
            SELECT 
                COUNT(*) as total_jobs,
                AVG(CAST(context_data->>'actual_quantity' AS INTEGER)) as avg_actual_quantity,
                AVG(CAST(context_data->>'target_quantity' AS INTEGER)) as avg_target_quantity,
                AVG(CAST(context_data->>'production_efficiency' AS REAL)) as avg_production_efficiency,
                AVG(CAST(context_data->>'quality_rate' AS REAL)) as avg_quality_rate,
                MAX(CAST(context_data->>'actual_quantity' AS INTEGER)) as max_quantity_produced,
                MIN(CAST(context_data->>'actual_quantity' AS INTEGER)) as min_quantity_produced
            FROM factory_telemetry.production_context_history pch
            WHERE pch.equipment_code = :equipment_code
            AND pch.changed_at >= :start_date
            AND pch.changed_at <= :end_date
            AND context_data->>'current_job_id' IS NOT NULL
            """
            
            result = await execute_query(summary_query, {
                "equipment_code": equipment_code,
                "start_date": start_date,
                "end_date": end_date
            })
            
            if not result:
                return {}
            
            summary = result[0]
            
            return {
                "equipment_code": equipment_code,
                "period": {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat()
                },
                "total_jobs": summary.get("total_jobs", 0),
                "avg_actual_quantity": round(summary.get("avg_actual_quantity", 0), 2),
                "avg_target_quantity": round(summary.get("avg_target_quantity", 0), 2),
                "avg_production_efficiency": round(summary.get("avg_production_efficiency", 0), 2),
                "avg_quality_rate": round(summary.get("avg_quality_rate", 0), 2),
                "max_quantity_produced": summary.get("max_quantity_produced", 0),
                "min_quantity_produced": summary.get("min_quantity_produced", 0),
                "total_quantity_produced": summary.get("total_quantity_produced", 0),
                "performance_score": self._calculate_performance_score(summary)
            }
            
        except Exception as e:
            logger.error("Failed to get equipment production summary", error=str(e), equipment_code=equipment_code)
            return {}
    
    async def _get_job_details(self, job_id: UUID) -> Optional[Dict]:
        """Get job assignment details."""
        try:
            job_query = """
            SELECT 
                ja.id,
                ja.schedule_id,
                ja.operator_id,
                ja.assigned_at,
                ja.status,
                ja.notes,
                ja.priority,
                ja.estimated_duration_minutes,
                ja.actual_duration_minutes,
                ja.start_time,
                ja.end_time,
                ja.created_at,
                ja.updated_at
            FROM factory_telemetry.job_assignments ja
            WHERE ja.id = :job_id
            """
            
            result = await execute_query(job_query, {"job_id": job_id})
            
            if result:
                return result[0]
            
            return None
            
        except Exception as e:
            logger.error("Failed to get job details", error=str(e), job_id=job_id)
            return None
    
    async def _get_schedule_details(self, schedule_id: UUID) -> Optional[Dict]:
        """Get production schedule details."""
        try:
            schedule_query = """
            SELECT 
                ps.id,
                ps.line_id,
                ps.product_type_id,
                ps.scheduled_start,
                ps.scheduled_end,
                ps.target_quantity,
                ps.priority,
                ps.status,
                ps.notes,
                ps.created_by,
                ps.created_at,
                ps.updated_at
            FROM factory_telemetry.production_schedules ps
            WHERE ps.id = :schedule_id
            """
            
            result = await execute_query(schedule_query, {"schedule_id": schedule_id})
            
            if result:
                return result[0]
            
            return None
            
        except Exception as e:
            logger.error("Failed to get schedule details", error=str(e), schedule_id=schedule_id)
            return None
    
    async def _get_product_type_details(self, product_type_id: UUID) -> Optional[Dict]:
        """Get product type details."""
        try:
            product_query = """
            SELECT 
                pt.id,
                pt.name,
                pt.description,
                pt.product_code,
                pt.specifications,
                pt.enabled,
                pt.created_at,
                pt.updated_at
            FROM factory_telemetry.product_types pt
            WHERE pt.id = :product_type_id
            """
            
            result = await execute_query(product_query, {"product_type_id": product_type_id})
            
            if result:
                return result[0]
            
            return None
            
        except Exception as e:
            logger.error("Failed to get product type details", error=str(e), product_type_id=product_type_id)
            return None
    
    async def _handle_job_completion(self, job_info: Dict, equipment_code: str, metrics: Dict):
        """Handle job completion event."""
        try:
            job_id = job_info.get("job_id")
            actual_quantity = metrics.get("product_count", 0)
            target_quantity = job_info.get("target_quantity", 0)
            
            # Update job assignment status
            job_update_query = """
            UPDATE factory_telemetry.job_assignments 
            SET 
                status = 'completed',
                actual_duration_minutes = EXTRACT(EPOCH FROM (NOW() - start_time)) / 60,
                end_time = NOW(),
                updated_at = NOW()
            WHERE id = :job_id
            """
            
            await execute_update(job_update_query, {"job_id": job_id})
            
            # Log job completion
            logger.info(
                "Job completed",
                equipment_code=equipment_code,
                job_id=job_id,
                target_quantity=target_quantity,
                actual_quantity=actual_quantity,
                completion_percentage=(actual_quantity / target_quantity * 100) if target_quantity > 0 else 0
            )
            
            # Clear cache
            self._clear_job_cache(equipment_code)
            
        except Exception as e:
            logger.error("Failed to handle job completion", error=str(e), equipment_code=equipment_code)
    
    def _calculate_progress_percentage(self, actual: int, target: int) -> float:
        """Calculate job progress percentage."""
        if target <= 0:
            return 0.0
        
        return min(100.0, round((actual / target) * 100.0, 2))
    
    def _estimate_completion_time(
        self, 
        actual: int, 
        target: int, 
        target_speed: float
    ) -> Optional[datetime]:
        """Estimate job completion time."""
        if target <= 0 or actual >= target:
            return None
        
        if target_speed <= 0:
            return None
        
        remaining_quantity = target - actual
        estimated_seconds = remaining_quantity / target_speed
        
        return datetime.utcnow() + timedelta(seconds=estimated_seconds)
    
    def _calculate_performance_score(self, summary: Dict) -> float:
        """Calculate overall performance score."""
        try:
            efficiency = summary.get("avg_production_efficiency", 0)
            quality = summary.get("avg_quality_rate", 0)
            
            # Simple performance score calculation
            performance_score = (efficiency * 0.6) + (quality * 0.4)
            return round(performance_score, 2)
            
        except Exception:
            return 0.0
    
    def _clear_job_cache(self, equipment_code: str):
        """Clear job cache for equipment."""
        keys_to_remove = [key for key in self.equipment_job_cache.keys() if key.startswith(equipment_code)]
        for key in keys_to_remove:
            del self.equipment_job_cache[key]
