"""
MS5.0 Floor Dashboard - Production Service

This module contains the business logic for production management operations
including production lines, schedules, and job assignments.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from uuid import UUID
import structlog

from app.database import execute_query, execute_scalar, execute_update, get_db_session
from app.models.production import (
    ProductionLineCreate, ProductionLineUpdate, ProductionLineResponse,
    ProductionScheduleCreate, ProductionScheduleUpdate, ProductionScheduleResponse,
    JobAssignmentCreate, JobAssignmentUpdate, JobAssignmentResponse,
    ScheduleStatus, JobStatus, ProductionLineStatus
)
from app.utils.exceptions import (
    NotFoundError, ValidationError, ConflictError, BusinessLogicError
)

logger = structlog.get_logger()


class ProductionLineService:
    """Service for production line management."""
    
    @staticmethod
    async def create_production_line(line_data: ProductionLineCreate) -> ProductionLineResponse:
        """Create a new production line."""
        try:
            # Check if line code already exists
            existing_query = """
            SELECT id FROM factory_telemetry.production_lines 
            WHERE line_code = :line_code
            """
            existing = await execute_scalar(existing_query, {"line_code": line_data.line_code})
            
            if existing:
                raise ConflictError("Production line with this code already exists")
            
            # Create production line
            create_query = """
            INSERT INTO factory_telemetry.production_lines 
            (line_code, name, description, equipment_codes, target_speed, enabled)
            VALUES (:line_code, :name, :description, :equipment_codes, :target_speed, :enabled)
            RETURNING id, line_code, name, description, equipment_codes, target_speed, 
                     enabled, created_at, updated_at
            """
            
            result = await execute_query(create_query, {
                "line_code": line_data.line_code,
                "name": line_data.name,
                "description": line_data.description,
                "equipment_codes": line_data.equipment_codes,
                "target_speed": line_data.target_speed,
                "enabled": line_data.enabled
            })
            
            if not result:
                raise BusinessLogicError("Failed to create production line")
            
            line = result[0]
            
            logger.info("Production line created", line_id=line["id"], line_code=line["line_code"])
            
            return ProductionLineResponse(
                id=line["id"],
                line_code=line["line_code"],
                name=line["name"],
                description=line["description"],
                equipment_codes=line["equipment_codes"],
                target_speed=line["target_speed"],
                enabled=line["enabled"],
                status=ProductionLineStatus.IDLE,  # Default status
                created_at=line["created_at"],
                updated_at=line["updated_at"]
            )
            
        except (ConflictError, BusinessLogicError):
            raise
        except Exception as e:
            logger.error("Failed to create production line", error=str(e), line_code=line_data.line_code)
            raise BusinessLogicError("Failed to create production line")
    
    @staticmethod
    async def get_production_line(line_id: UUID) -> ProductionLineResponse:
        """Get a production line by ID."""
        try:
            query = """
            SELECT id, line_code, name, description, equipment_codes, target_speed, 
                   enabled, created_at, updated_at
            FROM factory_telemetry.production_lines 
            WHERE id = :line_id
            """
            
            result = await execute_query(query, {"line_id": line_id})
            
            if not result:
                raise NotFoundError("Production line", str(line_id))
            
            line = result[0]
            
            # Get current status (this would be determined by equipment status)
            status = await ProductionLineService._get_line_status(line_id)
            
            return ProductionLineResponse(
                id=line["id"],
                line_code=line["line_code"],
                name=line["name"],
                description=line["description"],
                equipment_codes=line["equipment_codes"],
                target_speed=line["target_speed"],
                enabled=line["enabled"],
                status=status,
                created_at=line["created_at"],
                updated_at=line["updated_at"]
            )
            
        except NotFoundError:
            raise
        except Exception as e:
            logger.error("Failed to get production line", error=str(e), line_id=line_id)
            raise BusinessLogicError("Failed to get production line")
    
    @staticmethod
    async def update_production_line(line_id: UUID, update_data: ProductionLineUpdate) -> ProductionLineResponse:
        """Update a production line."""
        try:
            # Check if line exists
            existing = await ProductionLineService.get_production_line(line_id)
            
            # Build update query dynamically
            update_fields = []
            update_values = {"line_id": line_id}
            
            if update_data.name is not None:
                update_fields.append("name = :name")
                update_values["name"] = update_data.name
            
            if update_data.description is not None:
                update_fields.append("description = :description")
                update_values["description"] = update_data.description
            
            if update_data.equipment_codes is not None:
                update_fields.append("equipment_codes = :equipment_codes")
                update_values["equipment_codes"] = update_data.equipment_codes
            
            if update_data.target_speed is not None:
                update_fields.append("target_speed = :target_speed")
                update_values["target_speed"] = update_data.target_speed
            
            if update_data.enabled is not None:
                update_fields.append("enabled = :enabled")
                update_values["enabled"] = update_data.enabled
            
            if not update_fields:
                return existing
            
            update_query = f"""
            UPDATE factory_telemetry.production_lines 
            SET {', '.join(update_fields)}, updated_at = NOW()
            WHERE id = :line_id
            """
            
            await execute_update(update_query, update_values)
            
            logger.info("Production line updated", line_id=line_id)
            
            # Return updated line
            return await ProductionLineService.get_production_line(line_id)
            
        except NotFoundError:
            raise
        except Exception as e:
            logger.error("Failed to update production line", error=str(e), line_id=line_id)
            raise BusinessLogicError("Failed to update production line")
    
    @staticmethod
    async def delete_production_line(line_id: UUID) -> bool:
        """Delete a production line."""
        try:
            # Check if line exists
            await ProductionLineService.get_production_line(line_id)
            
            # Check if line has active schedules
            active_schedules_query = """
            SELECT COUNT(*) FROM factory_telemetry.production_schedules 
            WHERE line_id = :line_id AND status IN ('scheduled', 'in_progress')
            """
            
            active_count = await execute_scalar(active_schedules_query, {"line_id": line_id})
            
            if active_count > 0:
                raise BusinessLogicError("Cannot delete production line with active schedules")
            
            # Delete production line
            delete_query = """
            DELETE FROM factory_telemetry.production_lines 
            WHERE id = :line_id
            """
            
            deleted_rows = await execute_update(delete_query, {"line_id": line_id})
            
            if deleted_rows == 0:
                raise BusinessLogicError("Failed to delete production line")
            
            logger.info("Production line deleted", line_id=line_id)
            return True
            
        except (NotFoundError, BusinessLogicError):
            raise
        except Exception as e:
            logger.error("Failed to delete production line", error=str(e), line_id=line_id)
            raise BusinessLogicError("Failed to delete production line")
    
    @staticmethod
    async def list_production_lines(
        skip: int = 0, 
        limit: int = 100, 
        enabled_only: bool = True
    ) -> List[ProductionLineResponse]:
        """List production lines with pagination."""
        try:
            where_clause = "WHERE enabled = true" if enabled_only else ""
            
            query = f"""
            SELECT id, line_code, name, description, equipment_codes, target_speed, 
                   enabled, created_at, updated_at
            FROM factory_telemetry.production_lines 
            {where_clause}
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :skip
            """
            
            result = await execute_query(query, {"skip": skip, "limit": limit})
            
            lines = []
            for line in result:
                status = await ProductionLineService._get_line_status(line["id"])
                
                lines.append(ProductionLineResponse(
                    id=line["id"],
                    line_code=line["line_code"],
                    name=line["name"],
                    description=line["description"],
                    equipment_codes=line["equipment_codes"],
                    target_speed=line["target_speed"],
                    enabled=line["enabled"],
                    status=status,
                    created_at=line["created_at"],
                    updated_at=line["updated_at"]
                ))
            
            return lines
            
        except Exception as e:
            logger.error("Failed to list production lines", error=str(e))
            raise BusinessLogicError("Failed to list production lines")
    
    @staticmethod
    async def _get_line_status(line_id: UUID) -> ProductionLineStatus:
        """Get current status of a production line based on equipment status."""
        try:
            # This would typically check equipment status from PLC data
            # For now, return a default status
            return ProductionLineStatus.IDLE
        except Exception:
            return ProductionLineStatus.IDLE


class ProductionScheduleService:
    """Service for production schedule management."""
    
    @staticmethod
    async def create_schedule(schedule_data: ProductionScheduleCreate, user_id: UUID) -> ProductionScheduleResponse:
        """Create a new production schedule."""
        try:
            # Validate line exists
            line_query = """
            SELECT id FROM factory_telemetry.production_lines 
            WHERE id = :line_id AND enabled = true
            """
            line_exists = await execute_scalar(line_query, {"line_id": schedule_data.line_id})
            
            if not line_exists:
                raise NotFoundError("Production line", str(schedule_data.line_id))
            
            # Validate product type exists
            product_query = """
            SELECT id FROM factory_telemetry.product_types 
            WHERE id = :product_type_id AND enabled = true
            """
            product_exists = await execute_scalar(product_query, {"product_type_id": schedule_data.product_type_id})
            
            if not product_exists:
                raise NotFoundError("Product type", str(schedule_data.product_type_id))
            
            # Check for schedule conflicts
            conflict_query = """
            SELECT id FROM factory_telemetry.production_schedules 
            WHERE line_id = :line_id 
            AND status IN ('scheduled', 'in_progress')
            AND (
                (scheduled_start <= :start_time AND scheduled_end > :start_time) OR
                (scheduled_start < :end_time AND scheduled_end >= :end_time) OR
                (scheduled_start >= :start_time AND scheduled_end <= :end_time)
            )
            """
            
            conflict = await execute_scalar(conflict_query, {
                "line_id": schedule_data.line_id,
                "start_time": schedule_data.scheduled_start,
                "end_time": schedule_data.scheduled_end
            })
            
            if conflict:
                raise ConflictError("Schedule conflicts with existing schedule")
            
            # Create schedule
            create_query = """
            INSERT INTO factory_telemetry.production_schedules 
            (line_id, product_type_id, scheduled_start, scheduled_end, target_quantity, 
             priority, status, created_by, notes)
            VALUES (:line_id, :product_type_id, :scheduled_start, :scheduled_end, 
                   :target_quantity, :priority, :status, :created_by, :notes)
            RETURNING id, line_id, product_type_id, scheduled_start, scheduled_end, 
                     target_quantity, priority, status, created_by, notes, 
                     created_at, updated_at
            """
            
            result = await execute_query(create_query, {
                "line_id": schedule_data.line_id,
                "product_type_id": schedule_data.product_type_id,
                "scheduled_start": schedule_data.scheduled_start,
                "scheduled_end": schedule_data.scheduled_end,
                "target_quantity": schedule_data.target_quantity,
                "priority": schedule_data.priority,
                "status": ScheduleStatus.SCHEDULED,
                "created_by": user_id,
                "notes": schedule_data.notes
            })
            
            if not result:
                raise BusinessLogicError("Failed to create production schedule")
            
            schedule = result[0]
            
            logger.info("Production schedule created", schedule_id=schedule["id"], line_id=schedule_data.line_id)
            
            return ProductionScheduleResponse(
                id=schedule["id"],
                line_id=schedule["line_id"],
                product_type_id=schedule["product_type_id"],
                scheduled_start=schedule["scheduled_start"],
                scheduled_end=schedule["scheduled_end"],
                target_quantity=schedule["target_quantity"],
                priority=schedule["priority"],
                status=ScheduleStatus(schedule["status"]),
                notes=schedule["notes"],
                created_by=schedule["created_by"],
                created_at=schedule["created_at"],
                updated_at=schedule["updated_at"]
            )
            
        except (NotFoundError, ConflictError, BusinessLogicError):
            raise
        except Exception as e:
            logger.error("Failed to create production schedule", error=str(e), line_id=schedule_data.line_id)
            raise BusinessLogicError("Failed to create production schedule")
    
    @staticmethod
    async def get_schedule(schedule_id: UUID) -> ProductionScheduleResponse:
        """Get a production schedule by ID."""
        try:
            query = """
            SELECT id, line_id, product_type_id, scheduled_start, scheduled_end, 
                   target_quantity, priority, status, created_by, notes, 
                   created_at, updated_at
            FROM factory_telemetry.production_schedules 
            WHERE id = :schedule_id
            """
            
            result = await execute_query(query, {"schedule_id": schedule_id})
            
            if not result:
                raise NotFoundError("Production schedule", str(schedule_id))
            
            schedule = result[0]
            
            return ProductionScheduleResponse(
                id=schedule["id"],
                line_id=schedule["line_id"],
                product_type_id=schedule["product_type_id"],
                scheduled_start=schedule["scheduled_start"],
                scheduled_end=schedule["scheduled_end"],
                target_quantity=schedule["target_quantity"],
                priority=schedule["priority"],
                status=ScheduleStatus(schedule["status"]),
                notes=schedule["notes"],
                created_by=schedule["created_by"],
                created_at=schedule["created_at"],
                updated_at=schedule["updated_at"]
            )
            
        except NotFoundError:
            raise
        except Exception as e:
            logger.error("Failed to get production schedule", error=str(e), schedule_id=schedule_id)
            raise BusinessLogicError("Failed to get production schedule")
    
    @staticmethod
    async def update_schedule(schedule_id: UUID, update_data: ProductionScheduleUpdate) -> ProductionScheduleResponse:
        """Update a production schedule."""
        try:
            # Check if schedule exists
            existing = await ProductionScheduleService.get_schedule(schedule_id)
            
            # Build update query dynamically
            update_fields = []
            update_values = {"schedule_id": schedule_id}
            
            if update_data.product_type_id is not None:
                update_fields.append("product_type_id = :product_type_id")
                update_values["product_type_id"] = update_data.product_type_id
            
            if update_data.scheduled_start is not None:
                update_fields.append("scheduled_start = :scheduled_start")
                update_values["scheduled_start"] = update_data.scheduled_start
            
            if update_data.scheduled_end is not None:
                update_fields.append("scheduled_end = :scheduled_end")
                update_values["scheduled_end"] = update_data.scheduled_end
            
            if update_data.target_quantity is not None:
                update_fields.append("target_quantity = :target_quantity")
                update_values["target_quantity"] = update_data.target_quantity
            
            if update_data.priority is not None:
                update_fields.append("priority = :priority")
                update_values["priority"] = update_data.priority
            
            if update_data.status is not None:
                update_fields.append("status = :status")
                update_values["status"] = update_data.status.value
            
            if update_data.notes is not None:
                update_fields.append("notes = :notes")
                update_values["notes"] = update_data.notes
            
            if not update_fields:
                return existing
            
            update_query = f"""
            UPDATE factory_telemetry.production_schedules 
            SET {', '.join(update_fields)}, updated_at = NOW()
            WHERE id = :schedule_id
            """
            
            await execute_update(update_query, update_values)
            
            logger.info("Production schedule updated", schedule_id=schedule_id)
            
            # Return updated schedule
            return await ProductionScheduleService.get_schedule(schedule_id)
            
        except NotFoundError:
            raise
        except Exception as e:
            logger.error("Failed to update production schedule", error=str(e), schedule_id=schedule_id)
            raise BusinessLogicError("Failed to update production schedule")
    
    @staticmethod
    async def list_schedules(
        line_id: Optional[UUID] = None,
        status: Optional[ScheduleStatus] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[ProductionScheduleResponse]:
        """List production schedules with filters."""
        try:
            where_conditions = []
            query_params = {"skip": skip, "limit": limit}
            
            if line_id:
                where_conditions.append("line_id = :line_id")
                query_params["line_id"] = line_id
            
            if status:
                where_conditions.append("status = :status")
                query_params["status"] = status.value
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"""
            SELECT id, line_id, product_type_id, scheduled_start, scheduled_end, 
                   target_quantity, priority, status, created_by, notes, 
                   created_at, updated_at
            FROM factory_telemetry.production_schedules 
            {where_clause}
            ORDER BY scheduled_start ASC
            LIMIT :limit OFFSET :skip
            """
            
            result = await execute_query(query, query_params)
            
            schedules = []
            for schedule in result:
                schedules.append(ProductionScheduleResponse(
                    id=schedule["id"],
                    line_id=schedule["line_id"],
                    product_type_id=schedule["product_type_id"],
                    scheduled_start=schedule["scheduled_start"],
                    scheduled_end=schedule["scheduled_end"],
                    target_quantity=schedule["target_quantity"],
                    priority=schedule["priority"],
                    status=ScheduleStatus(schedule["status"]),
                    notes=schedule["notes"],
                    created_by=schedule["created_by"],
                    created_at=schedule["created_at"],
                    updated_at=schedule["updated_at"]
                ))
            
            return schedules
            
        except Exception as e:
            logger.error("Failed to list production schedules", error=str(e))
            raise BusinessLogicError("Failed to list production schedules")
    
    @staticmethod
    async def delete_schedule(schedule_id: UUID) -> bool:
        """Delete a production schedule."""
        try:
            # Check if schedule exists
            existing = await ProductionScheduleService.get_schedule(schedule_id)
            
            # Check if schedule is in progress
            if existing.status in [ScheduleStatus.IN_PROGRESS]:
                raise BusinessLogicError("Cannot delete schedule that is in progress")
            
            # Delete schedule
            delete_query = """
            DELETE FROM factory_telemetry.production_schedules 
            WHERE id = :schedule_id
            """
            
            deleted_rows = await execute_update(delete_query, {"schedule_id": schedule_id})
            
            if deleted_rows == 0:
                raise BusinessLogicError("Failed to delete production schedule")
            
            logger.info("Production schedule deleted", schedule_id=schedule_id)
            return True
            
        except (NotFoundError, BusinessLogicError):
            raise
        except Exception as e:
            logger.error("Failed to delete production schedule", error=str(e), schedule_id=schedule_id)
            raise BusinessLogicError("Failed to delete production schedule")


class ProductionStatisticsService:
    """Service for production statistics and analytics."""
    
    @staticmethod
    async def get_production_statistics(
        line_id: Optional[UUID] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get comprehensive production statistics."""
        try:
            # Default date range to last 30 days
            if not end_date:
                end_date = datetime.utcnow()
            if not start_date:
                start_date = end_date - timedelta(days=30)
            
            where_conditions = ["ps.created_at >= :start_date", "ps.created_at <= :end_date"]
            query_params = {"start_date": start_date, "end_date": end_date}
            
            if line_id:
                where_conditions.append("ps.line_id = :line_id")
                query_params["line_id"] = line_id
            
            where_clause = "WHERE " + " AND ".join(where_conditions)
            
            # Get basic statistics
            stats_query = f"""
            SELECT 
                COUNT(*) as total_schedules,
                COUNT(CASE WHEN ps.status = 'completed' THEN 1 END) as completed_schedules,
                COUNT(CASE WHEN ps.status = 'in_progress' THEN 1 END) as active_schedules,
                COUNT(CASE WHEN ps.status = 'scheduled' THEN 1 END) as scheduled_schedules,
                SUM(ps.target_quantity) as total_target_quantity,
                SUM(CASE WHEN ps.status = 'completed' THEN ps.target_quantity ELSE 0 END) as completed_quantity,
                AVG(EXTRACT(EPOCH FROM (ps.scheduled_end - ps.scheduled_start))/3600) as avg_schedule_duration_hours
            FROM factory_telemetry.production_schedules ps
            {where_clause}
            """
            
            stats_result = await execute_query(stats_query, query_params)
            stats = stats_result[0] if stats_result else {}
            
            # Get job assignment statistics
            job_stats_query = f"""
            SELECT 
                COUNT(*) as total_assignments,
                COUNT(CASE WHEN ja.status = 'completed' THEN 1 END) as completed_assignments,
                COUNT(CASE WHEN ja.status = 'in_progress' THEN 1 END) as active_assignments,
                COUNT(CASE WHEN ja.status = 'assigned' THEN 1 END) as pending_assignments,
                AVG(EXTRACT(EPOCH FROM (ja.completed_at - ja.started_at))/3600) as avg_completion_time_hours
            FROM factory_telemetry.job_assignments ja
            JOIN factory_telemetry.production_schedules ps ON ja.schedule_id = ps.id
            {where_clause}
            """
            
            job_stats_result = await execute_query(job_stats_query, query_params)
            job_stats = job_stats_result[0] if job_stats_result else {}
            
            # Calculate efficiency metrics
            total_schedules = stats.get("total_schedules", 0)
            completed_schedules = stats.get("completed_schedules", 0)
            schedule_completion_rate = (completed_schedules / total_schedules * 100) if total_schedules > 0 else 0
            
            total_assignments = job_stats.get("total_assignments", 0)
            completed_assignments = job_stats.get("completed_assignments", 0)
            job_completion_rate = (completed_assignments / total_assignments * 100) if total_assignments > 0 else 0
            
            return {
                "period": {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat()
                },
                "schedules": {
                    "total": total_schedules,
                    "completed": completed_schedules,
                    "active": stats.get("active_schedules", 0),
                    "scheduled": stats.get("scheduled_schedules", 0),
                    "completion_rate": round(schedule_completion_rate, 2)
                },
                "production": {
                    "target_quantity": stats.get("total_target_quantity", 0),
                    "completed_quantity": stats.get("completed_quantity", 0),
                    "efficiency_rate": round((stats.get("completed_quantity", 0) / stats.get("total_target_quantity", 1) * 100), 2)
                },
                "jobs": {
                    "total": total_assignments,
                    "completed": completed_assignments,
                    "active": job_stats.get("active_assignments", 0),
                    "pending": job_stats.get("pending_assignments", 0),
                    "completion_rate": round(job_completion_rate, 2)
                },
                "performance": {
                    "avg_schedule_duration_hours": round(stats.get("avg_schedule_duration_hours", 0), 2),
                    "avg_completion_time_hours": round(job_stats.get("avg_completion_time_hours", 0), 2)
                }
            }
            
        except Exception as e:
            logger.error("Failed to get production statistics", error=str(e))
            raise BusinessLogicError("Failed to get production statistics")
    
    @staticmethod
    async def get_line_performance_metrics(line_id: UUID, days: int = 7) -> Dict[str, Any]:
        """Get performance metrics for a specific production line."""
        try:
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=days)
            
            # Get line information
            line_query = """
            SELECT line_code, name, target_speed, equipment_codes
            FROM factory_telemetry.production_lines 
            WHERE id = :line_id
            """
            
            line_result = await execute_query(line_query, {"line_id": line_id})
            if not line_result:
                raise NotFoundError("Production line", str(line_id))
            
            line_info = line_result[0]
            
            # Get schedule performance
            schedule_query = """
            SELECT 
                COUNT(*) as total_schedules,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_schedules,
                COUNT(CASE WHEN status = 'in_progress' THEN 1 END) as active_schedules,
                SUM(target_quantity) as total_target,
                SUM(CASE WHEN status = 'completed' THEN target_quantity ELSE 0 END) as completed_target,
                AVG(EXTRACT(EPOCH FROM (scheduled_end - scheduled_start))/3600) as avg_duration_hours
            FROM factory_telemetry.production_schedules
            WHERE line_id = :line_id 
            AND created_at >= :start_date 
            AND created_at <= :end_date
            """
            
            schedule_result = await execute_query(schedule_query, {
                "line_id": line_id,
                "start_date": start_date,
                "end_date": end_date
            })
            
            schedule_metrics = schedule_result[0] if schedule_result else {}
            
            # Get equipment status (this would integrate with PLC data)
            equipment_status = await ProductionStatisticsService._get_equipment_status(line_info["equipment_codes"])
            
            # Calculate key performance indicators
            total_schedules = schedule_metrics.get("total_schedules", 0)
            completed_schedules = schedule_metrics.get("completed_schedules", 0)
            schedule_efficiency = (completed_schedules / total_schedules * 100) if total_schedules > 0 else 0
            
            total_target = schedule_metrics.get("total_target", 0)
            completed_target = schedule_metrics.get("completed_target", 0)
            production_efficiency = (completed_target / total_target * 100) if total_target > 0 else 0
            
            return {
                "line_info": {
                    "line_code": line_info["line_code"],
                    "name": line_info["name"],
                    "target_speed": line_info["target_speed"]
                },
                "period": {
                    "days": days,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat()
                },
                "schedules": {
                    "total": total_schedules,
                    "completed": completed_schedules,
                    "active": schedule_metrics.get("active_schedules", 0),
                    "efficiency_rate": round(schedule_efficiency, 2)
                },
                "production": {
                    "target_quantity": total_target,
                    "completed_quantity": completed_target,
                    "efficiency_rate": round(production_efficiency, 2)
                },
                "performance": {
                    "avg_duration_hours": round(schedule_metrics.get("avg_duration_hours", 0), 2)
                },
                "equipment": equipment_status
            }
            
        except NotFoundError:
            raise
        except Exception as e:
            logger.error("Failed to get line performance metrics", error=str(e), line_id=line_id)
            raise BusinessLogicError("Failed to get line performance metrics")
    
    @staticmethod
    async def _get_equipment_status(equipment_codes: List[str]) -> Dict[str, Any]:
        """Get equipment status for a list of equipment codes."""
        try:
            # This would typically integrate with PLC data
            # For now, return mock data structure
            equipment_status = {}
            
            for equipment_code in equipment_codes:
                equipment_status[equipment_code] = {
                    "status": "running",  # Would come from PLC data
                    "last_update": datetime.utcnow().isoformat(),
                    "faults": [],  # Would come from fault detection
                    "performance": {
                        "current_speed": 0.0,  # Would come from PLC data
                        "target_speed": 0.0,
                        "efficiency": 0.0
                    }
                }
            
            return equipment_status
            
        except Exception as e:
            logger.error("Failed to get equipment status", error=str(e))
            return {}


class JobAssignmentService:
    """Service for job assignment management."""
    
    @staticmethod
    async def create_job_assignment(assignment_data: JobAssignmentCreate, user_id: UUID) -> JobAssignmentResponse:
        """Create a new job assignment."""
        try:
            # Validate schedule exists
            schedule_query = """
            SELECT id FROM factory_telemetry.production_schedules 
            WHERE id = :schedule_id
            """
            schedule_exists = await execute_scalar(schedule_query, {"schedule_id": assignment_data.schedule_id})
            
            if not schedule_exists:
                raise NotFoundError("Production schedule", str(assignment_data.schedule_id))
            
            # Validate user exists
            user_query = """
            SELECT id FROM factory_telemetry.users 
            WHERE id = :user_id AND is_active = true
            """
            user_exists = await execute_scalar(user_query, {"user_id": assignment_data.user_id})
            
            if not user_exists:
                raise NotFoundError("User", str(assignment_data.user_id))
            
            # Create job assignment
            create_query = """
            INSERT INTO factory_telemetry.job_assignments 
            (schedule_id, user_id, status, notes)
            VALUES (:schedule_id, :user_id, :status, :notes)
            RETURNING id, schedule_id, user_id, assigned_at, accepted_at, started_at, 
                     completed_at, status, notes
            """
            
            result = await execute_query(create_query, {
                "schedule_id": assignment_data.schedule_id,
                "user_id": assignment_data.user_id,
                "status": JobStatus.ASSIGNED,
                "notes": assignment_data.notes
            })
            
            if not result:
                raise BusinessLogicError("Failed to create job assignment")
            
            assignment = result[0]
            
            logger.info("Job assignment created", assignment_id=assignment["id"], user_id=assignment_data.user_id)
            
            return JobAssignmentResponse(
                id=assignment["id"],
                schedule_id=assignment["schedule_id"],
                user_id=assignment["user_id"],
                assigned_at=assignment["assigned_at"],
                accepted_at=assignment["accepted_at"],
                started_at=assignment["started_at"],
                completed_at=assignment["completed_at"],
                status=JobStatus(assignment["status"]),
                notes=assignment["notes"]
            )
            
        except (NotFoundError, BusinessLogicError):
            raise
        except Exception as e:
            logger.error("Failed to create job assignment", error=str(e), user_id=assignment_data.user_id)
            raise BusinessLogicError("Failed to create job assignment")
    
    @staticmethod
    async def get_job_assignment(assignment_id: UUID) -> JobAssignmentResponse:
        """Get a job assignment by ID."""
        try:
            query = """
            SELECT id, schedule_id, user_id, assigned_at, accepted_at, started_at, 
                   completed_at, status, notes
            FROM factory_telemetry.job_assignments 
            WHERE id = :assignment_id
            """
            
            result = await execute_query(query, {"assignment_id": assignment_id})
            
            if not result:
                raise NotFoundError("Job assignment", str(assignment_id))
            
            assignment = result[0]
            
            return JobAssignmentResponse(
                id=assignment["id"],
                schedule_id=assignment["schedule_id"],
                user_id=assignment["user_id"],
                assigned_at=assignment["assigned_at"],
                accepted_at=assignment["accepted_at"],
                started_at=assignment["started_at"],
                completed_at=assignment["completed_at"],
                status=JobStatus(assignment["status"]),
                notes=assignment["notes"]
            )
            
        except NotFoundError:
            raise
        except Exception as e:
            logger.error("Failed to get job assignment", error=str(e), assignment_id=assignment_id)
            raise BusinessLogicError("Failed to get job assignment")
    
    @staticmethod
    async def update_job_assignment(assignment_id: UUID, update_data: JobAssignmentUpdate) -> JobAssignmentResponse:
        """Update a job assignment."""
        try:
            # Check if assignment exists
            existing = await JobAssignmentService.get_job_assignment(assignment_id)
            
            # Build update query dynamically
            update_fields = []
            update_values = {"assignment_id": assignment_id}
            
            if update_data.status is not None:
                update_fields.append("status = :status")
                update_values["status"] = update_data.status.value
                
                # Set timestamps based on status
                if update_data.status == JobStatus.ACCEPTED:
                    update_fields.append("accepted_at = NOW()")
                elif update_data.status == JobStatus.IN_PROGRESS:
                    update_fields.append("started_at = NOW()")
                elif update_data.status == JobStatus.COMPLETED:
                    update_fields.append("completed_at = NOW()")
            
            if update_data.notes is not None:
                update_fields.append("notes = :notes")
                update_values["notes"] = update_data.notes
            
            if not update_fields:
                return existing
            
            update_query = f"""
            UPDATE factory_telemetry.job_assignments 
            SET {', '.join(update_fields)}
            WHERE id = :assignment_id
            """
            
            await execute_update(update_query, update_values)
            
            logger.info("Job assignment updated", assignment_id=assignment_id)
            
            # Return updated assignment
            return await JobAssignmentService.get_job_assignment(assignment_id)
            
        except NotFoundError:
            raise
        except Exception as e:
            logger.error("Failed to update job assignment", error=str(e), assignment_id=assignment_id)
            raise BusinessLogicError("Failed to update job assignment")
    
    @staticmethod
    async def list_job_assignments(
        user_id: Optional[UUID] = None,
        status: Optional[JobStatus] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[JobAssignmentResponse]:
        """List job assignments with filters."""
        try:
            where_conditions = []
            query_params = {"skip": skip, "limit": limit}
            
            if user_id:
                where_conditions.append("user_id = :user_id")
                query_params["user_id"] = user_id
            
            if status:
                where_conditions.append("status = :status")
                query_params["status"] = status.value
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"""
            SELECT id, schedule_id, user_id, assigned_at, accepted_at, started_at, 
                   completed_at, status, notes
            FROM factory_telemetry.job_assignments 
            {where_clause}
            ORDER BY assigned_at DESC
            LIMIT :limit OFFSET :skip
            """
            
            result = await execute_query(query, query_params)
            
            assignments = []
            for assignment in result:
                assignments.append(JobAssignmentResponse(
                    id=assignment["id"],
                    schedule_id=assignment["schedule_id"],
                    user_id=assignment["user_id"],
                    assigned_at=assignment["assigned_at"],
                    accepted_at=assignment["accepted_at"],
                    started_at=assignment["started_at"],
                    completed_at=assignment["completed_at"],
                    status=JobStatus(assignment["status"]),
                    notes=assignment["notes"]
                ))
            
            return assignments
            
        except Exception as e:
            logger.error("Failed to list job assignments", error=str(e))
            raise BusinessLogicError("Failed to list job assignments")
    
    @staticmethod
    async def accept_job(assignment_id: UUID, user_id: UUID) -> JobAssignmentResponse:
        """Accept a job assignment."""
        try:
            # Get assignment
            assignment = await JobAssignmentService.get_job_assignment(assignment_id)
            
            # Check if user is assigned to this job
            if assignment.user_id != user_id:
                raise BusinessLogicError("User not assigned to this job")
            
            # Check if job can be accepted
            if assignment.status != JobStatus.ASSIGNED:
                raise BusinessLogicError("Job cannot be accepted in current status")
            
            # Update status to accepted
            update_data = JobAssignmentUpdate(status=JobStatus.ACCEPTED)
            return await JobAssignmentService.update_job_assignment(assignment_id, update_data)
            
        except (NotFoundError, BusinessLogicError):
            raise
        except Exception as e:
            logger.error("Failed to accept job", error=str(e), assignment_id=assignment_id, user_id=user_id)
            raise BusinessLogicError("Failed to accept job")
    
    @staticmethod
    async def start_job(assignment_id: UUID, user_id: UUID) -> JobAssignmentResponse:
        """Start a job assignment."""
        try:
            # Get assignment
            assignment = await JobAssignmentService.get_job_assignment(assignment_id)
            
            # Check if user is assigned to this job
            if assignment.user_id != user_id:
                raise BusinessLogicError("User not assigned to this job")
            
            # Check if job can be started
            if assignment.status != JobStatus.ACCEPTED:
                raise BusinessLogicError("Job must be accepted before starting")
            
            # Update status to in progress
            update_data = JobAssignmentUpdate(status=JobStatus.IN_PROGRESS)
            return await JobAssignmentService.update_job_assignment(assignment_id, update_data)
            
        except (NotFoundError, BusinessLogicError):
            raise
        except Exception as e:
            logger.error("Failed to start job", error=str(e), assignment_id=assignment_id, user_id=user_id)
            raise BusinessLogicError("Failed to start job")
    
    @staticmethod
    async def complete_job(assignment_id: UUID, user_id: UUID) -> JobAssignmentResponse:
        """Complete a job assignment."""
        try:
            # Get assignment
            assignment = await JobAssignmentService.get_job_assignment(assignment_id)
            
            # Check if user is assigned to this job
            if assignment.user_id != user_id:
                raise BusinessLogicError("User not assigned to this job")
            
            # Check if job can be completed
            if assignment.status != JobStatus.IN_PROGRESS:
                raise BusinessLogicError("Job must be in progress to complete")
            
            # Update status to completed
            update_data = JobAssignmentUpdate(status=JobStatus.COMPLETED)
            return await JobAssignmentService.update_job_assignment(assignment_id, update_data)
            
        except (NotFoundError, BusinessLogicError):
            raise
        except Exception as e:
            logger.error("Failed to complete job", error=str(e), assignment_id=assignment_id, user_id=user_id)
            raise BusinessLogicError("Failed to complete job")