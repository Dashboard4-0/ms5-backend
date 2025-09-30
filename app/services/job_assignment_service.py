"""
MS5.0 Floor Dashboard - Job Assignment Service

This module contains the business logic for job assignment and workflow management,
including job assignment, acceptance, execution, and completion workflows.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from uuid import UUID
import structlog

from app.database import execute_query, execute_scalar, execute_update, get_db_session
from app.models.production import (
    JobAssignmentCreate, JobAssignmentUpdate, JobAssignmentResponse,
    ChecklistCompletionCreate, ChecklistCompletionResponse,
    JobStatus, ScheduleStatus
)
from app.utils.exceptions import (
    NotFoundError, ValidationError, ConflictError, BusinessLogicError,
    JobAssignmentError
)
from app.services.notification_service import NotificationService
from app.api.websocket import broadcast_job_update

logger = structlog.get_logger()


class JobAssignmentService:
    """Service for job assignment and workflow management."""
    
    def __init__(self):
        self.notification_service = NotificationService()
    
    async def assign_job_to_operator(
        self, 
        schedule_id: UUID, 
        user_id: UUID, 
        assigned_by: UUID,
        notes: Optional[str] = None
    ) -> JobAssignmentResponse:
        """Assign a production job to an operator."""
        try:
            # Get schedule details
            schedule_query = """
            SELECT ps.id, ps.line_id, ps.product_type_id, ps.scheduled_start, 
                   ps.scheduled_end, ps.target_quantity, ps.priority, ps.status,
                   pl.line_code, pl.name as line_name,
                   pt.product_code, pt.name as product_name
            FROM factory_telemetry.production_schedules ps
            JOIN factory_telemetry.production_lines pl ON ps.line_id = pl.id
            JOIN factory_telemetry.product_types pt ON ps.product_type_id = pt.id
            WHERE ps.id = :schedule_id
            """
            
            schedule_result = await execute_query(schedule_query, {"schedule_id": schedule_id})
            if not schedule_result:
                raise NotFoundError("Production schedule", str(schedule_id))
            
            schedule = schedule_result[0]
            
            # Validate schedule is in correct status
            if schedule["status"] != ScheduleStatus.SCHEDULED.value:
                raise BusinessLogicError("Schedule must be in 'scheduled' status to assign jobs")
            
            # Check if user already has an active job assignment
            active_job_query = """
            SELECT id FROM factory_telemetry.job_assignments 
            WHERE user_id = :user_id AND status IN ('assigned', 'accepted', 'in_progress')
            """
            
            active_job = await execute_scalar(active_job_query, {"user_id": user_id})
            if active_job:
                raise ConflictError("User already has an active job assignment")
            
            # Check if schedule already has an active assignment
            existing_assignment_query = """
            SELECT id FROM factory_telemetry.job_assignments 
            WHERE schedule_id = :schedule_id AND status IN ('assigned', 'accepted', 'in_progress')
            """
            
            existing_assignment = await execute_scalar(existing_assignment_query, {"schedule_id": schedule_id})
            if existing_assignment:
                raise ConflictError("Schedule already has an active job assignment")
            
            # Create job assignment
            create_query = """
            INSERT INTO factory_telemetry.job_assignments 
            (schedule_id, user_id, assigned_at, status, notes)
            VALUES (:schedule_id, :user_id, NOW(), :status, :notes)
            RETURNING id, schedule_id, user_id, assigned_at, accepted_at, 
                     started_at, completed_at, status, notes
            """
            
            result = await execute_query(create_query, {
                "schedule_id": schedule_id,
                "user_id": user_id,
                "status": JobStatus.ASSIGNED.value,
                "notes": notes
            })
            
            if not result:
                raise BusinessLogicError("Failed to create job assignment")
            
            assignment = result[0]
            
            # Update schedule status to in_progress
            update_schedule_query = """
            UPDATE factory_telemetry.production_schedules 
            SET status = :status, updated_at = NOW()
            WHERE id = :schedule_id
            """
            
            await execute_update(update_schedule_query, {
                "schedule_id": schedule_id,
                "status": ScheduleStatus.IN_PROGRESS.value
            })
            
            # Send notification to operator
            await self._send_job_notification(user_id, assignment, schedule, "assigned")
            
            logger.info(
                "Job assignment created", 
                assignment_id=assignment["id"], 
                user_id=user_id, 
                schedule_id=schedule_id
            )
            
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
            
        except (NotFoundError, ConflictError, BusinessLogicError):
            raise
        except Exception as e:
            logger.error("Failed to assign job", error=str(e), schedule_id=schedule_id, user_id=user_id)
            raise BusinessLogicError("Failed to assign job")
    
    async def accept_job(self, assignment_id: UUID, user_id: UUID) -> JobAssignmentResponse:
        """Operator accepts a job assignment."""
        try:
            # Get assignment details
            assignment = await self._get_job_assignment(assignment_id)
            
            # Validate user authorization
            if assignment["user_id"] != user_id:
                raise ValidationError("User not authorized for this job")
            
            # Validate status
            if assignment["status"] != JobStatus.ASSIGNED.value:
                raise ValidationError("Job cannot be accepted in current status")
            
            # Update assignment status
            update_query = """
            UPDATE factory_telemetry.job_assignments 
            SET status = :status, accepted_at = NOW(), updated_at = NOW()
            WHERE id = :assignment_id
            """
            
            await execute_update(update_query, {
                "assignment_id": assignment_id,
                "status": JobStatus.ACCEPTED.value
            })
            
            # Get updated assignment
            updated_assignment = await self._get_job_assignment(assignment_id)
            
            # Send notification to managers
            await self._send_job_notification(user_id, updated_assignment, None, "accepted")
            
            # Broadcast WebSocket update
            await broadcast_job_update({
                "job_id": str(assignment_id),
                "user_id": str(user_id),
                "status": "accepted",
                "accepted_at": updated_assignment["accepted_at"].isoformat() if updated_assignment["accepted_at"] else None
            })
            
            logger.info("Job assignment accepted", assignment_id=assignment_id, user_id=user_id)
            
            return self._format_job_assignment_response(updated_assignment)
            
        except (NotFoundError, ValidationError):
            raise
        except Exception as e:
            logger.error("Failed to accept job", error=str(e), assignment_id=assignment_id, user_id=user_id)
            raise BusinessLogicError("Failed to accept job")
    
    async def start_job(self, assignment_id: UUID, user_id: UUID) -> JobAssignmentResponse:
        """Start a job assignment."""
        try:
            # Get assignment details
            assignment = await self._get_job_assignment(assignment_id)
            
            # Validate user authorization
            if assignment["user_id"] != user_id:
                raise ValidationError("User not authorized for this job")
            
            # Validate status
            if assignment["status"] != JobStatus.ACCEPTED.value:
                raise ValidationError("Job must be accepted before starting")
            
            # Update assignment status
            update_query = """
            UPDATE factory_telemetry.job_assignments 
            SET status = :status, started_at = NOW(), updated_at = NOW()
            WHERE id = :assignment_id
            """
            
            await execute_update(update_query, {
                "assignment_id": assignment_id,
                "status": JobStatus.IN_PROGRESS.value
            })
            
            # Get updated assignment
            updated_assignment = await self._get_job_assignment(assignment_id)
            
            # Send notification to managers
            await self._send_job_notification(user_id, updated_assignment, None, "started")
            
            # Broadcast WebSocket update
            await broadcast_job_update({
                "job_id": str(assignment_id),
                "user_id": str(user_id),
                "status": "in_progress",
                "started_at": updated_assignment["started_at"].isoformat() if updated_assignment["started_at"] else None
            })
            
            logger.info("Job assignment started", assignment_id=assignment_id, user_id=user_id)
            
            return self._format_job_assignment_response(updated_assignment)
            
        except (NotFoundError, ValidationError):
            raise
        except Exception as e:
            logger.error("Failed to start job", error=str(e), assignment_id=assignment_id, user_id=user_id)
            raise BusinessLogicError("Failed to start job")
    
    async def complete_job(
        self, 
        assignment_id: UUID, 
        user_id: UUID, 
        actual_quantity: Optional[int] = None,
        notes: Optional[str] = None
    ) -> JobAssignmentResponse:
        """Complete a job assignment."""
        try:
            # Get assignment details
            assignment = await self._get_job_assignment(assignment_id)
            
            # Validate user authorization
            if assignment["user_id"] != user_id:
                raise ValidationError("User not authorized for this job")
            
            # Validate status
            if assignment["status"] != JobStatus.IN_PROGRESS.value:
                raise ValidationError("Job must be in progress to complete")
            
            # Update assignment status
            update_query = """
            UPDATE factory_telemetry.job_assignments 
            SET status = :status, completed_at = NOW(), updated_at = NOW()
            WHERE id = :assignment_id
            """
            
            await execute_update(update_query, {
                "assignment_id": assignment_id,
                "status": JobStatus.COMPLETED.value
            })
            
            # Update schedule status to completed
            schedule_update_query = """
            UPDATE factory_telemetry.production_schedules 
            SET status = :status, updated_at = NOW()
            WHERE id = :schedule_id
            """
            
            await execute_update(schedule_update_query, {
                "schedule_id": assignment["schedule_id"],
                "status": ScheduleStatus.COMPLETED.value
            })
            
            # Get updated assignment
            updated_assignment = await self._get_job_assignment(assignment_id)
            
            # Send notification to managers
            await self._send_job_notification(user_id, updated_assignment, None, "completed")
            
            # Broadcast WebSocket update
            await broadcast_job_update({
                "job_id": str(assignment_id),
                "user_id": str(user_id),
                "status": "completed",
                "completed_at": updated_assignment["completed_at"].isoformat() if updated_assignment["completed_at"] else None,
                "actual_quantity": actual_quantity
            })
            
            logger.info("Job assignment completed", assignment_id=assignment_id, user_id=user_id)
            
            return self._format_job_assignment_response(updated_assignment)
            
        except (NotFoundError, ValidationError):
            raise
        except Exception as e:
            logger.error("Failed to complete job", error=str(e), assignment_id=assignment_id, user_id=user_id)
            raise BusinessLogicError("Failed to complete job")
    
    async def cancel_job(
        self, 
        assignment_id: UUID, 
        user_id: UUID, 
        reason: Optional[str] = None
    ) -> JobAssignmentResponse:
        """Cancel a job assignment."""
        try:
            # Get assignment details
            assignment = await self._get_job_assignment(assignment_id)
            
            # Validate user authorization (user or manager can cancel)
            if assignment["user_id"] != user_id:
                # Check if user is manager (this would be implemented with proper role checking)
                # For now, allow cancellation
                pass
            
            # Validate status
            if assignment["status"] in [JobStatus.COMPLETED.value, JobStatus.CANCELLED.value]:
                raise ValidationError("Job cannot be cancelled in current status")
            
            # Update assignment status
            update_query = """
            UPDATE factory_telemetry.job_assignments 
            SET status = :status, completed_at = NOW(), updated_at = NOW()
            WHERE id = :assignment_id
            """
            
            await execute_update(update_query, {
                "assignment_id": assignment_id,
                "status": JobStatus.CANCELLED.value
            })
            
            # Update schedule status back to scheduled
            schedule_update_query = """
            UPDATE factory_telemetry.production_schedules 
            SET status = :status, updated_at = NOW()
            WHERE id = :schedule_id
            """
            
            await execute_update(schedule_update_query, {
                "schedule_id": assignment["schedule_id"],
                "status": ScheduleStatus.SCHEDULED.value
            })
            
            # Get updated assignment
            updated_assignment = await self._get_job_assignment(assignment_id)
            
            # Send notification
            await self._send_job_notification(user_id, updated_assignment, None, "cancelled")
            
            # Broadcast WebSocket update
            await broadcast_job_update({
                "job_id": str(assignment_id),
                "user_id": str(user_id),
                "status": "cancelled",
                "cancelled_at": updated_assignment["completed_at"].isoformat() if updated_assignment["completed_at"] else None,
                "cancellation_reason": reason
            })
            
            logger.info("Job assignment cancelled", assignment_id=assignment_id, user_id=user_id)
            
            return self._format_job_assignment_response(updated_assignment)
            
        except (NotFoundError, ValidationError):
            raise
        except Exception as e:
            logger.error("Failed to cancel job", error=str(e), assignment_id=assignment_id, user_id=user_id)
            raise BusinessLogicError("Failed to cancel job")
    
    async def get_user_jobs(
        self, 
        user_id: UUID, 
        status: Optional[JobStatus] = None,
        limit: int = 50
    ) -> List[JobAssignmentResponse]:
        """Get job assignments for a specific user."""
        try:
            where_conditions = ["ja.user_id = :user_id"]
            query_params = {"user_id": user_id, "limit": limit}
            
            if status:
                where_conditions.append("ja.status = :status")
                query_params["status"] = status.value
            
            where_clause = "WHERE " + " AND ".join(where_conditions)
            
            query = f"""
            SELECT ja.id, ja.schedule_id, ja.user_id, ja.assigned_at, ja.accepted_at,
                   ja.started_at, ja.completed_at, ja.status, ja.notes,
                   ps.scheduled_start, ps.scheduled_end, ps.target_quantity,
                   pl.line_code, pl.name as line_name,
                   pt.product_code, pt.name as product_name
            FROM factory_telemetry.job_assignments ja
            JOIN factory_telemetry.production_schedules ps ON ja.schedule_id = ps.id
            JOIN factory_telemetry.production_lines pl ON ps.line_id = pl.id
            JOIN factory_telemetry.product_types pt ON ps.product_type_id = pt.id
            {where_clause}
            ORDER BY ja.assigned_at DESC
            LIMIT :limit
            """
            
            result = await execute_query(query, query_params)
            
            jobs = []
            for job in result:
                jobs.append(self._format_job_assignment_response(job))
            
            return jobs
            
        except Exception as e:
            logger.error("Failed to get user jobs", error=str(e), user_id=user_id)
            raise BusinessLogicError("Failed to get user jobs")
    
    async def get_job_assignment(self, assignment_id: UUID) -> JobAssignmentResponse:
        """Get a job assignment by ID."""
        try:
            assignment = await self._get_job_assignment(assignment_id)
            return self._format_job_assignment_response(assignment)
            
        except NotFoundError:
            raise
        except Exception as e:
            logger.error("Failed to get job assignment", error=str(e), assignment_id=assignment_id)
            raise BusinessLogicError("Failed to get job assignment")
    
    async def list_job_assignments(
        self,
        line_id: Optional[UUID] = None,
        status: Optional[JobStatus] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[JobAssignmentResponse]:
        """List job assignments with filters (admin/manager only)."""
        try:
            where_conditions = []
            query_params = {"skip": skip, "limit": limit}
            
            if line_id:
                where_conditions.append("ps.line_id = :line_id")
                query_params["line_id"] = line_id
            
            if status:
                where_conditions.append("ja.status = :status")
                query_params["status"] = status.value
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"""
            SELECT ja.id, ja.schedule_id, ja.user_id, ja.assigned_at, ja.accepted_at,
                   ja.started_at, ja.completed_at, ja.status, ja.notes,
                   ps.scheduled_start, ps.scheduled_end, ps.target_quantity,
                   pl.line_code, pl.name as line_name,
                   pt.product_code, pt.name as product_name
            FROM factory_telemetry.job_assignments ja
            JOIN factory_telemetry.production_schedules ps ON ja.schedule_id = ps.id
            JOIN factory_telemetry.production_lines pl ON ps.line_id = pl.id
            JOIN factory_telemetry.product_types pt ON ps.product_type_id = pt.id
            {where_clause}
            ORDER BY ja.assigned_at DESC
            LIMIT :limit OFFSET :skip
            """
            
            result = await execute_query(query, query_params)
            
            jobs = []
            for job in result:
                jobs.append(self._format_job_assignment_response(job))
            
            return jobs
            
        except Exception as e:
            logger.error("Failed to list job assignments", error=str(e))
            raise BusinessLogicError("Failed to list job assignments")
    
    async def get_job_statistics(self) -> Dict[str, Any]:
        """Get job assignment statistics."""
        try:
            stats_query = """
            SELECT 
                COUNT(*) as total_jobs,
                COUNT(CASE WHEN status = 'assigned' THEN 1 END) as assigned_jobs,
                COUNT(CASE WHEN status = 'accepted' THEN 1 END) as accepted_jobs,
                COUNT(CASE WHEN status = 'in_progress' THEN 1 END) as in_progress_jobs,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_jobs,
                COUNT(CASE WHEN status = 'cancelled' THEN 1 END) as cancelled_jobs,
                AVG(CASE WHEN completed_at IS NOT NULL AND started_at IS NOT NULL 
                    THEN EXTRACT(EPOCH FROM (completed_at - started_at))/3600 
                    END) as avg_completion_hours
            FROM factory_telemetry.job_assignments
            WHERE assigned_at >= NOW() - INTERVAL '30 days'
            """
            
            result = await execute_query(stats_query, {})
            
            if not result:
                return {
                    "total_jobs": 0,
                    "assigned_jobs": 0,
                    "accepted_jobs": 0,
                    "in_progress_jobs": 0,
                    "completed_jobs": 0,
                    "cancelled_jobs": 0,
                    "avg_completion_hours": 0
                }
            
            stats = result[0]
            return {
                "total_jobs": stats["total_jobs"] or 0,
                "assigned_jobs": stats["assigned_jobs"] or 0,
                "accepted_jobs": stats["accepted_jobs"] or 0,
                "in_progress_jobs": stats["in_progress_jobs"] or 0,
                "completed_jobs": stats["completed_jobs"] or 0,
                "cancelled_jobs": stats["cancelled_jobs"] or 0,
                "avg_completion_hours": round(stats["avg_completion_hours"] or 0, 2)
            }
            
        except Exception as e:
            logger.error("Failed to get job statistics", error=str(e))
            raise BusinessLogicError("Failed to get job statistics")
    
    async def _get_job_assignment(self, assignment_id: UUID) -> Dict[str, Any]:
        """Get job assignment details from database."""
        query = """
        SELECT ja.id, ja.schedule_id, ja.user_id, ja.assigned_at, ja.accepted_at,
               ja.started_at, ja.completed_at, ja.status, ja.notes
        FROM factory_telemetry.job_assignments ja
        WHERE ja.id = :assignment_id
        """
        
        result = await execute_query(query, {"assignment_id": assignment_id})
        
        if not result:
            raise NotFoundError("Job assignment", str(assignment_id))
        
        return result[0]
    
    def _format_job_assignment_response(self, job_data: Dict[str, Any]) -> JobAssignmentResponse:
        """Format job assignment data into response model."""
        return JobAssignmentResponse(
            id=job_data["id"],
            schedule_id=job_data["schedule_id"],
            user_id=job_data["user_id"],
            assigned_at=job_data["assigned_at"],
            accepted_at=job_data["accepted_at"],
            started_at=job_data["started_at"],
            completed_at=job_data["completed_at"],
            status=JobStatus(job_data["status"]),
            notes=job_data["notes"]
        )
    
    async def _send_job_notification(
        self, 
        user_id: UUID, 
        assignment: Dict[str, Any], 
        schedule: Optional[Dict[str, Any]], 
        action: str
    ) -> None:
        """Send notification for job assignment events."""
        try:
            if not schedule:
                # Get schedule details if not provided
                schedule_query = """
                SELECT ps.scheduled_start, ps.scheduled_end, ps.target_quantity,
                       pl.line_code, pl.name as line_name,
                       pt.product_code, pt.name as product_name
                FROM factory_telemetry.production_schedules ps
                JOIN factory_telemetry.production_lines pl ON ps.line_id = pl.id
                JOIN factory_telemetry.product_types pt ON ps.product_type_id = pt.id
                WHERE ps.id = :schedule_id
                """
                
                schedule_result = await execute_query(schedule_query, {"schedule_id": assignment["schedule_id"]})
                if schedule_result:
                    schedule = schedule_result[0]
            
            if not schedule:
                return
            
            # Create notification message
            action_messages = {
                "assigned": f"New job assigned: {schedule['product_name']} on {schedule['line_name']}",
                "accepted": f"Job accepted: {schedule['product_name']} on {schedule['line_name']}",
                "started": f"Job started: {schedule['product_name']} on {schedule['line_name']}",
                "completed": f"Job completed: {schedule['product_name']} on {schedule['line_name']}",
                "cancelled": f"Job cancelled: {schedule['product_name']} on {schedule['line_name']}"
            }
            
            message = action_messages.get(action, f"Job {action}")
            
            # Send notification to user
            await self.notification_service.send_notification(
                user_id=user_id,
                title="Job Assignment Update",
                body=message,
                notification_type="job_assignment"
            )
            
        except Exception as e:
            logger.error("Failed to send job notification", error=str(e), action=action)
            # Don't raise exception as notification failure shouldn't break the workflow
