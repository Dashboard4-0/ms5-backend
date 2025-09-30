"""
MS5.0 Floor Dashboard - Production Management API Routes

This module provides API endpoints for production management operations
including production lines, schedules, and job assignments.
"""

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.responses import JSONResponse
import structlog

from app.auth.permissions import get_current_user, UserContext, require_permission, Permission
from app.database import get_db
from app.models.production import (
    ProductionLineCreate, ProductionLineUpdate, ProductionLineResponse,
    ProductionScheduleCreate, ProductionScheduleUpdate, ProductionScheduleResponse,
    JobAssignmentCreate, JobAssignmentUpdate, JobAssignmentResponse,
    PaginationParams, PaginatedResponse
)
from app.services.production_service import ProductionLineService, ProductionScheduleService, JobAssignmentService
from app.utils.exceptions import NotFoundError, ValidationError, ConflictError, BusinessLogicError
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger()

router = APIRouter()


# Production Line Endpoints
@router.post("/lines", response_model=ProductionLineResponse, status_code=status.HTTP_201_CREATED)
async def create_production_line(
    line_data: ProductionLineCreate,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> ProductionLineResponse:
    """Create a new production line."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.LINE_WRITE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to create production lines"
            )
        
        line = await ProductionLineService.create_production_line(line_data)
        
        logger.info(
            "Production line created via API",
            line_id=line.id,
            line_code=line.line_code,
            user_id=current_user.user_id
        )
        
        return line
        
    except (ValidationError, ConflictError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to create production line via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/lines", response_model=List[ProductionLineResponse], status_code=status.HTTP_200_OK)
async def list_production_lines(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    enabled_only: bool = Query(True, description="Return only enabled lines"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> List[ProductionLineResponse]:
    """List production lines with pagination."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.LINE_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view production lines"
            )
        
        lines = await ProductionLineService.list_production_lines(
            skip=skip,
            limit=limit,
            enabled_only=enabled_only
        )
        
        logger.debug(
            "Production lines listed via API",
            count=len(lines),
            user_id=current_user.user_id
        )
        
        return lines
        
    except Exception as e:
        logger.error("Failed to list production lines via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/lines/{line_id}", response_model=ProductionLineResponse, status_code=status.HTTP_200_OK)
async def get_production_line(
    line_id: UUID,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> ProductionLineResponse:
    """Get a production line by ID."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.LINE_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view production lines"
            )
        
        line = await ProductionLineService.get_production_line(line_id)
        
        logger.debug(
            "Production line retrieved via API",
            line_id=line_id,
            user_id=current_user.user_id
        )
        
        return line
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Failed to get production line via API", error=str(e), line_id=line_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/lines/{line_id}", response_model=ProductionLineResponse, status_code=status.HTTP_200_OK)
async def update_production_line(
    line_id: UUID,
    update_data: ProductionLineUpdate,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> ProductionLineResponse:
    """Update a production line."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.LINE_WRITE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to update production lines"
            )
        
        line = await ProductionLineService.update_production_line(line_id, update_data)
        
        logger.info(
            "Production line updated via API",
            line_id=line_id,
            user_id=current_user.user_id
        )
        
        return line
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to update production line via API", error=str(e), line_id=line_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/lines/{line_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_production_line(
    line_id: UUID,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> JSONResponse:
    """Delete a production line."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.LINE_DELETE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to delete production lines"
            )
        
        await ProductionLineService.delete_production_line(line_id)
        
        logger.info(
            "Production line deleted via API",
            line_id=line_id,
            user_id=current_user.user_id
        )
        
        return JSONResponse(content={"message": "Production line deleted successfully"})
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to delete production line via API", error=str(e), line_id=line_id)
        raise HTTPException(status_code=500, detail="Internal server error")


# Production Schedule Endpoints
@router.post("/schedules", response_model=ProductionScheduleResponse, status_code=status.HTTP_201_CREATED)
async def create_production_schedule(
    schedule_data: ProductionScheduleCreate,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> ProductionScheduleResponse:
    """Create a new production schedule."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.SCHEDULE_WRITE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to create production schedules"
            )
        
        schedule = await ProductionScheduleService.create_schedule(schedule_data, UUID(current_user.user_id))
        
        logger.info(
            "Production schedule created via API",
            schedule_id=schedule.id,
            line_id=schedule.line_id,
            user_id=current_user.user_id
        )
        
        return schedule
        
    except (ValidationError, ConflictError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to create production schedule via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/schedules", response_model=List[ProductionScheduleResponse], status_code=status.HTTP_200_OK)
async def list_production_schedules(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    status: Optional[str] = Query(None, description="Filter by schedule status"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> List[ProductionScheduleResponse]:
    """List production schedules with filters."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.SCHEDULE_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view production schedules"
            )
        
        from app.models.production import ScheduleStatus
        schedule_status = ScheduleStatus(status) if status else None
        
        schedules = await ProductionScheduleService.list_schedules(
            line_id=line_id,
            status=schedule_status,
            skip=skip,
            limit=limit
        )
        
        logger.debug(
            "Production schedules listed via API",
            count=len(schedules),
            user_id=current_user.user_id
        )
        
        return schedules
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid status: {e}")
    except Exception as e:
        logger.error("Failed to list production schedules via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/schedules/{schedule_id}", response_model=ProductionScheduleResponse, status_code=status.HTTP_200_OK)
async def get_production_schedule(
    schedule_id: UUID,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> ProductionScheduleResponse:
    """Get a production schedule by ID."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.SCHEDULE_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view production schedules"
            )
        
        schedule = await ProductionScheduleService.get_schedule(schedule_id)
        
        logger.debug(
            "Production schedule retrieved via API",
            schedule_id=schedule_id,
            user_id=current_user.user_id
        )
        
        return schedule
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Failed to get production schedule via API", error=str(e), schedule_id=schedule_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/schedules/{schedule_id}", response_model=ProductionScheduleResponse, status_code=status.HTTP_200_OK)
async def update_production_schedule(
    schedule_id: UUID,
    update_data: ProductionScheduleUpdate,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> ProductionScheduleResponse:
    """Update a production schedule."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.SCHEDULE_WRITE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to update production schedules"
            )
        
        schedule = await ProductionScheduleService.update_schedule(schedule_id, update_data)
        
        logger.info(
            "Production schedule updated via API",
            schedule_id=schedule_id,
            user_id=current_user.user_id
        )
        
        return schedule
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to update production schedule via API", error=str(e), schedule_id=schedule_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/schedules/{schedule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_production_schedule(
    schedule_id: UUID,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> JSONResponse:
    """Delete a production schedule."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.SCHEDULE_DELETE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to delete production schedules"
            )
        
        await ProductionScheduleService.delete_schedule(schedule_id)
        
        logger.info(
            "Production schedule deleted via API",
            schedule_id=schedule_id,
            user_id=current_user.user_id
        )
        
        return JSONResponse(content={"message": "Production schedule deleted successfully"})
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to delete production schedule via API", error=str(e), schedule_id=schedule_id)
        raise HTTPException(status_code=500, detail="Internal server error")


# Production Statistics Endpoints
@router.get("/statistics", status_code=status.HTTP_200_OK)
async def get_production_statistics(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Get production statistics."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.PRODUCTION_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view production statistics"
            )
        
        # This would be implemented with actual statistics calculation
        statistics = {
            "total_lines": 0,
            "active_lines": 0,
            "total_schedules": 0,
            "active_schedules": 0,
            "completed_schedules": 0,
            "average_oee": 0.0
        }
        
        logger.debug(
            "Production statistics retrieved via API",
            user_id=current_user.user_id
        )
        
        return statistics
        
    except Exception as e:
        logger.error("Failed to get production statistics via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


# Job Assignment Endpoints
@router.post("/job-assignments", response_model=JobAssignmentResponse, status_code=status.HTTP_201_CREATED)
async def create_job_assignment(
    assignment_data: JobAssignmentCreate,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> JobAssignmentResponse:
    """Create a new job assignment."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.JOB_WRITE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to create job assignments"
            )
        
        assignment = await JobAssignmentService.create_job_assignment(assignment_data, UUID(current_user.user_id))
        
        logger.info(
            "Job assignment created via API",
            assignment_id=assignment.id,
            user_id=current_user.user_id
        )
        
        return assignment
        
    except (ValidationError, ConflictError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to create job assignment via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/job-assignments", response_model=List[JobAssignmentResponse], status_code=status.HTTP_200_OK)
async def list_job_assignments(
    user_id: Optional[UUID] = Query(None, description="Filter by user ID"),
    status: Optional[str] = Query(None, description="Filter by job status"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> List[JobAssignmentResponse]:
    """List job assignments with filters."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.JOB_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view job assignments"
            )
        
        from app.models.production import JobStatus
        job_status = JobStatus(status) if status else None
        
        assignments = await JobAssignmentService.list_job_assignments(
            user_id=user_id,
            status=job_status,
            skip=skip,
            limit=limit
        )
        
        logger.debug(
            "Job assignments listed via API",
            count=len(assignments),
            user_id=current_user.user_id
        )
        
        return assignments
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid status: {e}")
    except Exception as e:
        logger.error("Failed to list job assignments via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/job-assignments/{assignment_id}", response_model=JobAssignmentResponse, status_code=status.HTTP_200_OK)
async def get_job_assignment(
    assignment_id: UUID,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> JobAssignmentResponse:
    """Get a job assignment by ID."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.JOB_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view job assignments"
            )
        
        assignment = await JobAssignmentService.get_job_assignment(assignment_id)
        
        logger.debug(
            "Job assignment retrieved via API",
            assignment_id=assignment_id,
            user_id=current_user.user_id
        )
        
        return assignment
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Failed to get job assignment via API", error=str(e), assignment_id=assignment_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/job-assignments/{assignment_id}", response_model=JobAssignmentResponse, status_code=status.HTTP_200_OK)
async def update_job_assignment(
    assignment_id: UUID,
    update_data: JobAssignmentUpdate,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> JobAssignmentResponse:
    """Update a job assignment."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.JOB_WRITE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to update job assignments"
            )
        
        assignment = await JobAssignmentService.update_job_assignment(assignment_id, update_data)
        
        logger.info(
            "Job assignment updated via API",
            assignment_id=assignment_id,
            user_id=current_user.user_id
        )
        
        return assignment
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to update job assignment via API", error=str(e), assignment_id=assignment_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/job-assignments/{assignment_id}/accept", response_model=JobAssignmentResponse, status_code=status.HTTP_200_OK)
async def accept_job(
    assignment_id: UUID,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> JobAssignmentResponse:
    """Accept a job assignment."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.JOB_ACCEPT):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to accept job assignments"
            )
        
        assignment = await JobAssignmentService.accept_job(assignment_id, UUID(current_user.user_id))
        
        logger.info(
            "Job assignment accepted via API",
            assignment_id=assignment_id,
            user_id=current_user.user_id
        )
        
        return assignment
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to accept job via API", error=str(e), assignment_id=assignment_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/job-assignments/{assignment_id}/start", response_model=JobAssignmentResponse, status_code=status.HTTP_200_OK)
async def start_job(
    assignment_id: UUID,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> JobAssignmentResponse:
    """Start a job assignment."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.JOB_START):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to start job assignments"
            )
        
        assignment = await JobAssignmentService.start_job(assignment_id, UUID(current_user.user_id))
        
        logger.info(
            "Job assignment started via API",
            assignment_id=assignment_id,
            user_id=current_user.user_id
        )
        
        return assignment
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to start job via API", error=str(e), assignment_id=assignment_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/job-assignments/{assignment_id}/complete", response_model=JobAssignmentResponse, status_code=status.HTTP_200_OK)
async def complete_job(
    assignment_id: UUID,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> JobAssignmentResponse:
    """Complete a job assignment."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.JOB_COMPLETE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to complete job assignments"
            )
        
        assignment = await JobAssignmentService.complete_job(assignment_id, UUID(current_user.user_id))
        
        logger.info(
            "Job assignment completed via API",
            assignment_id=assignment_id,
            user_id=current_user.user_id
        )
        
        return assignment
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to complete job via API", error=str(e), assignment_id=assignment_id)
        raise HTTPException(status_code=500, detail="Internal server error")


# Phase 3 Implementation - Enhanced Production API Endpoints

@router.get("/analytics/statistics", status_code=status.HTTP_200_OK)
async def get_production_statistics(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Get comprehensive production statistics."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.PRODUCTION_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view production statistics"
            )
        
        # Parse dates
        from datetime import datetime
        start_dt = None
        end_dt = None
        
        if start_date:
            try:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid start_date format. Use YYYY-MM-DD")
        
        if end_date:
            try:
                end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid end_date format. Use YYYY-MM-DD")
        
        from app.services.production_service import ProductionStatisticsService
        statistics = await ProductionStatisticsService.get_production_statistics(
            line_id=line_id,
            start_date=start_dt,
            end_date=end_dt
        )
        
        logger.debug(
            "Production statistics retrieved via API",
            line_id=line_id,
            user_id=current_user.user_id
        )
        
        return statistics
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get production statistics via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/analytics/line-performance/{line_id}", status_code=status.HTTP_200_OK)
async def get_line_performance_metrics(
    line_id: UUID,
    days: int = Query(7, ge=1, le=30, description="Number of days for performance analysis"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Get performance metrics for a specific production line."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.PRODUCTION_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view line performance metrics"
            )
        
        from app.services.production_service import ProductionStatisticsService
        metrics = await ProductionStatisticsService.get_line_performance_metrics(
            line_id=line_id,
            days=days
        )
        
        logger.debug(
            "Line performance metrics retrieved via API",
            line_id=line_id,
            days=days,
            user_id=current_user.user_id
        )
        
        return metrics
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Failed to get line performance metrics via API", error=str(e), line_id=line_id)
        raise HTTPException(status_code=500, detail="Internal server error")
