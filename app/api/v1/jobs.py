"""
MS5.0 Floor Dashboard - Job Management API Routes

This module provides API endpoints for job assignment and workflow management
including job acceptance, completion, and status tracking.
"""

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Query, Body
from fastapi.responses import JSONResponse
import structlog

from app.auth.permissions import get_current_user, UserContext, require_permission, Permission
from app.database import get_db
from app.models.production import (
    JobAssignmentResponse, JobAssignmentUpdate, JobAssignmentCreate,
    JobStatus, PaginationParams
)
from app.utils.exceptions import NotFoundError, ValidationError, BusinessLogicError
from app.services.job_assignment_service import JobAssignmentService
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger()

router = APIRouter()
job_service = JobAssignmentService()


@router.post("/assign", response_model=JobAssignmentResponse, status_code=status.HTTP_201_CREATED)
async def assign_job(
    assignment_data: JobAssignmentCreate,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> JobAssignmentResponse:
    """Assign a job to an operator (manager/admin only)."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.JOB_ASSIGN):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to assign jobs"
            )
        
        # Assign the job
        job = await job_service.assign_job_to_operator(
            schedule_id=assignment_data.schedule_id,
            user_id=assignment_data.user_id,
            assigned_by=current_user.user_id,
            notes=assignment_data.notes
        )
        
        logger.info(
            "Job assigned via API", 
            job_id=job.id, 
            user_id=assignment_data.user_id,
            assigned_by=current_user.user_id
        )
        
        return job
        
    except (NotFoundError, ValidationError, ConflictError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to assign job via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/my-jobs", response_model=List[JobAssignmentResponse], status_code=status.HTTP_200_OK)
async def get_my_jobs(
    status: Optional[str] = Query(None, description="Filter by job status"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> List[JobAssignmentResponse]:
    """Get current user's job assignments."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.JOB_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view job assignments"
            )
        
        # Convert status string to enum
        job_status = JobStatus(status) if status else None
        
        # Get user's job assignments
        jobs = await job_service.get_user_jobs(
            user_id=current_user.user_id,
            status=job_status,
            limit=limit
        )
        
        logger.debug(
            "User jobs retrieved via API",
            user_id=current_user.user_id,
            count=len(jobs)
        )
        
        return jobs
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid status: {e}")
    except (NotFoundError, ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to get user jobs via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{job_id}", response_model=JobAssignmentResponse, status_code=status.HTTP_200_OK)
async def get_job_assignment(
    job_id: UUID,
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
        
        # Get job assignment
        job = await job_service.get_job_assignment(job_id)
        
        logger.debug("Job assignment retrieved via API", job_id=job_id, user_id=current_user.user_id)
        
        return job
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to get job assignment via API", error=str(e), job_id=job_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/{job_id}/accept", response_model=JobAssignmentResponse, status_code=status.HTTP_200_OK)
async def accept_job(
    job_id: UUID,
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
        
        # Accept the job
        job = await job_service.accept_job(job_id, current_user.user_id)
        
        logger.info("Job assignment accepted via API", job_id=job_id, user_id=current_user.user_id)
        
        return job
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to accept job via API", error=str(e), job_id=job_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/{job_id}/start", response_model=JobAssignmentResponse, status_code=status.HTTP_200_OK)
async def start_job(
    job_id: UUID,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> JobAssignmentResponse:
    """Start a job assignment."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.JOB_WRITE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to start job assignments"
            )
        
        # Start the job
        job = await job_service.start_job(job_id, current_user.user_id)
        
        logger.info("Job assignment started via API", job_id=job_id, user_id=current_user.user_id)
        
        return job
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to start job via API", error=str(e), job_id=job_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/{job_id}/complete", response_model=JobAssignmentResponse, status_code=status.HTTP_200_OK)
async def complete_job(
    job_id: UUID,
    completion_data: dict = Body(..., description="Completion data including actual quantity and notes"),
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
        
        # Extract completion data
        actual_quantity = completion_data.get("actual_quantity")
        notes = completion_data.get("notes")
        
        # Complete the job
        job = await job_service.complete_job(
            job_id, 
            current_user.user_id, 
            actual_quantity=actual_quantity,
            notes=notes
        )
        
        logger.info("Job assignment completed via API", job_id=job_id, user_id=current_user.user_id)
        
        return job
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to complete job via API", error=str(e), job_id=job_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/{job_id}/cancel", response_model=JobAssignmentResponse, status_code=status.HTTP_200_OK)
async def cancel_job(
    job_id: UUID,
    cancellation_data: dict = Body(..., description="Cancellation data including reason"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> JobAssignmentResponse:
    """Cancel a job assignment."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.JOB_WRITE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to cancel job assignments"
            )
        
        cancellation_reason = cancellation_data.get("reason")
        if not cancellation_reason or len(cancellation_reason.strip()) == 0:
            raise HTTPException(
                status_code=400,
                detail="Cancellation reason is required"
            )
        
        # Cancel the job
        job = await job_service.cancel_job(job_id, current_user.user_id, reason=cancellation_reason)
        
        logger.info("Job assignment cancelled via API", job_id=job_id, user_id=current_user.user_id)
        
        return job
        
    except HTTPException:
        raise
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to cancel job via API", error=str(e), job_id=job_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/", response_model=List[JobAssignmentResponse], status_code=status.HTTP_200_OK)
async def list_job_assignments(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    status: Optional[str] = Query(None, description="Filter by job status"),
    user_id: Optional[UUID] = Query(None, description="Filter by user ID"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> List[JobAssignmentResponse]:
    """List job assignments with filters (admin/manager only)."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.JOB_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view job assignments"
            )
        
        # Convert status string to enum
        job_status = JobStatus(status) if status else None
        
        # Get job assignments
        jobs = await job_service.list_job_assignments(
            line_id=line_id,
            status=job_status,
            skip=skip,
            limit=limit
        )
        
        logger.debug(
            "Job assignments listed via API",
            count=len(jobs),
            user_id=current_user.user_id
        )
        
        return jobs
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid status: {e}")
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to list job assignments via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/statistics", status_code=status.HTTP_200_OK)
async def get_job_statistics(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Get job assignment statistics."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.JOB_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view job statistics"
            )
        
        # Get job statistics
        statistics = await job_service.get_job_statistics()
        
        # Add additional context
        statistics.update({
            "line_id": line_id,
            "period": {
                "start_date": start_date,
                "end_date": end_date
            }
        })
        
        logger.debug(
            "Job statistics retrieved via API",
            user_id=current_user.user_id
        )
        
        return statistics
        
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to get job statistics via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")
