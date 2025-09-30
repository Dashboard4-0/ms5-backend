"""
MS5.0 Floor Dashboard - Checklist Management API Routes

This module provides API endpoints for checklist template management and
checklist completion workflows.
"""

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Query, Body
from fastapi.responses import JSONResponse
import structlog

from app.auth.permissions import get_current_user, UserContext, require_permission, Permission
from app.database import get_db
from app.models.production import (
    ChecklistTemplateCreate, ChecklistTemplateUpdate, ChecklistTemplateResponse,
    ChecklistCompletionCreate, ChecklistCompletionResponse
)
from app.utils.exceptions import NotFoundError, ValidationError, BusinessLogicError
from app.services.checklist_service import ChecklistService
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger()

router = APIRouter()
checklist_service = ChecklistService()


@router.post("/templates", response_model=ChecklistTemplateResponse, status_code=status.HTTP_201_CREATED)
async def create_checklist_template(
    template_data: ChecklistTemplateCreate,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> ChecklistTemplateResponse:
    """Create a new checklist template (admin/manager only)."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.CHECKLIST_WRITE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to create checklist templates"
            )
        
        # Create template
        template = await checklist_service.create_checklist_template(template_data)
        
        logger.info(
            "Checklist template created via API", 
            template_id=template.id, 
            name=template_data.name,
            user_id=current_user.user_id
        )
        
        return template
        
    except (ValidationError, ConflictError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to create checklist template via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/templates", response_model=List[ChecklistTemplateResponse], status_code=status.HTTP_200_OK)
async def list_checklist_templates(
    equipment_codes: Optional[str] = Query(None, description="Comma-separated equipment codes to filter by"),
    enabled_only: bool = Query(True, description="Only return enabled templates"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> List[ChecklistTemplateResponse]:
    """List checklist templates with filters."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.CHECKLIST_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view checklist templates"
            )
        
        # Parse equipment codes
        equipment_list = None
        if equipment_codes:
            equipment_list = [code.strip() for code in equipment_codes.split(",") if code.strip()]
        
        # Get templates
        templates = await checklist_service.list_checklist_templates(
            equipment_codes=equipment_list,
            enabled_only=enabled_only,
            skip=skip,
            limit=limit
        )
        
        logger.debug(
            "Checklist templates listed via API",
            count=len(templates),
            user_id=current_user.user_id
        )
        
        return templates
        
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to list checklist templates via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/templates/{template_id}", response_model=ChecklistTemplateResponse, status_code=status.HTTP_200_OK)
async def get_checklist_template(
    template_id: UUID,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> ChecklistTemplateResponse:
    """Get a checklist template by ID."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.CHECKLIST_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view checklist templates"
            )
        
        # Get template
        template = await checklist_service.get_checklist_template(template_id)
        
        logger.debug("Checklist template retrieved via API", template_id=template_id, user_id=current_user.user_id)
        
        return template
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to get checklist template via API", error=str(e), template_id=template_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/templates/{template_id}", response_model=ChecklistTemplateResponse, status_code=status.HTTP_200_OK)
async def update_checklist_template(
    template_id: UUID,
    update_data: ChecklistTemplateUpdate,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> ChecklistTemplateResponse:
    """Update a checklist template (admin/manager only)."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.CHECKLIST_WRITE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to update checklist templates"
            )
        
        # Update template
        template = await checklist_service.update_checklist_template(template_id, update_data)
        
        logger.info("Checklist template updated via API", template_id=template_id, user_id=current_user.user_id)
        
        return template
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to update checklist template via API", error=str(e), template_id=template_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/templates/for-equipment", response_model=Optional[ChecklistTemplateResponse], status_code=status.HTTP_200_OK)
async def get_checklist_template_for_equipment(
    equipment_codes: str = Query(..., description="Comma-separated equipment codes"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> Optional[ChecklistTemplateResponse]:
    """Get the most appropriate checklist template for given equipment codes."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.CHECKLIST_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view checklist templates"
            )
        
        # Parse equipment codes
        equipment_list = [code.strip() for code in equipment_codes.split(",") if code.strip()]
        if not equipment_list:
            raise HTTPException(status_code=400, detail="At least one equipment code is required")
        
        # Get template
        template = await checklist_service.get_checklist_template_for_equipment(equipment_list)
        
        logger.debug(
            "Checklist template for equipment retrieved via API", 
            equipment_codes=equipment_list, 
            user_id=current_user.user_id
        )
        
        return template
        
    except HTTPException:
        raise
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to get checklist template for equipment via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/complete", response_model=ChecklistCompletionResponse, status_code=status.HTTP_201_CREATED)
async def complete_checklist(
    completion_data: ChecklistCompletionCreate,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> ChecklistCompletionResponse:
    """Complete a pre-start checklist."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.CHECKLIST_COMPLETE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to complete checklists"
            )
        
        # Complete checklist
        completion = await checklist_service.complete_checklist(completion_data, current_user.user_id)
        
        logger.info(
            "Checklist completed via API", 
            completion_id=completion.id, 
            job_assignment_id=completion_data.job_assignment_id,
            user_id=current_user.user_id
        )
        
        return completion
        
    except (NotFoundError, ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to complete checklist via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/completions/{completion_id}", response_model=ChecklistCompletionResponse, status_code=status.HTTP_200_OK)
async def get_checklist_completion(
    completion_id: UUID,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> ChecklistCompletionResponse:
    """Get a checklist completion by ID."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.CHECKLIST_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view checklist completions"
            )
        
        # Get completion
        completion = await checklist_service.get_checklist_completion(completion_id)
        
        logger.debug("Checklist completion retrieved via API", completion_id=completion_id, user_id=current_user.user_id)
        
        return completion
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to get checklist completion via API", error=str(e), completion_id=completion_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/completions", response_model=List[ChecklistCompletionResponse], status_code=status.HTTP_200_OK)
async def list_checklist_completions(
    job_assignment_id: Optional[UUID] = Query(None, description="Filter by job assignment ID"),
    user_id: Optional[UUID] = Query(None, description="Filter by user ID"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> List[ChecklistCompletionResponse]:
    """List checklist completions with filters."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.CHECKLIST_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view checklist completions"
            )
        
        # Get completions
        completions = await checklist_service.list_checklist_completions(
            job_assignment_id=job_assignment_id,
            user_id=user_id,
            skip=skip,
            limit=limit
        )
        
        logger.debug(
            "Checklist completions listed via API",
            count=len(completions),
            user_id=current_user.user_id
        )
        
        return completions
        
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to list checklist completions via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")
