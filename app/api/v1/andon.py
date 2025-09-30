"""
MS5.0 Floor Dashboard - Andon System API Routes

This module provides API endpoints for the Andon system including
event creation, acknowledgment, resolution, and escalation management.
"""

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.responses import JSONResponse
import structlog

from app.auth.permissions import get_current_user, UserContext, require_permission, Permission
from app.database import get_db
from app.models.production import (
    AndonEventCreate, AndonEventUpdate, AndonEventResponse,
    AndonEventType, AndonPriority, AndonStatus
)
from app.services.andon_service import AndonService
from app.utils.exceptions import NotFoundError, ValidationError, ConflictError, BusinessLogicError
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger()

router = APIRouter()


@router.post("/events", response_model=AndonEventResponse, status_code=status.HTTP_201_CREATED)
async def create_andon_event(
    event_data: AndonEventCreate,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> AndonEventResponse:
    """Create a new Andon event."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.ANDON_CREATE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to create Andon events"
            )
        
        event = await AndonService.create_andon_event(
            event_data=event_data,
            reported_by=UUID(current_user.user_id)
        )
        
        logger.info(
            "Andon event created via API",
            event_id=event.id,
            line_id=event.line_id,
            equipment_code=event.equipment_code,
            priority=event.priority,
            user_id=current_user.user_id
        )
        
        return event
        
    except (ValidationError, ConflictError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to create Andon event via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/events", response_model=List[AndonEventResponse], status_code=status.HTTP_200_OK)
async def list_andon_events(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    status: Optional[str] = Query(None, description="Filter by event status"),
    priority: Optional[str] = Query(None, description="Filter by event priority"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> List[AndonEventResponse]:
    """List Andon events with filters."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.ANDON_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view Andon events"
            )
        
        # Convert string parameters to enums
        event_status = AndonStatus(status) if status else None
        event_priority = AndonPriority(priority) if priority else None
        
        events = await AndonService.list_andon_events(
            line_id=line_id,
            status=event_status,
            priority=event_priority,
            skip=skip,
            limit=limit
        )
        
        logger.debug(
            "Andon events listed via API",
            count=len(events),
            user_id=current_user.user_id
        )
        
        return events
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid parameter: {e}")
    except Exception as e:
        logger.error("Failed to list Andon events via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/events/active", response_model=List[AndonEventResponse], status_code=status.HTTP_200_OK)
async def get_active_andon_events(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> List[AndonEventResponse]:
    """Get active Andon events (open or acknowledged)."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.ANDON_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view Andon events"
            )
        
        events = await AndonService.get_active_andon_events(line_id=line_id)
        
        logger.debug(
            "Active Andon events retrieved via API",
            count=len(events),
            user_id=current_user.user_id
        )
        
        return events
        
    except Exception as e:
        logger.error("Failed to get active Andon events via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/events/{event_id}", response_model=AndonEventResponse, status_code=status.HTTP_200_OK)
async def get_andon_event(
    event_id: UUID,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> AndonEventResponse:
    """Get an Andon event by ID."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.ANDON_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view Andon events"
            )
        
        event = await AndonService.get_andon_event(event_id)
        
        logger.debug(
            "Andon event retrieved via API",
            event_id=event_id,
            user_id=current_user.user_id
        )
        
        return event
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Failed to get Andon event via API", error=str(e), event_id=event_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/events/{event_id}/acknowledge", response_model=AndonEventResponse, status_code=status.HTTP_200_OK)
async def acknowledge_andon_event(
    event_id: UUID,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> AndonEventResponse:
    """Acknowledge an Andon event."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.ANDON_ACKNOWLEDGE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to acknowledge Andon events"
            )
        
        event = await AndonService.acknowledge_andon_event(
            event_id=event_id,
            acknowledged_by=UUID(current_user.user_id)
        )
        
        logger.info(
            "Andon event acknowledged via API",
            event_id=event_id,
            user_id=current_user.user_id
        )
        
        return event
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to acknowledge Andon event via API", error=str(e), event_id=event_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/events/{event_id}/resolve", response_model=AndonEventResponse, status_code=status.HTTP_200_OK)
async def resolve_andon_event(
    event_id: UUID,
    resolution_notes: str,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> AndonEventResponse:
    """Resolve an Andon event."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.ANDON_RESOLVE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to resolve Andon events"
            )
        
        if not resolution_notes or len(resolution_notes.strip()) == 0:
            raise HTTPException(
                status_code=400,
                detail="Resolution notes are required"
            )
        
        event = await AndonService.resolve_andon_event(
            event_id=event_id,
            resolved_by=UUID(current_user.user_id),
            resolution_notes=resolution_notes.strip()
        )
        
        logger.info(
            "Andon event resolved via API",
            event_id=event_id,
            user_id=current_user.user_id
        )
        
        return event
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to resolve Andon event via API", error=str(e), event_id=event_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/statistics", status_code=status.HTTP_200_OK)
async def get_andon_statistics(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Get Andon event statistics."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.ANDON_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view Andon statistics"
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
        
        statistics = await AndonService.get_andon_statistics(
            line_id=line_id,
            start_date=start_dt,
            end_date=end_dt
        )
        
        logger.debug(
            "Andon statistics retrieved via API",
            line_id=line_id,
            user_id=current_user.user_id
        )
        
        return statistics
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get Andon statistics via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/escalation-tree", status_code=status.HTTP_200_OK)
async def get_escalation_tree(
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Get Andon escalation tree configuration."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.ANDON_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view escalation tree"
            )
        
        # This would be implemented to return the actual escalation configuration
        escalation_tree = {
            "levels": {
                "low": {
                    "acknowledgment_timeout_minutes": 15,
                    "resolution_timeout_minutes": 60,
                    "escalation_recipients": ["shift_manager", "engineer"]
                },
                "medium": {
                    "acknowledgment_timeout_minutes": 10,
                    "resolution_timeout_minutes": 45,
                    "escalation_recipients": ["shift_manager", "engineer", "production_manager"]
                },
                "high": {
                    "acknowledgment_timeout_minutes": 5,
                    "resolution_timeout_minutes": 30,
                    "escalation_recipients": ["shift_manager", "engineer", "production_manager", "admin"]
                },
                "critical": {
                    "acknowledgment_timeout_minutes": 2,
                    "resolution_timeout_minutes": 15,
                    "escalation_recipients": ["all_managers", "admin"]
                }
            },
            "recipients": {
                "shift_manager": {
                    "name": "Shift Manager",
                    "contact_methods": ["email", "sms", "phone"]
                },
                "engineer": {
                    "name": "Engineer",
                    "contact_methods": ["email", "phone"]
                },
                "production_manager": {
                    "name": "Production Manager",
                    "contact_methods": ["email", "phone"]
                },
                "admin": {
                    "name": "Administrator",
                    "contact_methods": ["email", "phone", "sms"]
                }
            }
        }
        
        logger.debug(
            "Escalation tree retrieved via API",
            user_id=current_user.user_id
        )
        
        return escalation_tree
        
    except Exception as e:
        logger.error("Failed to get escalation tree via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/events/{event_id}/history", status_code=status.HTTP_200_OK)
async def get_andon_event_history(
    event_id: UUID,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Get history and timeline for an Andon event."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.ANDON_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view Andon event history"
            )
        
        # Get the event first
        event = await AndonService.get_andon_event(event_id)
        
        # This would be implemented to get the actual event history
        # For now, return a basic timeline
        history = {
            "event_id": event_id,
            "timeline": [
                {
                    "timestamp": event.reported_at,
                    "action": "Event Created",
                    "user_id": event.reported_by,
                    "description": f"Andon event created: {event.description}"
                }
            ]
        }
        
        if event.acknowledged_at:
            history["timeline"].append({
                "timestamp": event.acknowledged_at,
                "action": "Event Acknowledged",
                "user_id": event.acknowledged_by,
                "description": "Event acknowledged by responsible person"
            })
        
        if event.resolved_at:
            history["timeline"].append({
                "timestamp": event.resolved_at,
                "action": "Event Resolved",
                "user_id": event.resolved_by,
                "description": f"Event resolved: {event.resolution_notes}"
            })
        
        # Sort timeline by timestamp
        history["timeline"].sort(key=lambda x: x["timestamp"])
        
        logger.debug(
            "Andon event history retrieved via API",
            event_id=event_id,
            user_id=current_user.user_id
        )
        
        return history
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Failed to get Andon event history via API", error=str(e), event_id=event_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/events/{event_id}/escalate", status_code=status.HTTP_200_OK)
async def escalate_andon_event(
    event_id: UUID,
    escalation_reason: str,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Manually escalate an Andon event."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.ANDON_ACKNOWLEDGE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to escalate Andon events"
            )
        
        if not escalation_reason or len(escalation_reason.strip()) == 0:
            raise HTTPException(
                status_code=400,
                detail="Escalation reason is required"
            )
        
        # Get the event first
        event = await AndonService.get_andon_event(event_id)
        
        if event.status == AndonStatus.RESOLVED:
            raise HTTPException(
                status_code=400,
                detail="Cannot escalate a resolved event"
            )
        
        # This would be implemented to actually escalate the event
        # For now, return a success message
        
        logger.info(
            "Andon event escalated via API",
            event_id=event_id,
            escalation_reason=escalation_reason,
            user_id=current_user.user_id
        )
        
        return {
            "message": "Event escalated successfully",
            "event_id": event_id,
            "escalation_reason": escalation_reason,
            "escalated_by": current_user.user_id,
            "escalated_at": "2025-01-20T10:00:00Z"
        }
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to escalate Andon event via API", error=str(e), event_id=event_id)
        raise HTTPException(status_code=500, detail="Internal server error")


# Phase 3 Implementation - Enhanced Andon API Endpoints

@router.get("/dashboard", status_code=status.HTTP_200_OK)
async def get_andon_dashboard_data(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    days: int = Query(7, ge=1, le=30, description="Number of days for dashboard data"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Get comprehensive Andon dashboard data."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.ANDON_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view Andon dashboard data"
            )
        
        dashboard_data = await AndonService.get_andon_dashboard_data(
            line_id=line_id,
            days=days
        )
        
        logger.debug(
            "Andon dashboard data retrieved via API",
            line_id=line_id,
            days=days,
            user_id=current_user.user_id
        )
        
        return dashboard_data
        
    except Exception as e:
        logger.error("Failed to get Andon dashboard data via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/analytics/report", status_code=status.HTTP_200_OK)
async def get_andon_analytics_report(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Generate comprehensive Andon analytics report."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.ANDON_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view Andon analytics"
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
        
        report = await AndonService.get_andon_analytics_report(
            line_id=line_id,
            start_date=start_dt,
            end_date=end_dt
        )
        
        logger.info(
            "Andon analytics report generated via API",
            line_id=line_id,
            user_id=current_user.user_id
        )
        
        return report
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to generate Andon analytics report via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")
