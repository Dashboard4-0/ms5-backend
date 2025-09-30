"""
MS5.0 Floor Dashboard - Downtime Management API

This module provides REST API endpoints for downtime event management,
including creation, retrieval, confirmation, and analytics.
"""

from datetime import date, datetime
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.permissions import Permission, get_current_user, require_permission
from app.database import get_db
from app.models.production import (
    DowntimeEventCreate, DowntimeEventUpdate, DowntimeEventResponse,
    DowntimeStatisticsResponse
)
from app.services.downtime_tracker import DowntimeTracker
from app.utils.exceptions import ValidationError, BusinessLogicError, NotFoundError
from app.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


@router.get("/events", response_model=List[DowntimeEventResponse])
async def get_downtime_events(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    equipment_code: Optional[str] = Query(None, description="Filter by equipment code"),
    start_date: Optional[date] = Query(None, description="Filter by start date"),
    end_date: Optional[date] = Query(None, description="Filter by end date"),
    category: Optional[str] = Query(None, description="Filter by category"),
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of events to return"),
    offset: int = Query(0, ge=0, description="Number of events to skip"),
    current_user = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get downtime events with filtering options."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.DOWNTIME_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view downtime events"
            )
        
        tracker = DowntimeTracker()
        events = await tracker.get_downtime_events(
            line_id=line_id,
            equipment_code=equipment_code,
            start_date=start_date,
            end_date=end_date,
            category=category,
            status=status,
            limit=limit,
            offset=offset
        )
        
        logger.info(
            "Downtime events retrieved",
            user_id=current_user.user_id,
            count=len(events),
            filters={
                "line_id": line_id,
                "equipment_code": equipment_code,
                "start_date": start_date,
                "end_date": end_date,
                "category": category,
                "status": status
            }
        )
        
        return events
        
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        logger.error("Failed to get downtime events", error=str(e), user_id=current_user.user_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/events/{event_id}", response_model=DowntimeEventResponse)
async def get_downtime_event(
    event_id: UUID,
    current_user = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get a specific downtime event by ID."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.DOWNTIME_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view downtime events"
            )
        
        tracker = DowntimeTracker()
        events = await tracker.get_downtime_events(limit=1)
        
        if not events:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Downtime event not found"
            )
        
        logger.info(
            "Downtime event retrieved",
            event_id=event_id,
            user_id=current_user.user_id
        )
        
        return events[0]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get downtime event", error=str(e), event_id=event_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.post("/events", response_model=DowntimeEventResponse, status_code=status.HTTP_201_CREATED)
async def create_downtime_event(
    event_data: DowntimeEventCreate,
    current_user = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Create a new downtime event."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.DOWNTIME_WRITE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to create downtime events"
            )
        
        # Create downtime event
        tracker = DowntimeTracker()
        
        # Convert to dict for internal processing
        event_dict = event_data.dict()
        event_dict["reported_by"] = current_user.user_id
        
        # Store event
        event_id = await tracker._store_downtime_event(event_dict)
        
        # Get created event
        events = await tracker.get_downtime_events(limit=1)
        if not events:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve created downtime event"
            )
        
        logger.info(
            "Downtime event created",
            event_id=event_id,
            line_id=event_data.line_id,
            equipment_code=event_data.equipment_code,
            user_id=current_user.user_id
        )
        
        return events[0]
        
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        logger.error("Failed to create downtime event", error=str(e), user_id=current_user.user_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.put("/events/{event_id}", response_model=DowntimeEventResponse)
async def update_downtime_event(
    event_id: UUID,
    event_data: DowntimeEventUpdate,
    current_user = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Update a downtime event."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.DOWNTIME_WRITE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to update downtime events"
            )
        
        # Update downtime event
        tracker = DowntimeTracker()
        
        # Get existing event
        events = await tracker.get_downtime_events(limit=1)
        if not events:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Downtime event not found"
            )
        
        # Update event in database
        update_data = event_data.dict(exclude_unset=True)
        if update_data:
            await tracker._update_downtime_event_in_db(event_id, **update_data)
        
        # Get updated event
        updated_events = await tracker.get_downtime_events(limit=1)
        if not updated_events:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Downtime event not found after update"
            )
        
        logger.info(
            "Downtime event updated",
            event_id=event_id,
            user_id=current_user.user_id
        )
        
        return updated_events[0]
        
    except HTTPException:
        raise
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        logger.error("Failed to update downtime event", error=str(e), event_id=event_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.post("/events/{event_id}/confirm", response_model=DowntimeEventResponse)
async def confirm_downtime_event(
    event_id: UUID,
    notes: Optional[str] = None,
    current_user = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Confirm a downtime event."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.DOWNTIME_CONFIRM):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to confirm downtime events"
            )
        
        tracker = DowntimeTracker()
        event = await tracker.confirm_downtime_event(
            event_id=event_id,
            confirmed_by=current_user.user_id,
            notes=notes
        )
        
        logger.info(
            "Downtime event confirmed",
            event_id=event_id,
            confirmed_by=current_user.user_id
        )
        
        return event
        
    except NotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Downtime event not found"
        )
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        logger.error("Failed to confirm downtime event", error=str(e), event_id=event_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/statistics", response_model=DowntimeStatisticsResponse)
async def get_downtime_statistics(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    start_date: Optional[date] = Query(None, description="Filter by start date"),
    end_date: Optional[date] = Query(None, description="Filter by end date"),
    current_user = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get downtime statistics and analysis."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.DOWNTIME_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view downtime statistics"
            )
        
        tracker = DowntimeTracker()
        statistics = await tracker.get_downtime_statistics(
            line_id=line_id,
            start_date=start_date,
            end_date=end_date
        )
        
        logger.info(
            "Downtime statistics retrieved",
            user_id=current_user.user_id,
            line_id=line_id,
            start_date=start_date,
            end_date=end_date
        )
        
        return DowntimeStatisticsResponse(**statistics)
        
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        logger.error("Failed to get downtime statistics", error=str(e), user_id=current_user.user_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/reasons")
async def get_downtime_reasons(
    current_user = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get available downtime reason codes and descriptions."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.DOWNTIME_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view downtime reasons"
            )
        
        tracker = DowntimeTracker()
        reason_codes = tracker._load_reason_codes()
        
        logger.info(
            "Downtime reasons retrieved",
            user_id=current_user.user_id,
            count=len(reason_codes)
        )
        
        return {
            "reason_codes": [
                {
                    "code": code,
                    "description": data["description"],
                    "category": data["category"]
                }
                for code, data in reason_codes.items()
            ]
        }
        
    except Exception as e:
        logger.error("Failed to get downtime reasons", error=str(e), user_id=current_user.user_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/active")
async def get_active_downtime_events(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    current_user = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get currently active downtime events."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.DOWNTIME_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view active downtime events"
            )
        
        tracker = DowntimeTracker()
        
        # Get active events from memory
        active_events = []
        for equipment_code, event_data in tracker.active_events.items():
            if line_id is None or event_data.get("line_id") == line_id:
                active_events.append({
                    "equipment_code": equipment_code,
                    "line_id": event_data.get("line_id"),
                    "start_time": event_data.get("start_time"),
                    "reason_code": event_data.get("reason_code"),
                    "reason_description": event_data.get("reason_description"),
                    "category": event_data.get("category"),
                    "duration_seconds": int((datetime.utcnow() - event_data.get("start_time", datetime.utcnow())).total_seconds()),
                    "fault_data": event_data.get("fault_data", {}),
                    "context_data": event_data.get("context_data", {})
                })
        
        logger.info(
            "Active downtime events retrieved",
            user_id=current_user.user_id,
            count=len(active_events),
            line_id=line_id
        )
        
        return {
            "active_events": active_events,
            "count": len(active_events)
        }
        
    except Exception as e:
        logger.error("Failed to get active downtime events", error=str(e), user_id=current_user.user_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.post("/detect")
async def detect_downtime_events(
    line_id: UUID,
    equipment_code: str,
    current_status: dict,
    timestamp: Optional[datetime] = None,
    current_user = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Detect downtime events from PLC data (internal API)."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.DOWNTIME_WRITE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to detect downtime events"
            )
        
        tracker = DowntimeTracker()
        event = await tracker.detect_downtime_event(
            line_id=line_id,
            equipment_code=equipment_code,
            current_status=current_status,
            timestamp=timestamp
        )
        
        logger.info(
            "Downtime detection completed",
            line_id=line_id,
            equipment_code=equipment_code,
            event_detected=event is not None,
            user_id=current_user.user_id
        )
        
        return {
            "event_detected": event is not None,
            "event": event
        }
        
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        logger.error("Failed to detect downtime events", error=str(e), user_id=current_user.user_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")
