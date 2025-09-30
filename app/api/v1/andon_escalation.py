"""
MS5.0 Floor Dashboard - Andon Escalation API

This module provides API endpoints for Andon escalation management including
escalation creation, acknowledgment, resolution, and monitoring.
"""

from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.responses import JSONResponse

from app.auth.permissions import require_permission, Permission
from app.models.production import AndonPriority
from app.services.andon_escalation_service import AndonEscalationService
from app.utils.exceptions import (
    NotFoundError, ValidationError, BusinessLogicError, ConflictError
)

router = APIRouter(prefix="/andon/escalations", tags=["andon-escalation"])


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_escalation(
    event_id: UUID,
    priority: AndonPriority,
    acknowledgment_timeout_minutes: Optional[int] = None,
    resolution_timeout_minutes: Optional[int] = None,
    current_user: dict = Depends(require_permission(Permission.ANDON_MANAGE))
) -> JSONResponse:
    """Create a new escalation for an Andon event."""
    try:
        escalation = await AndonEscalationService.create_escalation(
            event_id=event_id,
            priority=priority,
            acknowledgment_timeout_minutes=acknowledgment_timeout_minutes,
            resolution_timeout_minutes=resolution_timeout_minutes
        )
        
        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={
                "message": "Escalation created successfully",
                "escalation": escalation
            }
        )
        
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.put("/{escalation_id}/acknowledge", status_code=status.HTTP_200_OK)
async def acknowledge_escalation(
    escalation_id: UUID,
    notes: Optional[str] = None,
    current_user: dict = Depends(require_permission(Permission.ANDON_MANAGE))
) -> JSONResponse:
    """Acknowledge an escalation."""
    try:
        escalation = await AndonEscalationService.acknowledge_escalation(
            escalation_id=escalation_id,
            acknowledged_by=current_user["id"],
            notes=notes
        )
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "Escalation acknowledged successfully",
                "escalation": escalation
            }
        )
        
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.put("/{escalation_id}/resolve", status_code=status.HTTP_200_OK)
async def resolve_escalation(
    escalation_id: UUID,
    resolution_notes: str,
    current_user: dict = Depends(require_permission(Permission.ANDON_MANAGE))
) -> JSONResponse:
    """Resolve an escalation."""
    try:
        escalation = await AndonEscalationService.resolve_escalation(
            escalation_id=escalation_id,
            resolved_by=current_user["id"],
            resolution_notes=resolution_notes
        )
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "Escalation resolved successfully",
                "escalation": escalation
            }
        )
        
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.post("/{escalation_id}/escalate", status_code=status.HTTP_200_OK)
async def escalate_manually(
    escalation_id: UUID,
    escalation_notes: str,
    target_level: Optional[int] = None,
    current_user: dict = Depends(require_permission(Permission.ANDON_MANAGE))
) -> JSONResponse:
    """Manually escalate to next level or specific level."""
    try:
        escalation = await AndonEscalationService.escalate_manually(
            escalation_id=escalation_id,
            escalated_by=current_user["id"],
            escalation_notes=escalation_notes,
            target_level=target_level
        )
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "Escalation escalated successfully",
                "escalation": escalation
            }
        )
        
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/active", status_code=status.HTTP_200_OK)
async def get_active_escalations(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    priority: Optional[AndonPriority] = Query(None, description="Filter by priority"),
    current_user: dict = Depends(require_permission(Permission.ANDON_VIEW))
) -> JSONResponse:
    """Get active escalations with filtering."""
    try:
        escalations = await AndonEscalationService.get_active_escalations(
            line_id=line_id,
            priority=priority
        )
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "escalations": escalations,
                "count": len(escalations)
            }
        )
        
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/{escalation_id}/history", status_code=status.HTTP_200_OK)
async def get_escalation_history(
    escalation_id: UUID,
    current_user: dict = Depends(require_permission(Permission.ANDON_VIEW))
) -> JSONResponse:
    """Get escalation history and timeline."""
    try:
        history = await AndonEscalationService.get_escalation_history(escalation_id)
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "escalation_id": str(escalation_id),
                "history": history,
                "count": len(history)
            }
        )
        
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/statistics", status_code=status.HTTP_200_OK)
async def get_escalation_statistics(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    start_date: Optional[datetime] = Query(None, description="Start date for statistics"),
    end_date: Optional[datetime] = Query(None, description="End date for statistics"),
    current_user: dict = Depends(require_permission(Permission.ANDON_VIEW))
) -> JSONResponse:
    """Get escalation statistics and analytics."""
    try:
        statistics = await AndonEscalationService.get_escalation_statistics(
            line_id=line_id,
            start_date=start_date,
            end_date=end_date
        )
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=statistics
        )
        
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.post("/process-automatic", status_code=status.HTTP_200_OK)
async def process_automatic_escalations(
    current_user: dict = Depends(require_permission(Permission.ANDON_MANAGE))
) -> JSONResponse:
    """Process automatic escalations based on timeouts."""
    try:
        processed_count = await AndonEscalationService.process_automatic_escalations()
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "Automatic escalations processed successfully",
                "processed_count": processed_count
            }
        )
        
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/escalation-tree", status_code=status.HTTP_200_OK)
async def get_escalation_tree(
    current_user: dict = Depends(require_permission(Permission.ANDON_VIEW))
) -> JSONResponse:
    """Get escalation tree configuration."""
    try:
        # This would typically come from a configuration service
        escalation_tree = {
            "levels": [
                {
                    "level": 1,
                    "name": "Initial Escalation",
                    "delay_minutes": 5,
                    "recipients": ["shift_supervisor", "line_operator"],
                    "notification_methods": ["websocket", "push"]
                },
                {
                    "level": 2,
                    "name": "Supervisor Escalation",
                    "delay_minutes": 15,
                    "recipients": ["shift_manager", "maintenance_engineer"],
                    "notification_methods": ["email", "sms", "websocket"]
                },
                {
                    "level": 3,
                    "name": "Management Escalation",
                    "delay_minutes": 30,
                    "recipients": ["production_manager", "maintenance_manager"],
                    "notification_methods": ["email", "sms", "phone"]
                },
                {
                    "level": 4,
                    "name": "Executive Escalation",
                    "delay_minutes": 60,
                    "recipients": ["plant_manager", "operations_director"],
                    "notification_methods": ["email", "sms", "phone"]
                }
            ],
            "priority_rules": {
                "low": {"max_level": 2, "escalation_interval": 15},
                "medium": {"max_level": 3, "escalation_interval": 10},
                "high": {"max_level": 4, "escalation_interval": 5},
                "critical": {"max_level": 4, "escalation_interval": 2}
            }
        }
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=escalation_tree
        )
        
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/monitoring/dashboard", status_code=status.HTTP_200_OK)
async def get_escalation_monitoring_dashboard(
    current_user: dict = Depends(require_permission(Permission.ANDON_VIEW))
) -> JSONResponse:
    """Get escalation monitoring dashboard data."""
    try:
        # Get active escalations
        active_escalations = await AndonEscalationService.get_active_escalations()
        
        # Get statistics for last 24 hours
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(hours=24)
        
        statistics = await AndonEscalationService.get_escalation_statistics(
            start_date=start_date,
            end_date=end_date
        )
        
        # Calculate dashboard metrics
        dashboard_data = {
            "active_escalations": {
                "total": len(active_escalations),
                "by_priority": {},
                "by_status": {},
                "overdue": 0
            },
            "recent_activity": {
                "escalations_created_24h": 0,
                "escalations_resolved_24h": 0,
                "avg_resolution_time_minutes": 0
            },
            "escalation_trends": {
                "escalation_rate_per_hour": 0,
                "resolution_rate_per_hour": 0,
                "escalation_level_distribution": {}
            }
        }
        
        # Process active escalations
        for escalation in active_escalations:
            priority = escalation["priority"]
            status = escalation["escalation_status"]
            
            # Count by priority
            if priority not in dashboard_data["active_escalations"]["by_priority"]:
                dashboard_data["active_escalations"]["by_priority"][priority] = 0
            dashboard_data["active_escalations"]["by_priority"][priority] += 1
            
            # Count by status
            if status not in dashboard_data["active_escalations"]["by_status"]:
                dashboard_data["active_escalations"]["by_status"][status] = 0
            dashboard_data["active_escalations"]["by_status"][status] += 1
            
            # Check if overdue
            if escalation["acknowledgment_time_remaining_minutes"] is not None:
                if escalation["acknowledgment_time_remaining_minutes"] <= 0:
                    dashboard_data["active_escalations"]["overdue"] += 1
        
        # Process statistics
        for priority, stats in statistics["priority_breakdown"].items():
            dashboard_data["recent_activity"]["escalations_created_24h"] += stats["total_escalations"]
            dashboard_data["recent_activity"]["escalations_resolved_24h"] += stats["resolved_escalations"]
            
            if stats["avg_resolution_time_minutes"] > 0:
                dashboard_data["recent_activity"]["avg_resolution_time_minutes"] = max(
                    dashboard_data["recent_activity"]["avg_resolution_time_minutes"],
                    stats["avg_resolution_time_minutes"]
                )
        
        # Calculate rates
        hours = 24
        dashboard_data["escalation_trends"]["escalation_rate_per_hour"] = round(
            dashboard_data["recent_activity"]["escalations_created_24h"] / hours, 2
        )
        dashboard_data["escalation_trends"]["resolution_rate_per_hour"] = round(
            dashboard_data["recent_activity"]["escalations_resolved_24h"] / hours, 2
        )
        
        # Add level distribution
        dashboard_data["escalation_trends"]["escalation_level_distribution"] = statistics["level_distribution"]
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=dashboard_data
        )
        
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")
