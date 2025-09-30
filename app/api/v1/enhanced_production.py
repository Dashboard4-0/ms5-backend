"""
MS5.0 Floor Dashboard - Enhanced Production Management API Routes

This module provides enhanced API endpoints for production management operations
with PLC integration, including real-time production status, equipment production
context, and integrated production metrics.
"""

from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime, date, timedelta

from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.responses import JSONResponse
import structlog

from app.auth.permissions import get_current_user, UserContext, require_permission, Permission
from app.database import get_db
from app.services.equipment_job_mapper import EquipmentJobMapper
from app.services.plc_integrated_oee_calculator import PLCIntegratedOEECalculator
from app.services.plc_integrated_downtime_tracker import PLCIntegratedDowntimeTracker
from app.services.plc_integrated_andon_service import PLCIntegratedAndonService
from app.services.enhanced_telemetry_poller import EnhancedTelemetryPoller
from app.services.real_time_integration_service import RealTimeIntegrationService
from app.utils.exceptions import NotFoundError, ValidationError, BusinessLogicError
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger()

router = APIRouter()

# Initialize services
equipment_job_mapper = EquipmentJobMapper()
plc_oee_calculator = PLCIntegratedOEECalculator()
plc_downtime_tracker = PLCIntegratedDowntimeTracker()
plc_andon_service = PLCIntegratedAndonService()


@router.get("/equipment/{equipment_code}/production-status", status_code=status.HTTP_200_OK)
async def get_equipment_production_status(
    equipment_code: str,
    include_plc_data: bool = Query(True, description="Include PLC telemetry data"),
    include_oee: bool = Query(True, description="Include OEE calculations"),
    include_downtime: bool = Query(True, description="Include downtime information"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Get comprehensive production status for equipment with PLC integration."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.EQUIPMENT_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view equipment production status"
            )
        
        # Get current job assignment
        current_job = await equipment_job_mapper.get_current_job(equipment_code)
        
        # Get production context
        production_context = await equipment_job_mapper.get_equipment_production_context(equipment_code)
        
        # Initialize response
        status_data = {
            "equipment_code": equipment_code,
            "timestamp": datetime.utcnow().isoformat(),
            "production_status": {
                "current_job": current_job,
                "production_context": production_context,
                "status": "running" if current_job else "idle",
                "last_updated": datetime.utcnow().isoformat()
            }
        }
        
        # Add PLC data if requested
        if include_plc_data:
            try:
                # Get current PLC metrics (this would integrate with the enhanced telemetry poller)
                plc_metrics = await _get_current_plc_metrics(equipment_code)
                status_data["plc_data"] = plc_metrics
            except Exception as e:
                logger.warning("Failed to get PLC data for equipment", equipment_code=equipment_code, error=str(e))
                status_data["plc_data"] = {"error": "PLC data unavailable"}
        
        # Add OEE data if requested
        if include_oee and current_job:
            try:
                oee_data = await plc_oee_calculator.calculate_real_time_oee(
                    line_id=current_job.get("line_id"),
                    equipment_code=equipment_code,
                    current_metrics=status_data.get("plc_data", {})
                )
                status_data["oee"] = oee_data
            except Exception as e:
                logger.warning("Failed to calculate OEE for equipment", equipment_code=equipment_code, error=str(e))
                status_data["oee"] = {"error": "OEE calculation unavailable"}
        
        # Add downtime information if requested
        if include_downtime:
            try:
                downtime_data = await plc_downtime_tracker.get_current_downtime_status(equipment_code)
                status_data["downtime"] = downtime_data
            except Exception as e:
                logger.warning("Failed to get downtime data for equipment", equipment_code=equipment_code, error=str(e))
                status_data["downtime"] = {"error": "Downtime data unavailable"}
        
        logger.debug(
            "Equipment production status retrieved via API",
            equipment_code=equipment_code,
            user_id=current_user.user_id
        )
        
        return status_data
        
    except Exception as e:
        logger.error("Failed to get equipment production status via API", error=str(e), equipment_code=equipment_code)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/lines/{line_id}/real-time-oee", status_code=status.HTTP_200_OK)
async def get_real_time_oee(
    line_id: UUID,
    equipment_code: Optional[str] = Query(None, description="Specific equipment code"),
    include_trends: bool = Query(False, description="Include OEE trends"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Get real-time OEE for production line with PLC integration."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.OEE_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view real-time OEE"
            )
        
        # Get all equipment on the line
        if equipment_code:
            equipment_list = [equipment_code]
        else:
            equipment_list = await _get_line_equipment(line_id)
        
        oee_data = {
            "line_id": str(line_id),
            "timestamp": datetime.utcnow().isoformat(),
            "equipment_oee": {},
            "line_oee": {
                "oee": 0.0,
                "availability": 0.0,
                "performance": 0.0,
                "quality": 0.0
            }
        }
        
        total_oee = 0.0
        total_availability = 0.0
        total_performance = 0.0
        total_quality = 0.0
        equipment_count = 0
        
        # Calculate OEE for each equipment
        for eq_code in equipment_list:
            try:
                # Get current PLC metrics
                plc_metrics = await _get_current_plc_metrics(eq_code)
                
                # Calculate real-time OEE
                equipment_oee = await plc_oee_calculator.calculate_real_time_oee(
                    line_id=line_id,
                    equipment_code=eq_code,
                    current_metrics=plc_metrics
                )
                
                oee_data["equipment_oee"][eq_code] = equipment_oee
                
                # Accumulate for line-level OEE
                if equipment_oee.get("oee") is not None:
                    total_oee += equipment_oee["oee"]
                    total_availability += equipment_oee.get("availability", 0.0)
                    total_performance += equipment_oee.get("performance", 0.0)
                    total_quality += equipment_oee.get("quality", 0.0)
                    equipment_count += 1
                    
            except Exception as e:
                logger.warning("Failed to calculate OEE for equipment", equipment_code=eq_code, error=str(e))
                oee_data["equipment_oee"][eq_code] = {"error": "OEE calculation failed"}
        
        # Calculate line-level OEE
        if equipment_count > 0:
            oee_data["line_oee"] = {
                "oee": total_oee / equipment_count,
                "availability": total_availability / equipment_count,
                "performance": total_performance / equipment_count,
                "quality": total_quality / equipment_count
            }
        
        # Add trends if requested
        if include_trends:
            try:
                trends = await plc_oee_calculator.get_oee_trends_from_plc(
                    line_id=line_id,
                    days=7
                )
                oee_data["trends"] = trends
            except Exception as e:
                logger.warning("Failed to get OEE trends", line_id=line_id, error=str(e))
                oee_data["trends"] = {"error": "Trend data unavailable"}
        
        logger.debug(
            "Real-time OEE retrieved via API",
            line_id=line_id,
            equipment_count=len(equipment_list),
            user_id=current_user.user_id
        )
        
        return oee_data
        
    except Exception as e:
        logger.error("Failed to get real-time OEE via API", error=str(e), line_id=line_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/equipment/{equipment_code}/job-progress", status_code=status.HTTP_200_OK)
async def get_job_progress(
    equipment_code: str,
    include_plc_metrics: bool = Query(True, description="Include PLC production metrics"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Get current job progress for equipment with PLC integration."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.PRODUCTION_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view job progress"
            )
        
        # Get current job
        current_job = await equipment_job_mapper.get_current_job(equipment_code)
        
        if not current_job:
            return {
                "equipment_code": equipment_code,
                "status": "no_active_job",
                "message": "No active job found for this equipment"
            }
        
        # Get job progress
        progress_data = await equipment_job_mapper.get_job_progress(equipment_code, current_job["id"])
        
        # Add PLC metrics if requested
        if include_plc_metrics:
            try:
                plc_metrics = await _get_current_plc_metrics(equipment_code)
                progress_data["plc_metrics"] = plc_metrics
                
                # Calculate progress based on PLC data
                if plc_metrics.get("product_count") and current_job.get("target_quantity"):
                    actual_quantity = plc_metrics["product_count"]
                    target_quantity = current_job["target_quantity"]
                    progress_data["progress_percentage"] = (actual_quantity / target_quantity) * 100
                    
            except Exception as e:
                logger.warning("Failed to get PLC metrics for job progress", equipment_code=equipment_code, error=str(e))
                progress_data["plc_metrics"] = {"error": "PLC data unavailable"}
        
        logger.debug(
            "Job progress retrieved via API",
            equipment_code=equipment_code,
            job_id=current_job["id"],
            user_id=current_user.user_id
        )
        
        return progress_data
        
    except Exception as e:
        logger.error("Failed to get job progress via API", error=str(e), equipment_code=equipment_code)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/equipment/{equipment_code}/job-assignment", status_code=status.HTTP_201_CREATED)
async def assign_job_to_equipment(
    equipment_code: str,
    job_id: UUID,
    assign_reason: str = Query(..., description="Reason for job assignment"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Assign a job to equipment with production context integration."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.JOB_ASSIGN):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to assign jobs"
            )
        
        # Assign job to equipment
        assignment_result = await equipment_job_mapper.assign_job_to_equipment(
            equipment_code=equipment_code,
            job_id=job_id,
            assigned_by=UUID(current_user.user_id),
            assign_reason=assign_reason
        )
        
        logger.info(
            "Job assigned to equipment via API",
            equipment_code=equipment_code,
            job_id=job_id,
            user_id=current_user.user_id
        )
        
        return {
            "message": "Job assigned successfully",
            "assignment": assignment_result,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to assign job via API", error=str(e), equipment_code=equipment_code)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/equipment/{equipment_code}/job-completion", status_code=status.HTTP_200_OK)
async def complete_job_on_equipment(
    equipment_code: str,
    completion_notes: Optional[str] = Query(None, description="Job completion notes"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Complete current job on equipment with PLC data integration."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.JOB_COMPLETE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to complete jobs"
            )
        
        # Get current job
        current_job = await equipment_job_mapper.get_current_job(equipment_code)
        
        if not current_job:
            raise HTTPException(
                status_code=404,
                detail="No active job found for this equipment"
            )
        
        # Get final PLC metrics
        final_plc_metrics = await _get_current_plc_metrics(equipment_code)
        
        # Complete job with PLC data
        completion_result = await equipment_job_mapper.complete_job_on_equipment(
            equipment_code=equipment_code,
            completed_by=UUID(current_user.user_id),
            completion_notes=completion_notes,
            final_metrics=final_plc_metrics
        )
        
        logger.info(
            "Job completed via API",
            equipment_code=equipment_code,
            job_id=current_job["id"],
            user_id=current_user.user_id
        )
        
        return {
            "message": "Job completed successfully",
            "completion": completion_result,
            "final_metrics": final_plc_metrics,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to complete job via API", error=str(e), equipment_code=equipment_code)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/lines/{line_id}/production-metrics", status_code=status.HTTP_200_OK)
async def get_line_production_metrics(
    line_id: UUID,
    time_period_hours: int = Query(24, ge=1, le=168, description="Time period in hours"),
    include_equipment_breakdown: bool = Query(True, description="Include equipment-level breakdown"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Get production metrics for a line with PLC integration."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.PRODUCTION_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view production metrics"
            )
        
        # Get line equipment
        equipment_list = await _get_line_equipment(line_id)
        
        metrics_data = {
            "line_id": str(line_id),
            "time_period_hours": time_period_hours,
            "timestamp": datetime.utcnow().isoformat(),
            "line_metrics": {
                "total_production": 0,
                "target_production": 0,
                "efficiency": 0.0,
                "quality_rate": 0.0,
                "oee": 0.0,
                "downtime_hours": 0.0,
                "active_andon_events": 0
            },
            "equipment_metrics": {} if include_equipment_breakdown else None
        }
        
        total_production = 0
        total_target = 0
        total_downtime = 0.0
        active_andon_count = 0
        
        # Get metrics for each equipment
        for eq_code in equipment_list:
            try:
                # Get PLC-based metrics
                equipment_metrics = await _get_equipment_production_metrics(
                    eq_code, time_period_hours
                )
                
                if include_equipment_breakdown:
                    metrics_data["equipment_metrics"][eq_code] = equipment_metrics
                
                # Accumulate line-level metrics
                total_production += equipment_metrics.get("total_production", 0)
                total_target += equipment_metrics.get("target_production", 0)
                total_downtime += equipment_metrics.get("downtime_hours", 0.0)
                active_andon_count += equipment_metrics.get("active_andon_events", 0)
                
            except Exception as e:
                logger.warning("Failed to get metrics for equipment", equipment_code=eq_code, error=str(e))
                if include_equipment_breakdown:
                    metrics_data["equipment_metrics"][eq_code] = {"error": "Metrics unavailable"}
        
        # Calculate line-level metrics
        metrics_data["line_metrics"] = {
            "total_production": total_production,
            "target_production": total_target,
            "efficiency": (total_production / total_target * 100) if total_target > 0 else 0.0,
            "quality_rate": 0.0,  # Would be calculated from quality data
            "oee": 0.0,  # Would be calculated from OEE data
            "downtime_hours": total_downtime,
            "active_andon_events": active_andon_count
        }
        
        logger.debug(
            "Line production metrics retrieved via API",
            line_id=line_id,
            equipment_count=len(equipment_list),
            user_id=current_user.user_id
        )
        
        return metrics_data
        
    except Exception as e:
        logger.error("Failed to get line production metrics via API", error=str(e), line_id=line_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/equipment/{equipment_code}/downtime-status", status_code=status.HTTP_200_OK)
async def get_equipment_downtime_status(
    equipment_code: str,
    include_current_downtime: bool = Query(True, description="Include current downtime event"),
    include_downtime_history: bool = Query(True, description="Include recent downtime history"),
    history_hours: int = Query(24, ge=1, le=168, description="Hours of downtime history"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Get downtime status for equipment with PLC integration."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.EQUIPMENT_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view downtime status"
            )
        
        downtime_data = {
            "equipment_code": equipment_code,
            "timestamp": datetime.utcnow().isoformat(),
            "current_downtime": None,
            "downtime_history": [],
            "downtime_statistics": {
                "total_downtime_hours": 0.0,
                "downtime_events": 0,
                "average_downtime_duration": 0.0,
                "downtime_categories": {}
            }
        }
        
        # Get current downtime if requested
        if include_current_downtime:
            try:
                current_downtime = await plc_downtime_tracker.get_current_downtime_status(equipment_code)
                downtime_data["current_downtime"] = current_downtime
            except Exception as e:
                logger.warning("Failed to get current downtime", equipment_code=equipment_code, error=str(e))
        
        # Get downtime history if requested
        if include_downtime_history:
            try:
                downtime_history = await plc_downtime_tracker.get_downtime_history(
                    equipment_code=equipment_code,
                    hours=history_hours
                )
                downtime_data["downtime_history"] = downtime_history
                
                # Calculate statistics
                if downtime_history:
                    total_hours = sum(event.get("duration_hours", 0) for event in downtime_history)
                    downtime_data["downtime_statistics"] = {
                        "total_downtime_hours": total_hours,
                        "downtime_events": len(downtime_history),
                        "average_downtime_duration": total_hours / len(downtime_history) if downtime_history else 0.0,
                        "downtime_categories": _categorize_downtime_events(downtime_history)
                    }
                    
            except Exception as e:
                logger.warning("Failed to get downtime history", equipment_code=equipment_code, error=str(e))
        
        logger.debug(
            "Equipment downtime status retrieved via API",
            equipment_code=equipment_code,
            user_id=current_user.user_id
        )
        
        return downtime_data
        
    except Exception as e:
        logger.error("Failed to get equipment downtime status via API", error=str(e), equipment_code=equipment_code)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/lines/{line_id}/andon-status", status_code=status.HTTP_200_OK)
async def get_line_andon_status(
    line_id: UUID,
    include_active_events: bool = Query(True, description="Include active Andon events"),
    include_recent_events: bool = Query(True, description="Include recent Andon events"),
    recent_hours: int = Query(24, ge=1, le=168, description="Hours of recent events"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Get Andon status for a production line with PLC integration."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.ANDON_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view Andon status"
            )
        
        andon_data = {
            "line_id": str(line_id),
            "timestamp": datetime.utcnow().isoformat(),
            "active_events": [],
            "recent_events": [],
            "andon_statistics": {
                "total_events": 0,
                "active_events": 0,
                "resolved_events": 0,
                "event_categories": {},
                "average_resolution_time": 0.0
            }
        }
        
        # Get active Andon events if requested
        if include_active_events:
            try:
                active_events = await plc_andon_service.get_active_andon_events(line_id)
                andon_data["active_events"] = active_events
                andon_data["andon_statistics"]["active_events"] = len(active_events)
            except Exception as e:
                logger.warning("Failed to get active Andon events", line_id=line_id, error=str(e))
        
        # Get recent Andon events if requested
        if include_recent_events:
            try:
                recent_events = await plc_andon_service.get_recent_andon_events(
                    line_id=line_id,
                    hours=recent_hours
                )
                andon_data["recent_events"] = recent_events
                
                # Calculate statistics
                if recent_events:
                    andon_data["andon_statistics"]["total_events"] = len(recent_events)
                    andon_data["andon_statistics"]["resolved_events"] = len([
                        event for event in recent_events if event.get("status") == "resolved"
                    ])
                    andon_data["andon_statistics"]["event_categories"] = _categorize_andon_events(recent_events)
                    
            except Exception as e:
                logger.warning("Failed to get recent Andon events", line_id=line_id, error=str(e))
        
        logger.debug(
            "Line Andon status retrieved via API",
            line_id=line_id,
            user_id=current_user.user_id
        )
        
        return andon_data
        
    except Exception as e:
        logger.error("Failed to get line Andon status via API", error=str(e), line_id=line_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/equipment/{equipment_code}/trigger-andon", status_code=status.HTTP_201_CREATED)
async def trigger_andon_event(
    equipment_code: str,
    event_type: str,
    priority: str,
    description: str,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Manually trigger an Andon event for equipment."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.ANDON_CREATE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to create Andon events"
            )
        
        # Validate event type and priority
        valid_event_types = ["maintenance", "quality", "safety", "material", "changeover"]
        valid_priorities = ["low", "medium", "high", "critical"]
        
        if event_type not in valid_event_types:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid event type. Must be one of: {valid_event_types}"
            )
        
        if priority not in valid_priorities:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid priority. Must be one of: {valid_priorities}"
            )
        
        # Get line ID for equipment
        line_id = await _get_equipment_line_id(equipment_code)
        
        # Create Andon event
        andon_event = await plc_andon_service.create_andon_event(
            line_id=line_id,
            equipment_code=equipment_code,
            event_type=event_type,
            priority=priority,
            description=description,
            created_by=UUID(current_user.user_id),
            auto_generated=False
        )
        
        logger.info(
            "Manual Andon event created via API",
            equipment_code=equipment_code,
            event_type=event_type,
            priority=priority,
            user_id=current_user.user_id
        )
        
        return {
            "message": "Andon event created successfully",
            "andon_event": andon_event,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to trigger Andon event via API", error=str(e), equipment_code=equipment_code)
        raise HTTPException(status_code=500, detail="Internal server error")


# Helper functions
async def _get_current_plc_metrics(equipment_code: str) -> Dict[str, Any]:
    """Get current PLC metrics for equipment."""
    # This would integrate with the enhanced telemetry poller
    # For now, return mock data
    return {
        "equipment_code": equipment_code,
        "timestamp": datetime.utcnow().isoformat(),
        "running_status": True,
        "product_count": 0,
        "speed": 0.0,
        "temperature": 0.0,
        "pressure": 0.0,
        "has_faults": False,
        "active_alarms": []
    }


async def _get_line_equipment(line_id: UUID) -> List[str]:
    """Get list of equipment codes for a production line."""
    # This would query the equipment line mapping table
    # For now, return mock data
    return ["BP01.PACK.BAG1", "BP01.PACK.BAG1.BL"]


async def _get_equipment_production_metrics(equipment_code: str, hours: int) -> Dict[str, Any]:
    """Get production metrics for equipment over a time period."""
    # This would integrate with PLC historical data
    # For now, return mock data
    return {
        "equipment_code": equipment_code,
        "total_production": 0,
        "target_production": 0,
        "downtime_hours": 0.0,
        "active_andon_events": 0
    }


async def _get_equipment_line_id(equipment_code: str) -> UUID:
    """Get production line ID for equipment."""
    # This would query the equipment line mapping table
    # For now, return a mock UUID
    return UUID("12345678-1234-5678-9abc-123456789012")


def _categorize_downtime_events(downtime_history: List[Dict]) -> Dict[str, int]:
    """Categorize downtime events by reason."""
    categories = {}
    for event in downtime_history:
        category = event.get("category", "unknown")
        categories[category] = categories.get(category, 0) + 1
    return categories


def _categorize_andon_events(andon_events: List[Dict]) -> Dict[str, int]:
    """Categorize Andon events by type."""
    categories = {}
    for event in andon_events:
        event_type = event.get("event_type", "unknown")
        categories[event_type] = categories.get(event_type, 0) + 1
    return categories
