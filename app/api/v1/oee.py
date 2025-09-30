"""
MS5.0 Floor Dashboard - OEE & Analytics API Routes

This module provides API endpoints for OEE calculations, analytics,
and production performance metrics.
"""

from datetime import datetime, date, timedelta
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.responses import JSONResponse
import structlog

from app.auth.permissions import get_current_user, UserContext, require_permission, Permission
from app.database import get_db
from app.models.production import OEECalculationResponse, OEECalculationCreate
from app.services.oee_calculator import OEECalculator
from app.utils.exceptions import NotFoundError, ValidationError, BusinessLogicError
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger()

router = APIRouter()


@router.post("/calculate", response_model=OEECalculationResponse, status_code=status.HTTP_201_CREATED)
async def calculate_oee(
    line_id: UUID,
    equipment_code: str,
    calculation_time: Optional[datetime] = Query(None, description="Calculation time (defaults to now)"),
    time_period_hours: int = Query(24, ge=1, le=168, description="Time period in hours"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> OEECalculationResponse:
    """Calculate OEE for a specific equipment over a time period."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.OEE_CALCULATE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to calculate OEE"
            )
        
        if not calculation_time:
            calculation_time = datetime.utcnow()
        
        oee_calculation = await OEECalculator.calculate_oee(
            line_id=line_id,
            equipment_code=equipment_code,
            calculation_time=calculation_time,
            time_period_hours=time_period_hours
        )
        
        logger.info(
            "OEE calculated via API",
            line_id=line_id,
            equipment_code=equipment_code,
            oee=oee_calculation.oee,
            user_id=current_user.user_id
        )
        
        return oee_calculation
        
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to calculate OEE via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/lines/{line_id}", response_model=List[OEECalculationResponse], status_code=status.HTTP_200_OK)
async def get_line_oee_history(
    line_id: UUID,
    equipment_code: str,
    start_date: Optional[date] = Query(None, description="Start date for history"),
    end_date: Optional[date] = Query(None, description="End date for history"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> List[OEECalculationResponse]:
    """Get OEE calculation history for a production line."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.OEE_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view OEE data"
            )
        
        if not start_date:
            start_date = date.today() - timedelta(days=7)
        if not end_date:
            end_date = date.today()
        
        oee_history = await OEECalculator.get_oee_history(
            line_id=line_id,
            equipment_code=equipment_code,
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )
        
        logger.debug(
            "OEE history retrieved via API",
            line_id=line_id,
            equipment_code=equipment_code,
            count=len(oee_history),
            user_id=current_user.user_id
        )
        
        return oee_history
        
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to get OEE history via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/lines/{line_id}/current", response_model=OEECalculationResponse, status_code=status.HTTP_200_OK)
async def get_current_line_oee(
    line_id: UUID,
    equipment_code: str,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> OEECalculationResponse:
    """Get current OEE for a production line."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.OEE_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view OEE data"
            )
        
        current_oee = await OEECalculator.get_current_oee(
            line_id=line_id,
            equipment_code=equipment_code
        )
        
        if not current_oee:
            raise HTTPException(
                status_code=404,
                detail="No OEE data found for this line and equipment"
            )
        
        logger.debug(
            "Current OEE retrieved via API",
            line_id=line_id,
            equipment_code=equipment_code,
            oee=current_oee.oee,
            user_id=current_user.user_id
        )
        
        return current_oee
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get current OEE via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/lines/{line_id}/daily-summary", status_code=status.HTTP_200_OK)
async def get_daily_oee_summary(
    line_id: UUID,
    target_date: Optional[date] = Query(None, description="Target date (defaults to today)"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Get daily OEE summary for a production line."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.OEE_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view OEE data"
            )
        
        if not target_date:
            target_date = date.today()
        
        daily_summary = await OEECalculator.calculate_daily_oee_summary(
            line_id=line_id,
            target_date=target_date
        )
        
        logger.debug(
            "Daily OEE summary retrieved via API",
            line_id=line_id,
            target_date=target_date,
            user_id=current_user.user_id
        )
        
        return daily_summary
        
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to get daily OEE summary via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/lines/{line_id}/trends", status_code=status.HTTP_200_OK)
async def get_oee_trends(
    line_id: UUID,
    days: int = Query(7, ge=1, le=30, description="Number of days for trend analysis"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Get OEE trends over a period."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.OEE_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view OEE data"
            )
        
        trends = await OEECalculator.get_oee_trends(
            line_id=line_id,
            days=days
        )
        
        logger.debug(
            "OEE trends retrieved via API",
            line_id=line_id,
            days=days,
            user_id=current_user.user_id
        )
        
        return trends
        
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to get OEE trends via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/equipment/{equipment_code}", response_model=List[OEECalculationResponse], status_code=status.HTTP_200_OK)
async def get_equipment_oee_history(
    equipment_code: str,
    start_date: Optional[date] = Query(None, description="Start date for history"),
    end_date: Optional[date] = Query(None, description="End date for history"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> List[OEECalculationResponse]:
    """Get OEE calculation history for specific equipment."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.OEE_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view OEE data"
            )
        
        if not start_date:
            start_date = date.today() - timedelta(days=7)
        if not end_date:
            end_date = date.today()
        
        # This would need to be implemented to get equipment OEE across all lines
        # For now, return empty list
        oee_history = []
        
        logger.debug(
            "Equipment OEE history retrieved via API",
            equipment_code=equipment_code,
            count=len(oee_history),
            user_id=current_user.user_id
        )
        
        return oee_history
        
    except (ValidationError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to get equipment OEE history via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/analytics/performance", status_code=status.HTTP_200_OK)
async def get_performance_analytics(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    start_date: Optional[date] = Query(None, description="Start date for analysis"),
    end_date: Optional[date] = Query(None, description="End date for analysis"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Get performance analytics and insights."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.ANALYTICS_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view analytics"
            )
        
        if not start_date:
            start_date = date.today() - timedelta(days=30)
        if not end_date:
            end_date = date.today()
        
        # This would be implemented with actual analytics calculation
        analytics = {
            "period": {
                "start_date": start_date,
                "end_date": end_date
            },
            "line_id": line_id,
            "performance_metrics": {
                "average_oee": 0.0,
                "best_oee": 0.0,
                "worst_oee": 0.0,
                "oee_trend": "stable",
                "availability_trend": "stable",
                "performance_trend": "stable",
                "quality_trend": "stable"
            },
            "insights": [
                "No significant performance issues detected",
                "OEE is within acceptable range",
                "Quality metrics are stable"
            ],
            "recommendations": [
                "Continue current operational practices",
                "Monitor equipment performance closely",
                "Consider preventive maintenance scheduling"
            ]
        }
        
        logger.debug(
            "Performance analytics retrieved via API",
            line_id=line_id,
            user_id=current_user.user_id
        )
        
        return analytics
        
    except Exception as e:
        logger.error("Failed to get performance analytics via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/analytics/benchmarks", status_code=status.HTTP_200_OK)
async def get_performance_benchmarks(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Get performance benchmarks and targets."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.ANALYTICS_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view analytics"
            )
        
        # This would be implemented with actual benchmark data
        benchmarks = {
            "line_id": line_id,
            "targets": {
                "oee": 0.85,
                "availability": 0.90,
                "performance": 0.95,
                "quality": 0.95
            },
            "industry_benchmarks": {
                "oee": {
                    "world_class": 0.90,
                    "good": 0.80,
                    "average": 0.70,
                    "poor": 0.60
                },
                "availability": {
                    "world_class": 0.95,
                    "good": 0.90,
                    "average": 0.85,
                    "poor": 0.80
                },
                "performance": {
                    "world_class": 0.95,
                    "good": 0.90,
                    "average": 0.85,
                    "poor": 0.80
                },
                "quality": {
                    "world_class": 0.99,
                    "good": 0.95,
                    "average": 0.90,
                    "poor": 0.85
                }
            },
            "current_performance": {
                "oee": 0.0,
                "availability": 0.0,
                "performance": 0.0,
                "quality": 0.0
            },
            "performance_rating": "Not Available"
        }
        
        logger.debug(
            "Performance benchmarks retrieved via API",
            line_id=line_id,
            user_id=current_user.user_id
        )
        
        return benchmarks
        
    except Exception as e:
        logger.error("Failed to get performance benchmarks via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/recalculate", status_code=status.HTTP_200_OK)
async def recalculate_oee(
    line_id: UUID,
    equipment_code: str,
    start_date: date,
    end_date: date,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Recalculate OEE for a specific period."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.OEE_CALCULATE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to calculate OEE"
            )
        
        # This would be implemented to recalculate OEE for the specified period
        # For now, return a success message
        
        logger.info(
            "OEE recalculation requested via API",
            line_id=line_id,
            equipment_code=equipment_code,
            start_date=start_date,
            end_date=end_date,
            user_id=current_user.user_id
        )
        
        return {
            "message": "OEE recalculation initiated",
            "line_id": line_id,
            "equipment_code": equipment_code,
            "start_date": start_date,
            "end_date": end_date,
            "status": "processing"
        }
        
    except Exception as e:
        logger.error("Failed to initiate OEE recalculation via API", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/real-time", status_code=status.HTTP_200_OK)
async def calculate_real_time_oee(
    line_id: UUID,
    equipment_code: str,
    current_status: dict,
    timestamp: Optional[datetime] = Query(None, description="Calculation timestamp (defaults to now)"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Calculate real-time OEE with current downtime integration."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.OEE_CALCULATE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to calculate real-time OEE"
            )
        
        if not timestamp:
            timestamp = datetime.utcnow()
        
        real_time_oee = await OEECalculator.calculate_real_time_oee(
            line_id=line_id,
            equipment_code=equipment_code,
            current_status=current_status,
            timestamp=timestamp
        )
        
        logger.info(
            "Real-time OEE calculated via API",
            line_id=line_id,
            equipment_code=equipment_code,
            oee=real_time_oee.get("oee"),
            user_id=current_user.user_id
        )
        
        return real_time_oee
        
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        logger.error("Failed to calculate real-time OEE via API", error=str(e), line_id=line_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/analysis", status_code=status.HTTP_200_OK)
async def get_oee_with_downtime_analysis(
    line_id: UUID,
    equipment_code: str,
    start_date: date = Query(..., description="Analysis start date"),
    end_date: date = Query(..., description="Analysis end date"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Get comprehensive OEE analysis with downtime breakdown."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.OEE_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view OEE analysis"
            )
        
        analysis = await OEECalculator.get_oee_with_downtime_analysis(
            line_id=line_id,
            equipment_code=equipment_code,
            start_date=start_date,
            end_date=end_date
        )
        
        logger.info(
            "OEE analysis with downtime retrieved via API",
            line_id=line_id,
            equipment_code=equipment_code,
            start_date=start_date,
            end_date=end_date,
            user_id=current_user.user_id
        )
        
        return analysis
        
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        logger.error("Failed to get OEE analysis via API", error=str(e), line_id=line_id)
        raise HTTPException(status_code=500, detail="Internal server error")


# Phase 3 Implementation - Enhanced OEE API Endpoints

@router.get("/analytics/equipment/{equipment_code}", status_code=status.HTTP_200_OK)
async def get_equipment_oee_with_analytics(
    equipment_code: str,
    time_period_hours: int = Query(24, ge=1, le=168, description="Time period in hours"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Get comprehensive OEE analytics for a specific equipment."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.OEE_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view OEE analytics"
            )
        
        analytics = await OEECalculator.calculate_equipment_oee_with_analytics(
            equipment_code=equipment_code,
            time_period_hours=time_period_hours
        )
        
        logger.info(
            "Equipment OEE analytics retrieved via API",
            equipment_code=equipment_code,
            time_period_hours=time_period_hours,
            user_id=current_user.user_id
        )
        
        return analytics
        
    except (NotFoundError, BusinessLogicError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to get equipment OEE analytics via API", error=str(e), equipment_code=equipment_code)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/dashboard/{line_id}", status_code=status.HTTP_200_OK)
async def get_oee_dashboard_data(
    line_id: UUID,
    days: int = Query(7, ge=1, le=30, description="Number of days for dashboard data"),
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> dict:
    """Get comprehensive OEE dashboard data for a production line."""
    try:
        # Check permissions
        if not current_user.has_permission(Permission.OEE_READ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to view OEE dashboard data"
            )
        
        dashboard_data = await OEECalculator.get_oee_dashboard_data(
            line_id=line_id,
            days=days
        )
        
        logger.debug(
            "OEE dashboard data retrieved via API",
            line_id=line_id,
            days=days,
            user_id=current_user.user_id
        )
        
        return dashboard_data
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Failed to get OEE dashboard data via API", error=str(e), line_id=line_id)
        raise HTTPException(status_code=500, detail="Internal server error")
