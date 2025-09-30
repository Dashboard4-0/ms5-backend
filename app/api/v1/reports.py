"""
MS5.0 Floor Dashboard - Reports API

This module provides API endpoints for report generation, management,
and PDF download functionality.
"""

from datetime import date, datetime, timedelta
from typing import List, Optional, Dict, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Query, Response
from fastapi.responses import FileResponse
from fastapi.exceptions import RequestValidationError

from app.auth.permissions import require_permission, Permission
from app.services.report_generator import ReportGenerator
from app.utils.exceptions import (
    NotFoundError, ValidationError, BusinessLogicError, ConflictError
)

router = APIRouter(prefix="/reports", tags=["reports"])


@router.post("/production", status_code=status.HTTP_201_CREATED)
async def generate_production_report(
    line_id: UUID,
    report_date: date,
    shift: Optional[str] = None,
    report_type: str = "daily",
    current_user: dict = Depends(require_permission(Permission.REPORTS_READ))
) -> Dict[str, Any]:
    """Generate production report for a specific line and date."""
    try:
        report_generator = ReportGenerator()
        result = await report_generator.generate_production_report(
            line_id=line_id,
            report_date=report_date,
            shift=shift,
            report_type=report_type
        )
        
        return {
            "message": "Production report generated successfully",
            "report": result
        }
        
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.post("/oee", status_code=status.HTTP_201_CREATED)
async def generate_oee_report(
    line_id: UUID,
    start_date: date,
    end_date: date,
    report_type: str = "oee_analysis",
    current_user: dict = Depends(require_permission(Permission.REPORTS_READ))
) -> Dict[str, Any]:
    """Generate OEE analysis report for a specific line and date range."""
    try:
        report_generator = ReportGenerator()
        result = await report_generator.generate_oee_report(
            line_id=line_id,
            start_date=start_date,
            end_date=end_date,
            report_type=report_type
        )
        
        return {
            "message": "OEE report generated successfully",
            "report": result
        }
        
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.post("/downtime", status_code=status.HTTP_201_CREATED)
async def generate_downtime_report(
    line_id: UUID,
    start_date: date,
    end_date: date,
    report_type: str = "downtime_analysis",
    current_user: dict = Depends(require_permission(Permission.REPORTS_READ))
) -> Dict[str, Any]:
    """Generate downtime analysis report for a specific line and date range."""
    try:
        report_generator = ReportGenerator()
        result = await report_generator.generate_downtime_report(
            line_id=line_id,
            start_date=start_date,
            end_date=end_date,
            report_type=report_type
        )
        
        return {
            "message": "Downtime report generated successfully",
            "report": result
        }
        
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.post("/custom", status_code=status.HTTP_201_CREATED)
async def generate_custom_report(
    template_id: UUID,
    parameters: Dict[str, Any],
    current_user: dict = Depends(require_permission(Permission.REPORTS_READ))
) -> Dict[str, Any]:
    """Generate custom report from template."""
    try:
        report_generator = ReportGenerator()
        result = await report_generator.generate_custom_report(
            template_id=template_id,
            parameters=parameters,
            user_id=current_user["id"]
        )
        
        return {
            "message": "Custom report generated successfully",
            "report": result
        }
        
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except BusinessLogicError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/", status_code=status.HTTP_200_OK)
async def list_reports(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    report_type: Optional[str] = Query(None, description="Filter by report type"),
    start_date: Optional[date] = Query(None, description="Filter by start date"),
    end_date: Optional[date] = Query(None, description="Filter by end date"),
    limit: int = Query(50, description="Number of reports to return"),
    offset: int = Query(0, description="Number of reports to skip"),
    current_user: dict = Depends(require_permission(Permission.REPORTS_READ))
) -> Dict[str, Any]:
    """List generated reports with filtering."""
    try:
        # Implementation would query database for reports
        # This is a placeholder implementation
        reports = [
            {
                "id": "12345678-1234-5678-9012-123456789012",
                "line_id": str(line_id) if line_id else None,
                "report_type": report_type or "daily",
                "report_date": "2025-01-20",
                "shift": "Day",
                "filename": "production_12345678-1234-5678-9012-123456789012_2025-01-20_daily.pdf",
                "file_path": "reports/production_12345678-1234-5678-9012-123456789012_2025-01-20_daily.pdf",
                "generated_at": "2025-01-20T10:00:00Z",
                "generated_by": current_user["id"],
                "file_size": 1024000,
                "status": "completed"
            }
        ]
        
        return {
            "reports": reports,
            "count": len(reports),
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/{report_id}", status_code=status.HTTP_200_OK)
async def get_report(
    report_id: UUID,
    current_user: dict = Depends(require_permission(Permission.REPORTS_READ))
) -> Dict[str, Any]:
    """Get specific report details."""
    try:
        # Implementation would query database for specific report
        # This is a placeholder implementation
        report = {
            "id": str(report_id),
            "line_id": "12345678-1234-5678-9012-123456789012",
            "report_type": "daily",
            "report_date": "2025-01-20",
            "shift": "Day",
            "filename": f"production_{report_id}_2025-01-20_daily.pdf",
            "file_path": f"reports/production_{report_id}_2025-01-20_daily.pdf",
            "generated_at": "2025-01-20T10:00:00Z",
            "generated_by": current_user["id"],
            "file_size": 1024000,
            "status": "completed",
            "data": {
                "total_production": 1000,
                "target_production": 1200,
                "oee": 0.73
            }
        }
        
        return report
        
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/{report_id}/pdf", status_code=status.HTTP_200_OK)
async def download_report_pdf(
    report_id: UUID,
    current_user: dict = Depends(require_permission(Permission.REPORTS_READ))
) -> FileResponse:
    """Download report PDF file."""
    try:
        # Implementation would get report file path from database
        # This is a placeholder implementation
        file_path = f"reports/production_{report_id}_2025-01-20_daily.pdf"
        
        # Check if file exists
        import os
        if not os.path.exists(file_path):
            raise NotFoundError("Report file not found")
        
        return FileResponse(
            path=file_path,
            filename=f"production_report_{report_id}.pdf",
            media_type="application/pdf"
        )
        
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.delete("/{report_id}", status_code=status.HTTP_200_OK)
async def delete_report(
    report_id: UUID,
    current_user: dict = Depends(require_permission(Permission.REPORTS_WRITE))
) -> Dict[str, Any]:
    """Delete a report and its associated files."""
    try:
        # Implementation would delete report from database and file system
        # This is a placeholder implementation
        
        return {
            "message": "Report deleted successfully",
            "report_id": str(report_id)
        }
        
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/templates/", status_code=status.HTTP_200_OK)
async def list_report_templates(
    current_user: dict = Depends(require_permission(Permission.REPORTS_READ))
) -> Dict[str, Any]:
    """List available report templates."""
    try:
        # Implementation would query database for report templates
        # This is a placeholder implementation
        templates = [
            {
                "id": "12345678-1234-5678-9012-123456789012",
                "name": "Daily Production Report",
                "description": "Standard daily production report with OEE and downtime analysis",
                "type": "production",
                "parameters": [
                    {"name": "line_id", "type": "uuid", "required": True, "description": "Production line ID"},
                    {"name": "report_date", "type": "date", "required": True, "description": "Report date"},
                    {"name": "shift", "type": "string", "required": False, "description": "Shift (Day/Night)"}
                ],
                "created_at": "2025-01-20T10:00:00Z",
                "created_by": current_user["id"]
            },
            {
                "id": "12345678-1234-5678-9012-123456789013",
                "name": "OEE Analysis Report",
                "description": "Comprehensive OEE analysis report for a date range",
                "type": "oee",
                "parameters": [
                    {"name": "line_id", "type": "uuid", "required": True, "description": "Production line ID"},
                    {"name": "start_date", "type": "date", "required": True, "description": "Start date"},
                    {"name": "end_date", "type": "date", "required": True, "description": "End date"}
                ],
                "created_at": "2025-01-20T10:00:00Z",
                "created_by": current_user["id"]
            }
        ]
        
        return {
            "templates": templates,
            "count": len(templates)
        }
        
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/templates/{template_id}", status_code=status.HTTP_200_OK)
async def get_report_template(
    template_id: UUID,
    current_user: dict = Depends(require_permission(Permission.REPORTS_READ))
) -> Dict[str, Any]:
    """Get specific report template details."""
    try:
        # Implementation would query database for specific template
        # This is a placeholder implementation
        template = {
            "id": str(template_id),
            "name": "Daily Production Report",
            "description": "Standard daily production report with OEE and downtime analysis",
            "type": "production",
            "parameters": [
                {"name": "line_id", "type": "uuid", "required": True, "description": "Production line ID"},
                {"name": "report_date", "type": "date", "required": True, "description": "Report date"},
                {"name": "shift", "type": "string", "required": False, "description": "Shift (Day/Night)"}
            ],
            "sections": [
                {"type": "header", "title": "Report Header"},
                {"type": "summary", "title": "Executive Summary"},
                {"type": "oee", "title": "OEE Analysis"},
                {"type": "downtime", "title": "Downtime Analysis"},
                {"type": "production", "title": "Production Details"},
                {"type": "quality", "title": "Quality Analysis"},
                {"type": "equipment", "title": "Equipment Status"}
            ],
            "created_at": "2025-01-20T10:00:00Z",
            "created_by": current_user["id"]
        }
        
        return template
        
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.post("/templates/", status_code=status.HTTP_201_CREATED)
async def create_report_template(
    name: str,
    description: str,
    template_type: str,
    parameters: List[Dict[str, Any]],
    sections: List[Dict[str, Any]],
    current_user: dict = Depends(require_permission(Permission.REPORTS_WRITE))
) -> Dict[str, Any]:
    """Create a new report template."""
    try:
        # Implementation would create template in database
        # This is a placeholder implementation
        template_id = "12345678-1234-5678-9012-123456789014"
        
        return {
            "message": "Report template created successfully",
            "template": {
                "id": template_id,
                "name": name,
                "description": description,
                "type": template_type,
                "parameters": parameters,
                "sections": sections,
                "created_at": "2025-01-20T10:00:00Z",
                "created_by": current_user["id"]
            }
        }
        
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.put("/templates/{template_id}", status_code=status.HTTP_200_OK)
async def update_report_template(
    template_id: UUID,
    name: Optional[str] = None,
    description: Optional[str] = None,
    parameters: Optional[List[Dict[str, Any]]] = None,
    sections: Optional[List[Dict[str, Any]]] = None,
    current_user: dict = Depends(require_permission(Permission.REPORTS_WRITE))
) -> Dict[str, Any]:
    """Update an existing report template."""
    try:
        # Implementation would update template in database
        # This is a placeholder implementation
        
        return {
            "message": "Report template updated successfully",
            "template_id": str(template_id)
        }
        
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.delete("/templates/{template_id}", status_code=status.HTTP_200_OK)
async def delete_report_template(
    template_id: UUID,
    current_user: dict = Depends(require_permission(Permission.REPORTS_WRITE))
) -> Dict[str, Any]:
    """Delete a report template."""
    try:
        # Implementation would delete template from database
        # This is a placeholder implementation
        
        return {
            "message": "Report template deleted successfully",
            "template_id": str(template_id)
        }
        
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/statistics/", status_code=status.HTTP_200_OK)
async def get_report_statistics(
    line_id: Optional[UUID] = Query(None, description="Filter by production line ID"),
    start_date: Optional[date] = Query(None, description="Filter by start date"),
    end_date: Optional[date] = Query(None, description="Filter by end date"),
    current_user: dict = Depends(require_permission(Permission.REPORTS_READ))
) -> Dict[str, Any]:
    """Get report generation statistics."""
    try:
        # Implementation would query database for report statistics
        # This is a placeholder implementation
        statistics = {
            "total_reports": 150,
            "reports_this_month": 25,
            "reports_this_week": 8,
            "reports_today": 2,
            "most_common_type": "daily",
            "average_file_size": 1024000,
            "total_storage_used": 153600000,
            "reports_by_type": {
                "daily": 100,
                "oee_analysis": 30,
                "downtime_analysis": 20
            },
            "reports_by_line": {
                "12345678-1234-5678-9012-123456789012": 50,
                "12345678-1234-5678-9012-123456789013": 45,
                "12345678-1234-5678-9012-123456789014": 35,
                "12345678-1234-5678-9012-123456789015": 20
            }
        }
        
        return statistics
        
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/health/", status_code=status.HTTP_200_OK)
async def get_reports_health(
    current_user: dict = Depends(require_permission(Permission.REPORTS_READ))
) -> Dict[str, Any]:
    """Get reports system health status."""
    try:
        # Implementation would check reports system health
        # This is a placeholder implementation
        health_status = {
            "status": "healthy",
            "report_generator": "operational",
            "file_storage": "operational",
            "database": "operational",
            "last_check": "2025-01-20T10:00:00Z",
            "uptime": "99.9%",
            "active_reports": 5,
            "queued_reports": 0,
            "failed_reports": 0
        }
        
        return health_status
        
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")