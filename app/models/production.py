"""
MS5.0 Floor Dashboard - Production Management Models

This module defines Pydantic models for production management operations
including production lines, schedules, jobs, OEE, and Andon events.
"""

from datetime import datetime, date, time
from typing import Any, Dict, List, Optional, Union
from uuid import UUID
from enum import Enum

from pydantic import BaseModel, Field, validator, root_validator


# Enums for status and types
class ProductionLineStatus(str, Enum):
    """Production line status enumeration."""
    RUNNING = "running"
    STOPPED = "stopped"
    FAULT = "fault"
    MAINTENANCE = "maintenance"
    SETUP = "setup"
    IDLE = "idle"


class JobStatus(str, Enum):
    """Job assignment status enumeration."""
    ASSIGNED = "assigned"
    ACCEPTED = "accepted"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class ScheduleStatus(str, Enum):
    """Production schedule status enumeration."""
    SCHEDULED = "scheduled"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class DowntimeCategory(str, Enum):
    """Downtime event category enumeration."""
    PLANNED = "planned"
    UNPLANNED = "unplanned"
    CHANGEOVER = "changeover"
    MAINTENANCE = "maintenance"


class AndonEventType(str, Enum):
    """Andon event type enumeration."""
    STOP = "stop"
    QUALITY = "quality"
    MAINTENANCE = "maintenance"
    MATERIAL = "material"
    SAFETY = "safety"


class AndonPriority(str, Enum):
    """Andon event priority enumeration."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AndonStatus(str, Enum):
    """Andon event status enumeration."""
    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    ESCALATED = "escalated"


# Base models
class BaseProductionModel(BaseModel):
    """Base model for production entities."""
    
    class Config:
        from_attributes = True
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
            time: lambda v: v.isoformat()
        }


# Production Line Models
class ProductionLineCreate(BaseProductionModel):
    """Model for creating a production line."""
    line_code: str = Field(..., min_length=1, max_length=50, description="Unique line code")
    name: str = Field(..., min_length=1, max_length=100, description="Line name")
    description: Optional[str] = Field(None, max_length=500, description="Line description")
    equipment_codes: List[str] = Field(..., min_items=1, description="List of equipment codes on this line")
    target_speed: Optional[float] = Field(None, gt=0, description="Target production speed")
    enabled: bool = Field(True, description="Whether the line is enabled")


class ProductionLineUpdate(BaseProductionModel):
    """Model for updating a production line."""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    equipment_codes: Optional[List[str]] = Field(None, min_items=1)
    target_speed: Optional[float] = Field(None, gt=0)
    enabled: Optional[bool] = None


class ProductionLineResponse(BaseProductionModel):
    """Model for production line response."""
    id: UUID
    line_code: str
    name: str
    description: Optional[str]
    equipment_codes: List[str]
    target_speed: Optional[float]
    enabled: bool
    status: ProductionLineStatus
    created_at: datetime
    updated_at: datetime


# Product Type Models
class ProductTypeCreate(BaseProductionModel):
    """Model for creating a product type."""
    product_code: str = Field(..., min_length=1, max_length=50, description="Unique product code")
    name: str = Field(..., min_length=1, max_length=100, description="Product name")
    description: Optional[str] = Field(None, max_length=500, description="Product description")
    target_speed: Optional[float] = Field(None, gt=0, description="Target production speed")
    cycle_time_seconds: Optional[float] = Field(None, gt=0, description="Cycle time in seconds")
    quality_specs: Optional[Dict[str, Any]] = Field(None, description="Quality specifications")
    enabled: bool = Field(True, description="Whether the product type is enabled")


class ProductTypeUpdate(BaseProductionModel):
    """Model for updating a product type."""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    target_speed: Optional[float] = Field(None, gt=0)
    cycle_time_seconds: Optional[float] = Field(None, gt=0)
    quality_specs: Optional[Dict[str, Any]] = None
    enabled: Optional[bool] = None


class ProductTypeResponse(BaseProductionModel):
    """Model for product type response."""
    id: UUID
    product_code: str
    name: str
    description: Optional[str]
    target_speed: Optional[float]
    cycle_time_seconds: Optional[float]
    quality_specs: Optional[Dict[str, Any]]
    enabled: bool
    created_at: datetime


# Production Schedule Models
class ProductionScheduleCreate(BaseProductionModel):
    """Model for creating a production schedule."""
    line_id: UUID = Field(..., description="Production line ID")
    product_type_id: UUID = Field(..., description="Product type ID")
    scheduled_start: datetime = Field(..., description="Scheduled start time")
    scheduled_end: datetime = Field(..., description="Scheduled end time")
    target_quantity: int = Field(..., gt=0, description="Target production quantity")
    priority: int = Field(1, ge=1, le=10, description="Schedule priority (1-10)")
    notes: Optional[str] = Field(None, max_length=1000, description="Schedule notes")
    
    @validator('scheduled_end')
    def validate_scheduled_end(cls, v, values):
        """Validate that end time is after start time."""
        if 'scheduled_start' in values and v <= values['scheduled_start']:
            raise ValueError('Scheduled end time must be after start time')
        return v


class ProductionScheduleUpdate(BaseProductionModel):
    """Model for updating a production schedule."""
    product_type_id: Optional[UUID] = None
    scheduled_start: Optional[datetime] = None
    scheduled_end: Optional[datetime] = None
    target_quantity: Optional[int] = Field(None, gt=0)
    priority: Optional[int] = Field(None, ge=1, le=10)
    status: Optional[ScheduleStatus] = None
    notes: Optional[str] = Field(None, max_length=1000)
    
    @root_validator
    def validate_schedule_times(cls, values):
        """Validate schedule times if both are provided."""
        start = values.get('scheduled_start')
        end = values.get('scheduled_end')
        
        if start and end and end <= start:
            raise ValueError('Scheduled end time must be after start time')
        
        return values


class ProductionScheduleResponse(BaseProductionModel):
    """Model for production schedule response."""
    id: UUID
    line_id: UUID
    product_type_id: UUID
    scheduled_start: datetime
    scheduled_end: datetime
    target_quantity: int
    priority: int
    status: ScheduleStatus
    notes: Optional[str]
    created_by: UUID
    created_at: datetime
    updated_at: datetime
    line: Optional[ProductionLineResponse] = None
    product_type: Optional[ProductTypeResponse] = None


# Job Assignment Models
class JobAssignmentCreate(BaseProductionModel):
    """Model for creating a job assignment."""
    schedule_id: UUID = Field(..., description="Production schedule ID")
    user_id: UUID = Field(..., description="Assigned user ID")
    notes: Optional[str] = Field(None, max_length=1000, description="Assignment notes")


class JobAssignmentUpdate(BaseProductionModel):
    """Model for updating a job assignment."""
    user_id: Optional[UUID] = None
    status: Optional[JobStatus] = None
    notes: Optional[str] = Field(None, max_length=1000)


class JobAssignmentResponse(BaseProductionModel):
    """Model for job assignment response."""
    id: UUID
    schedule_id: UUID
    user_id: UUID
    assigned_at: datetime
    accepted_at: Optional[datetime]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    status: JobStatus
    notes: Optional[str]
    schedule: Optional[ProductionScheduleResponse] = None


# Checklist Models
class ChecklistTemplateCreate(BaseProductionModel):
    """Model for creating a checklist template."""
    name: str = Field(..., min_length=1, max_length=100, description="Template name")
    equipment_codes: List[str] = Field(..., min_items=1, description="Applicable equipment codes")
    checklist_items: List[Dict[str, Any]] = Field(..., min_items=1, description="Checklist items")
    enabled: bool = Field(True, description="Whether the template is enabled")


class ChecklistTemplateUpdate(BaseProductionModel):
    """Model for updating a checklist template."""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    equipment_codes: Optional[List[str]] = Field(None, min_items=1)
    checklist_items: Optional[List[Dict[str, Any]]] = Field(None, min_items=1)
    enabled: Optional[bool] = None


class ChecklistTemplateResponse(BaseProductionModel):
    """Model for checklist template response."""
    id: UUID
    name: str
    equipment_codes: List[str]
    checklist_items: List[Dict[str, Any]]
    enabled: bool
    created_at: datetime


class ChecklistCompletionCreate(BaseProductionModel):
    """Model for completing a checklist."""
    job_assignment_id: UUID = Field(..., description="Job assignment ID")
    template_id: UUID = Field(..., description="Checklist template ID")
    responses: Dict[str, Any] = Field(..., description="Checklist responses")
    signature_data: Optional[Dict[str, Any]] = Field(None, description="Digital signature data")


class ChecklistCompletionResponse(BaseProductionModel):
    """Model for checklist completion response."""
    id: UUID
    job_assignment_id: UUID
    template_id: UUID
    completed_by: UUID
    completed_at: datetime
    responses: Dict[str, Any]
    signature_data: Optional[Dict[str, Any]]
    status: str


# Downtime Event Models
class DowntimeEventCreate(BaseProductionModel):
    """Model for creating a downtime event."""
    line_id: UUID = Field(..., description="Production line ID")
    equipment_code: str = Field(..., min_length=1, max_length=50, description="Equipment code")
    start_time: datetime = Field(..., description="Downtime start time")
    reason_code: str = Field(..., min_length=1, max_length=50, description="Reason code")
    reason_description: str = Field(..., min_length=1, max_length=500, description="Reason description")
    category: DowntimeCategory = Field(..., description="Downtime category")
    subcategory: Optional[str] = Field(None, max_length=100, description="Downtime subcategory")
    notes: Optional[str] = Field(None, max_length=1000, description="Additional notes")


class DowntimeEventUpdate(BaseProductionModel):
    """Model for updating a downtime event."""
    end_time: Optional[datetime] = None
    reason_code: Optional[str] = Field(None, min_length=1, max_length=50)
    reason_description: Optional[str] = Field(None, min_length=1, max_length=500)
    category: Optional[DowntimeCategory] = None
    subcategory: Optional[str] = Field(None, max_length=100)
    notes: Optional[str] = Field(None, max_length=1000)


class DowntimeEventResponse(BaseProductionModel):
    """Model for downtime event response."""
    id: UUID
    line_id: UUID
    equipment_code: str
    start_time: datetime
    end_time: Optional[datetime]
    duration_seconds: Optional[int]
    reason_code: str
    reason_description: str
    category: DowntimeCategory
    subcategory: Optional[str]
    reported_by: UUID
    confirmed_by: Optional[UUID]
    confirmed_at: Optional[datetime]
    notes: Optional[str]
    created_at: datetime


class DowntimeStatisticsResponse(BaseProductionModel):
    """Model for downtime statistics response."""
    total_events: int = Field(..., description="Total number of downtime events")
    total_downtime_seconds: int = Field(..., description="Total downtime in seconds")
    total_downtime_minutes: float = Field(..., description="Total downtime in minutes")
    total_downtime_hours: float = Field(..., description="Total downtime in hours")
    avg_duration_seconds: float = Field(..., description="Average duration in seconds")
    avg_duration_minutes: float = Field(..., description="Average duration in minutes")
    unplanned_events: int = Field(..., description="Number of unplanned events")
    planned_events: int = Field(..., description="Number of planned events")
    maintenance_events: int = Field(..., description="Number of maintenance events")
    changeover_events: int = Field(..., description="Number of changeover events")
    top_reasons: List[Dict[str, Any]] = Field(..., description="Top downtime reasons")
    daily_breakdown: List[Dict[str, Any]] = Field(..., description="Daily breakdown of events")


# OEE Models
class OEECalculationCreate(BaseProductionModel):
    """Model for creating OEE calculation."""
    line_id: UUID = Field(..., description="Production line ID")
    equipment_code: str = Field(..., min_length=1, max_length=50, description="Equipment code")
    calculation_time: datetime = Field(..., description="Calculation timestamp")
    planned_production_time: int = Field(..., ge=0, description="Planned production time in seconds")
    actual_production_time: int = Field(..., ge=0, description="Actual production time in seconds")
    ideal_cycle_time: float = Field(..., gt=0, description="Ideal cycle time in seconds")
    actual_cycle_time: float = Field(..., gt=0, description="Actual cycle time in seconds")
    good_parts: int = Field(..., ge=0, description="Number of good parts")
    total_parts: int = Field(..., ge=0, description="Total number of parts")


class OEECalculationResponse(BaseProductionModel):
    """Model for OEE calculation response."""
    id: int
    line_id: UUID
    equipment_code: str
    calculation_time: datetime
    availability: float
    performance: float
    quality: float
    oee: float
    planned_production_time: int
    actual_production_time: int
    ideal_cycle_time: float
    actual_cycle_time: float
    good_parts: int
    total_parts: int


# Andon Event Models
class AndonEventCreate(BaseProductionModel):
    """Model for creating an Andon event."""
    line_id: UUID = Field(..., description="Production line ID")
    equipment_code: str = Field(..., min_length=1, max_length=50, description="Equipment code")
    event_type: AndonEventType = Field(..., description="Event type")
    priority: AndonPriority = Field(..., description="Event priority")
    description: str = Field(..., min_length=1, max_length=500, description="Event description")


class AndonEventUpdate(BaseProductionModel):
    """Model for updating an Andon event."""
    status: Optional[AndonStatus] = None
    resolution_notes: Optional[str] = Field(None, max_length=1000, description="Resolution notes")


class AndonEventResponse(BaseProductionModel):
    """Model for Andon event response."""
    id: UUID
    line_id: UUID
    equipment_code: str
    event_type: AndonEventType
    priority: AndonPriority
    description: str
    status: AndonStatus
    reported_by: UUID
    reported_at: datetime
    acknowledged_by: Optional[UUID]
    acknowledged_at: Optional[datetime]
    resolved_by: Optional[UUID]
    resolved_at: Optional[datetime]
    resolution_notes: Optional[str]


# Production Report Models
class ProductionReportCreate(BaseProductionModel):
    """Model for creating a production report."""
    line_id: UUID = Field(..., description="Production line ID")
    report_date: date = Field(..., description="Report date")
    shift: Optional[str] = Field(None, max_length=50, description="Shift identifier")
    report_data: Optional[Dict[str, Any]] = Field(None, description="Report data")


class ProductionReportResponse(BaseProductionModel):
    """Model for production report response."""
    id: UUID
    line_id: UUID
    report_date: date
    shift: Optional[str]
    total_production: int
    good_parts: int
    scrap_parts: int
    rework_parts: int
    total_downtime_minutes: int
    oee_average: Optional[float]
    report_data: Optional[Dict[str, Any]]
    generated_by: UUID
    generated_at: datetime
    pdf_path: Optional[str]


# Dashboard Models
class LineStatusResponse(BaseProductionModel):
    """Model for line status response."""
    line_id: UUID
    line_code: str
    name: str
    status: ProductionLineStatus
    oee: float
    availability: float
    performance: float
    quality: float
    current_job: Optional[JobAssignmentResponse]
    active_downtime: Optional[DowntimeEventResponse]
    active_andon_events: List[AndonEventResponse]


class DashboardSummaryResponse(BaseProductionModel):
    """Model for dashboard summary response."""
    total_lines: int
    running_lines: int
    stopped_lines: int
    fault_lines: int
    total_jobs: int
    active_jobs: int
    completed_jobs: int
    total_andon_events: int
    open_andon_events: int
    average_oee: float
    total_downtime_minutes: int


# Pagination Models
class PaginationParams(BaseProductionModel):
    """Model for pagination parameters."""
    page: int = Field(1, ge=1, description="Page number")
    size: int = Field(20, ge=1, le=100, description="Page size")
    sort_by: Optional[str] = Field(None, description="Sort field")
    sort_order: Optional[str] = Field("asc", regex="^(asc|desc)$", description="Sort order")


class PaginatedResponse(BaseProductionModel):
    """Model for paginated response."""
    items: List[Any]
    total: int
    page: int
    size: int
    pages: int
    has_next: bool
    has_prev: bool
