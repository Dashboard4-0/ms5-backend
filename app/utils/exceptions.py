"""
MS5.0 Floor Dashboard - Custom Exception Classes

This module defines custom exception classes for the MS5.0 Floor Dashboard API.
These exceptions provide structured error handling with proper HTTP status codes
and detailed error information for better API responses.
"""

from typing import Any, Dict, Optional
from fastapi import status


class MS5Exception(Exception):
    """Base exception class for MS5.0 Floor Dashboard."""
    
    def __init__(
        self,
        message: str,
        error_code: str = "MS5_ERROR",
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        details: Optional[Dict[str, Any]] = None
    ):
        self.message = message
        self.error_code = error_code
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)


class AuthenticationError(MS5Exception):
    """Exception raised for authentication failures."""
    
    def __init__(self, message: str = "Authentication failed", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            error_code="AUTHENTICATION_ERROR",
            status_code=status.HTTP_401_UNAUTHORIZED,
            details=details
        )


class AuthorizationError(MS5Exception):
    """Exception raised for authorization failures."""
    
    def __init__(self, message: str = "Insufficient permissions", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            error_code="AUTHORIZATION_ERROR",
            status_code=status.HTTP_403_FORBIDDEN,
            details=details
        )


class ValidationError(MS5Exception):
    """Exception raised for validation failures."""
    
    def __init__(self, message: str = "Validation failed", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            error_code="VALIDATION_ERROR",
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            details=details
        )


class NotFoundError(MS5Exception):
    """Exception raised when a resource is not found."""
    
    def __init__(self, resource: str = "Resource", resource_id: Optional[str] = None):
        message = f"{resource} not found"
        if resource_id:
            message += f" with ID: {resource_id}"
        
        super().__init__(
            message=message,
            error_code="NOT_FOUND",
            status_code=status.HTTP_404_NOT_FOUND,
            details={"resource": resource, "resource_id": resource_id}
        )


class ConflictError(MS5Exception):
    """Exception raised for resource conflicts."""
    
    def __init__(self, message: str = "Resource conflict", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            error_code="CONFLICT",
            status_code=status.HTTP_409_CONFLICT,
            details=details
        )


class BusinessLogicError(MS5Exception):
    """Exception raised for business logic violations."""
    
    def __init__(self, message: str = "Business logic violation", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            error_code="BUSINESS_LOGIC_ERROR",
            status_code=status.HTTP_400_BAD_REQUEST,
            details=details
        )


class DatabaseError(MS5Exception):
    """Exception raised for database operation failures."""
    
    def __init__(self, message: str = "Database operation failed", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            error_code="DATABASE_ERROR",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=details
        )


class ExternalServiceError(MS5Exception):
    """Exception raised for external service failures."""
    
    def __init__(self, service: str, message: str = "External service error", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=f"{service}: {message}",
            error_code="EXTERNAL_SERVICE_ERROR",
            status_code=status.HTTP_502_BAD_GATEWAY,
            details={"service": service, **details or {}}
        )


class RateLimitError(MS5Exception):
    """Exception raised when rate limits are exceeded."""
    
    def __init__(self, message: str = "Rate limit exceeded", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            error_code="RATE_LIMIT_EXCEEDED",
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            details=details
        )


class MaintenanceModeError(MS5Exception):
    """Exception raised when the system is in maintenance mode."""
    
    def __init__(self, message: str = "System is in maintenance mode", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            error_code="MAINTENANCE_MODE",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            details=details
        )


# Production-specific exceptions
class ProductionLineError(MS5Exception):
    """Exception raised for production line related errors."""
    
    def __init__(self, line_id: str, message: str = "Production line error", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=f"Line {line_id}: {message}",
            error_code="PRODUCTION_LINE_ERROR",
            status_code=status.HTTP_400_BAD_REQUEST,
            details={"line_id": line_id, **details or {}}
        )


class JobAssignmentError(MS5Exception):
    """Exception raised for job assignment related errors."""
    
    def __init__(self, job_id: str, message: str = "Job assignment error", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=f"Job {job_id}: {message}",
            error_code="JOB_ASSIGNMENT_ERROR",
            status_code=status.HTTP_400_BAD_REQUEST,
            details={"job_id": job_id, **details or {}}
        )


class OEEError(MS5Exception):
    """Exception raised for OEE calculation errors."""
    
    def __init__(self, message: str = "OEE calculation error", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            error_code="OEE_ERROR",
            status_code=status.HTTP_400_BAD_REQUEST,
            details=details
        )


class AndonError(MS5Exception):
    """Exception raised for Andon system errors."""
    
    def __init__(self, event_id: str, message: str = "Andon system error", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=f"Andon event {event_id}: {message}",
            error_code="ANDON_ERROR",
            status_code=status.HTTP_400_BAD_REQUEST,
            details={"event_id": event_id, **details or {}}
        )


class EquipmentError(MS5Exception):
    """Exception raised for equipment related errors."""
    
    def __init__(self, equipment_code: str, message: str = "Equipment error", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=f"Equipment {equipment_code}: {message}",
            error_code="EQUIPMENT_ERROR",
            status_code=status.HTTP_400_BAD_REQUEST,
            details={"equipment_code": equipment_code, **details or {}}
        )


class ReportGenerationError(MS5Exception):
    """Exception raised for report generation errors."""
    
    def __init__(self, report_type: str, message: str = "Report generation error", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=f"{report_type} report: {message}",
            error_code="REPORT_GENERATION_ERROR",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details={"report_type": report_type, **details or {}}
        )


class WebSocketError(MS5Exception):
    """Exception raised for WebSocket related errors."""
    
    def __init__(self, message: str = "WebSocket error", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            error_code="WEBSOCKET_ERROR",
            status_code=status.HTTP_400_BAD_REQUEST,
            details=details
        )


# Utility functions for exception handling
def handle_database_exception(e: Exception) -> MS5Exception:
    """Convert database exceptions to MS5Exception."""
    if "duplicate key" in str(e).lower():
        return ConflictError("Resource already exists", {"original_error": str(e)})
    elif "foreign key" in str(e).lower():
        return ValidationError("Invalid reference to related resource", {"original_error": str(e)})
    elif "not null" in str(e).lower():
        return ValidationError("Required field is missing", {"original_error": str(e)})
    else:
        return DatabaseError("Database operation failed", {"original_error": str(e)})


def handle_validation_exception(e: Exception) -> ValidationError:
    """Convert validation exceptions to ValidationError."""
    return ValidationError("Input validation failed", {"original_error": str(e)})


def handle_authentication_exception(e: Exception) -> AuthenticationError:
    """Convert authentication exceptions to AuthenticationError."""
    return AuthenticationError("Authentication failed", {"original_error": str(e)})


def handle_authorization_exception(e: Exception) -> AuthorizationError:
    """Convert authorization exceptions to AuthorizationError."""
    return AuthorizationError("Insufficient permissions", {"original_error": str(e)})
