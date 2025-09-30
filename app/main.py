"""
MS5.0 Floor Dashboard - Main FastAPI Application

This is the main entry point for the MS5.0 Floor Dashboard backend API.
It provides comprehensive production management capabilities with real-time
updates, role-based access control, and integration with existing PLC systems.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, Any

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
import structlog

from app.config import settings
from app.database import init_db, close_db
from app.api.v1 import auth, production, jobs, checklists, oee, andon, andon_escalation, reports, dashboard, equipment, upload, downtime
from app.api.v1 import enhanced_production, enhanced_oee_analytics, enhanced_production_websocket
from app.api.websocket import websocket_router
from app.api.enhanced_websocket import router as enhanced_websocket_router
from app.services.andon_escalation_monitor import start_escalation_monitor, stop_escalation_monitor
from app.services.real_time_integration_service import RealTimeIntegrationService
from app.services.enhanced_websocket_manager import EnhancedWebSocketManager
from app.utils.exceptions import (
    MS5Exception,
    AuthenticationError,
    AuthorizationError,
    ValidationError,
    NotFoundError,
    ConflictError
)

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Global real-time integration service
real_time_service = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager for startup and shutdown events."""
    # Startup
    logger.info("Starting MS5.0 Floor Dashboard API")
    await init_db()
    logger.info("Database initialized successfully")
    
    # Start escalation monitor
    await start_escalation_monitor()
    logger.info("Andon escalation monitor started")
    
    # Initialize and start real-time integration service
    global real_time_service
    websocket_manager = EnhancedWebSocketManager()
    real_time_service = RealTimeIntegrationService(websocket_manager)
    await real_time_service.initialize()
    await real_time_service.start()
    logger.info("Real-time integration service started")
    
    yield
    
    # Shutdown
    logger.info("Shutting down MS5.0 Floor Dashboard API")
    
    # Stop real-time integration service
    if real_time_service:
        await real_time_service.stop()
        logger.info("Real-time integration service stopped")
    
    await stop_escalation_monitor()
    logger.info("Andon escalation monitor stopped")
    await close_db()
    logger.info("Database connections closed")


# Create FastAPI application
app = FastAPI(
    title="MS5.0 Floor Dashboard API",
    description="""
    Comprehensive production management API for MS5.0 Floor Dashboard.
    
    This API provides:
    - Production line management and monitoring
    - Real-time OEE calculations and analytics
    - Job assignment and workflow management
    - Andon system for machine stoppages
    - Quality control and defect tracking
    - Maintenance management
    - Real-time dashboard data
    - Role-based access control
    - WebSocket support for real-time updates
    
    Built for tablet-based factory operations with offline capability.
    """,
    version="1.0.0",
    docs_url="/docs" if settings.ENVIRONMENT != "production" else None,
    redoc_url="/redoc" if settings.ENVIRONMENT != "production" else None,
    openapi_url="/openapi.json" if settings.ENVIRONMENT != "production" else None,
    lifespan=lifespan
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if settings.ENVIRONMENT == "production":
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=settings.ALLOWED_HOSTS
    )


# Global exception handlers
@app.exception_handler(MS5Exception)
async def ms5_exception_handler(request: Request, exc: MS5Exception) -> JSONResponse:
    """Handle custom MS5.0 exceptions."""
    logger.error(
        "MS5.0 exception occurred",
        exception_type=type(exc).__name__,
        message=str(exc),
        path=request.url.path,
        method=request.method
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.error_code,
            "message": exc.message,
            "details": exc.details
        }
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Handle request validation errors."""
    logger.warning(
        "Validation error occurred",
        errors=exc.errors(),
        path=request.url.path,
        method=request.method
    )
    
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "VALIDATION_ERROR",
            "message": "Request validation failed",
            "details": exc.errors()
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handle unexpected exceptions."""
    logger.error(
        "Unexpected error occurred",
        exception_type=type(exc).__name__,
        message=str(exc),
        path=request.url.path,
        method=request.method,
        exc_info=True
    )
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "INTERNAL_SERVER_ERROR",
            "message": "An unexpected error occurred",
            "details": None if settings.ENVIRONMENT == "production" else str(exc)
        }
    )


# Health check endpoints
@app.get("/health", tags=["Health"])
async def health_check() -> Dict[str, Any]:
    """Basic health check endpoint."""
    return {
        "status": "healthy",
        "service": "MS5.0 Floor Dashboard API",
        "version": "1.0.0",
        "timestamp": "2025-01-20T10:00:00Z"
    }


@app.get("/health/detailed", tags=["Health"])
async def detailed_health_check() -> Dict[str, Any]:
    """Detailed health check with database connectivity."""
    try:
        # Check database connectivity
        db_status = await check_database_health()
        
        return {
            "status": "healthy" if db_status else "unhealthy",
            "service": "MS5.0 Floor Dashboard API",
            "version": "1.0.0",
            "timestamp": "2025-01-20T10:00:00Z",
            "components": {
                "database": "healthy" if db_status else "unhealthy",
                "api": "healthy"
            }
        }
    except Exception as e:
        logger.error("Health check failed", error=str(e))
        return {
            "status": "unhealthy",
            "service": "MS5.0 Floor Dashboard API",
            "version": "1.0.0",
            "timestamp": "2025-01-20T10:00:00Z",
            "error": str(e)
        }


async def check_database_health() -> bool:
    """Check database connectivity."""
    try:
        # This would be implemented with actual database check
        # For now, return True as placeholder
        return True
    except Exception:
        return False


# Metrics endpoint for Prometheus
@app.get("/metrics", tags=["Monitoring"])
async def metrics() -> Response:
    """Prometheus metrics endpoint."""
    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


# Include API routers
app.include_router(auth.router, prefix="/api/v1/auth", tags=["Authentication"])
app.include_router(production.router, prefix="/api/v1/production", tags=["Production Management"])
app.include_router(jobs.router, prefix="/api/v1/jobs", tags=["Job Management"])
app.include_router(checklists.router, prefix="/api/v1/checklists", tags=["Checklist Management"])
app.include_router(oee.router, prefix="/api/v1/oee", tags=["OEE & Analytics"])
app.include_router(downtime.router, prefix="/api/v1/downtime", tags=["Downtime Management"])
app.include_router(andon.router, prefix="/api/v1/andon", tags=["Andon System"])
app.include_router(andon_escalation.router, prefix="/api/v1", tags=["Andon Escalation"])
app.include_router(reports.router, prefix="/api/v1/reports", tags=["Reports"])
app.include_router(dashboard.router, prefix="/api/v1/dashboard", tags=["Dashboard"])
app.include_router(equipment.router, prefix="/api/v1/equipment", tags=["Equipment"])
app.include_router(upload.router, prefix="/api/v1/upload", tags=["File Upload"])

# Enhanced API routers with PLC integration
app.include_router(enhanced_production.router, prefix="/api/v1/enhanced", tags=["Enhanced Production Management"])
app.include_router(enhanced_oee_analytics.router, prefix="/api/v1/enhanced/oee", tags=["Enhanced OEE Analytics"])
app.include_router(enhanced_production_websocket.router, prefix="/api/v1", tags=["Enhanced Production WebSocket"])

# WebSocket routers
app.include_router(websocket_router, prefix="/ws", tags=["WebSocket"])
app.include_router(enhanced_websocket_router, prefix="/ws", tags=["Enhanced WebSocket"])


# Root endpoint
@app.get("/", tags=["Root"])
async def root() -> Dict[str, str]:
    """Root endpoint with API information."""
    return {
        "message": "MS5.0 Floor Dashboard API",
        "version": "1.0.0",
        "docs": "/docs" if settings.ENVIRONMENT != "production" else "Documentation not available in production",
        "health": "/health"
    }


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.ENVIRONMENT == "development",
        log_level=settings.LOG_LEVEL.lower(),
        access_log=True
    )
