"""
MS5.0 Floor Dashboard - Celery Application Configuration

This module configures the Celery application for background task processing.
It integrates with Redis as the message broker and result backend, providing
comprehensive task management for production operations.

Architecture:
- Redis as message broker and result backend
- Task routing by business domain (production, oee, andon, etc.)
- Comprehensive monitoring and error handling
- Beat scheduler for periodic tasks
- Integration with existing RealTimeIntegrationService
"""

import os
import logging
from datetime import timedelta
from celery import Celery
from celery.schedules import crontab
from app.config import settings

# Configure logging
logger = logging.getLogger(__name__)

# Create Celery instance with comprehensive configuration
celery_app = Celery(
    "ms5_floor_dashboard",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=[
        "app.tasks.production_tasks",
        "app.tasks.maintenance_tasks", 
        "app.tasks.report_tasks",
        "app.tasks.notification_tasks",
        "app.tasks.data_processing_tasks",
        "app.tasks.oee_tasks",
        "app.tasks.andon_tasks",
        "app.tasks.quality_tasks"
    ]
)

# Comprehensive Celery configuration
celery_app.conf.update(
    # Task execution settings
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    
    # Task routing by business domain
    task_routes={
        # Production tasks - high priority, frequent execution
        "app.tasks.production_tasks.*": {
            "queue": "production",
            "priority": 8,
            "routing_key": "production"
        },
        # OEE tasks - medium priority, regular execution
        "app.tasks.oee_tasks.*": {
            "queue": "oee",
            "priority": 7,
            "routing_key": "oee"
        },
        # Andon tasks - high priority, immediate execution
        "app.tasks.andon_tasks.*": {
            "queue": "andon",
            "priority": 9,
            "routing_key": "andon"
        },
        # Quality tasks - medium priority
        "app.tasks.quality_tasks.*": {
            "queue": "quality",
            "priority": 6,
            "routing_key": "quality"
        },
        # Maintenance tasks - low priority, batch processing
        "app.tasks.maintenance_tasks.*": {
            "queue": "maintenance",
            "priority": 4,
            "routing_key": "maintenance"
        },
        # Report tasks - low priority, scheduled execution
        "app.tasks.report_tasks.*": {
            "queue": "reports",
            "priority": 3,
            "routing_key": "reports"
        },
        # Notification tasks - medium priority
        "app.tasks.notification_tasks.*": {
            "queue": "notifications",
            "priority": 5,
            "routing_key": "notifications"
        },
        # Data processing tasks - medium priority
        "app.tasks.data_processing_tasks.*": {
            "queue": "data_processing",
            "priority": 6,
            "routing_key": "data_processing"
        },
    },
    
    # Worker settings for optimal performance
    worker_prefetch_multiplier=1,  # Process one task at a time for better memory management
    task_acks_late=True,  # Acknowledge tasks after completion
    worker_disable_rate_limits=False,
    worker_max_tasks_per_child=1000,  # Restart workers after 1000 tasks to prevent memory leaks
    
    # Result backend settings
    result_expires=3600,  # Results expire after 1 hour
    result_persistent=True,  # Persist results to Redis
    
    # Task execution settings with appropriate timeouts
    task_soft_time_limit=300,  # 5 minutes soft limit
    task_time_limit=600,  # 10 minutes hard limit
    task_default_retry_delay=60,  # 1 minute retry delay
    task_max_retries=3,  # Maximum 3 retries
    
    # Monitoring and observability
    worker_send_task_events=True,
    task_send_sent_event=True,
    task_track_started=True,
    
    # Beat scheduler configuration for periodic tasks
    beat_schedule={
        # Production monitoring tasks
        "poll-production-data": {
            "task": "app.tasks.production_tasks.poll_production_data",
            "schedule": crontab(minute="*/1"),  # Every minute
            "options": {"queue": "production", "priority": 8}
        },
        "update-production-metrics": {
            "task": "app.tasks.production_tasks.update_production_metrics",
            "schedule": crontab(minute="*/5"),  # Every 5 minutes
            "options": {"queue": "production", "priority": 7}
        },
        
        # OEE calculation tasks
        "calculate-oee-metrics": {
            "task": "app.tasks.oee_tasks.calculate_oee_metrics",
            "schedule": crontab(minute="*/10"),  # Every 10 minutes
            "options": {"queue": "oee", "priority": 7}
        },
        "calculate-line-oee": {
            "task": "app.tasks.oee_tasks.calculate_line_oee",
            "schedule": crontab(minute="*/15"),  # Every 15 minutes
            "options": {"queue": "oee", "priority": 6}
        },
        
        # Andon monitoring tasks
        "monitor-andon-events": {
            "task": "app.tasks.andon_tasks.monitor_andon_events",
            "schedule": crontab(minute="*/30"),  # Every 30 seconds
            "options": {"queue": "andon", "priority": 9}
        },
        "process-andon-escalations": {
            "task": "app.tasks.andon_tasks.process_andon_escalations",
            "schedule": crontab(minute="*/1"),  # Every minute
            "options": {"queue": "andon", "priority": 8}
        },
        
        # Quality monitoring tasks
        "monitor-quality-metrics": {
            "task": "app.tasks.quality_tasks.monitor_quality_metrics",
            "schedule": crontab(minute="*/5"),  # Every 5 minutes
            "options": {"queue": "quality", "priority": 6}
        },
        
        # Maintenance tasks
        "check-maintenance-schedules": {
            "task": "app.tasks.maintenance_tasks.check_maintenance_schedules",
            "schedule": crontab(hour="*/1"),  # Every hour
            "options": {"queue": "maintenance", "priority": 4}
        },
        
        # Report generation tasks
        "generate-daily-reports": {
            "task": "app.tasks.report_tasks.generate_daily_reports",
            "schedule": crontab(hour=6, minute=0),  # Daily at 6 AM
            "options": {"queue": "reports", "priority": 3}
        },
        "generate-weekly-reports": {
            "task": "app.tasks.report_tasks.generate_weekly_reports",
            "schedule": crontab(hour=7, minute=0, day_of_week=1),  # Weekly on Monday at 7 AM
            "options": {"queue": "reports", "priority": 3}
        },
        
        # Data processing tasks
        "process-telemetry-data": {
            "task": "app.tasks.data_processing_tasks.process_telemetry_data",
            "schedule": crontab(minute="*/2"),  # Every 2 minutes
            "options": {"queue": "data_processing", "priority": 6}
        },
        "cleanup-old-data": {
            "task": "app.tasks.data_processing_tasks.cleanup_old_data",
            "schedule": crontab(hour=2, minute=0),  # Daily at 2 AM
            "options": {"queue": "data_processing", "priority": 2}
        },
        
        # Notification tasks
        "send-notifications": {
            "task": "app.tasks.notification_tasks.send_notifications",
            "schedule": crontab(minute="*/1"),  # Every minute
            "options": {"queue": "notifications", "priority": 5}
        },
        "cleanup-notifications": {
            "task": "app.tasks.notification_tasks.cleanup_notifications",
            "schedule": crontab(hour=3, minute=0),  # Daily at 3 AM
            "options": {"queue": "notifications", "priority": 2}
        },
    },
    
    # Queue configuration
    task_default_queue="default",
    task_default_exchange="default",
    task_default_exchange_type="direct",
    task_default_routing_key="default",
    
    # Error handling
    task_ignore_result=False,  # Don't ignore results for monitoring
    task_store_eager_result=True,  # Store results immediately
    
    # Security settings
    task_always_eager=False,  # Run tasks asynchronously in production
    task_eager_propagates=True,
    
    # Performance optimization
    worker_direct=True,  # Direct worker communication
    worker_pool_restarts=True,  # Allow worker pool restarts
    
    # Logging configuration
    worker_log_format="[%(asctime)s: %(levelname)s/%(processName)s] %(message)s",
    worker_task_log_format="[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s",
    
    # Connection settings
    broker_connection_retry_on_startup=True,
    broker_connection_retry=True,
    broker_connection_max_retries=10,
    
    # Redis-specific settings
    redis_socket_keepalive=True,
    redis_socket_keepalive_options={},
    redis_retry_on_timeout=True,
    redis_health_check_interval=30,
    
    # Task compression
    task_compression="gzip",
    result_compression="gzip",
    
    # Task result settings
    result_backend_transport_options={
        "master_name": "mymaster",
        "visibility_timeout": 3600,
    },
    
    # Beat scheduler settings
    beat_scheduler="celery.beat:PersistentScheduler",
    beat_schedule_filename="celerybeat-schedule",
    beat_max_loop_interval=5,  # Maximum loop interval in seconds
)

# Configure task error handling
@celery_app.task(bind=True)
def debug_task(self):
    """Debug task for testing Celery configuration."""
    logger.info(f"Request: {self.request!r}")
    return "Debug task completed successfully"

# Task signal handlers for monitoring
from celery.signals import task_prerun, task_postrun, task_failure

@task_prerun.connect
def task_prerun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, **kwds):
    """Log task start."""
    logger.info(
        f"Task starting: {task.name}",
        task_id=task_id,
        task_name=task.name,
        args=args,
        kwargs=kwargs
    )

@task_postrun.connect
def task_postrun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, retval=None, state=None, **kwds):
    """Log task completion."""
    logger.info(
        f"Task completed: {task.name}",
        task_id=task_id,
        task_name=task.name,
        state=state,
        retval=retval
    )

@task_failure.connect
def task_failure_handler(sender=None, task_id=None, exception=None, traceback=None, einfo=None, **kwds):
    """Log task failure."""
    logger.error(
        f"Task failed: {sender.name}",
        task_id=task_id,
        task_name=sender.name,
        exception=str(exception),
        traceback=traceback
    )

# Health check task
@celery_app.task(bind=True, name="health_check")
def health_check_task(self):
    """Health check task for monitoring."""
    try:
        # Basic health check - can be extended with more checks
        return {
            "status": "healthy",
            "worker": self.request.hostname,
            "timestamp": "2025-01-20T10:00:00Z"
        }
    except Exception as exc:
        logger.error(f"Health check failed: {exc}")
        raise self.retry(exc=exc, countdown=60, max_retries=3)

# Export the configured Celery app
__all__ = ["celery_app"]