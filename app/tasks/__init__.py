"""
MS5.0 Floor Dashboard - Task Modules

This package contains all Celery task definitions for background processing.
"""

from app.celery import celery_app

__all__ = ["celery_app"]
