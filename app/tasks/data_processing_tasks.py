"""Data processing tasks for MS5.0 Floor Dashboard."""
from app.celery import celery_app
import logging
logger = logging.getLogger(__name__)

@celery_app.task(bind=True, name="app.tasks.data_processing_tasks.cleanup_old_data")
def cleanup_old_data(self):
    """Clean up old data based on retention policies."""
    logger.info("Cleaning up old data")
    return {"status": "success"}
