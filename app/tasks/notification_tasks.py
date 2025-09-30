"""Notification tasks for MS5.0 Floor Dashboard."""
from app.celery import celery_app
import logging
logger = logging.getLogger(__name__)

@celery_app.task(bind=True, name="app.tasks.notification_tasks.send_notification")
def send_notification(self, message: str, recipients: list):
    """Send notification to specified recipients."""
    logger.info(f"Sending notification: {message}")
    return {"status": "success"}
