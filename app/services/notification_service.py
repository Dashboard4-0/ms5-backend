"""
MS5.0 Floor Dashboard - Notification Service

This module provides push notification services for the MS5.0 Floor Dashboard application.
It supports Firebase Cloud Messaging (FCM) and email notifications.
"""

import json
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import structlog

from app.config import settings

logger = structlog.get_logger()


class NotificationService:
    """Service for sending push notifications and emails."""
    
    def __init__(self):
        self.fcm_server_key = getattr(settings, 'FCM_SERVER_KEY', None)
        self.enabled = getattr(settings, 'ENABLE_PUSH_NOTIFICATIONS', False)
        
    async def send_push_notification(
        self,
        user_id: str,
        title: str,
        body: str,
        data: Optional[Dict[str, Any]] = None,
        notification_type: str = "general"
    ) -> bool:
        """Send a push notification to a user."""
        try:
            if not self.enabled or not self.fcm_server_key:
                logger.warning("Push notifications not enabled or FCM server key not configured")
                return False
            
            # Get user's FCM token (this would typically come from database)
            fcm_token = await self._get_user_fcm_token(user_id)
            if not fcm_token:
                logger.warning("No FCM token found for user", user_id=user_id)
                return False
            
            # Prepare notification payload
            payload = {
                "to": fcm_token,
                "notification": {
                    "title": title,
                    "body": body,
                    "sound": "default",
                    "badge": 1
                },
                "data": {
                    "notification_type": notification_type,
                    "user_id": user_id,
                    "timestamp": datetime.utcnow().isoformat(),
                    **(data or {})
                }
            }
            
            # Send notification via FCM
            success = await self._send_fcm_notification(payload)
            
            if success:
                logger.info(
                    "Push notification sent successfully",
                    user_id=user_id,
                    notification_type=notification_type,
                    title=title
                )
            else:
                logger.error(
                    "Failed to send push notification",
                    user_id=user_id,
                    notification_type=notification_type
                )
            
            return success
            
        except Exception as e:
            logger.error(
                "Error sending push notification",
                error=str(e),
                user_id=user_id,
                notification_type=notification_type
            )
            return False
    
    async def send_bulk_push_notification(
        self,
        user_ids: List[str],
        title: str,
        body: str,
        data: Optional[Dict[str, Any]] = None,
        notification_type: str = "general"
    ) -> Dict[str, Any]:
        """Send push notifications to multiple users."""
        results = {
            "successful": [],
            "failed": [],
            "total_sent": 0,
            "total_failed": 0
        }
        
        for user_id in user_ids:
            success = await self.send_push_notification(
                user_id=user_id,
                title=title,
                body=body,
                data=data,
                notification_type=notification_type
            )
            
            if success:
                results["successful"].append(user_id)
                results["total_sent"] += 1
            else:
                results["failed"].append(user_id)
                results["total_failed"] += 1
        
        logger.info(
            "Bulk push notification completed",
            total_users=len(user_ids),
            successful=results["total_sent"],
            failed=results["total_failed"]
        )
        
        return results
    
    async def send_notification_to_role(
        self,
        role: str,
        title: str,
        body: str,
        data: Optional[Dict[str, Any]] = None,
        notification_type: str = "general"
    ) -> Dict[str, Any]:
        """Send push notification to all users with a specific role."""
        try:
            # Get all users with the specified role (this would typically come from database)
            user_ids = await self._get_users_by_role(role)
            
            if not user_ids:
                logger.warning("No users found with role", role=role)
                return {
                    "successful": [],
                    "failed": [],
                    "total_sent": 0,
                    "total_failed": 0
                }
            
            return await self.send_bulk_push_notification(
                user_ids=user_ids,
                title=title,
                body=body,
                data=data,
                notification_type=notification_type
            )
            
        except Exception as e:
            logger.error("Error sending notification to role", error=str(e), role=role)
            return {
                "successful": [],
                "failed": [],
                "total_sent": 0,
                "total_failed": 0
            }
    
    async def send_email_notification(
        self,
        email: str,
        subject: str,
        body: str,
        html_body: Optional[str] = None
    ) -> bool:
        """Send an email notification."""
        try:
            # This would integrate with an email service like SendGrid, AWS SES, etc.
            # For now, we'll just log the email notification
            logger.info(
                "Email notification prepared",
                email=email,
                subject=subject,
                has_html=bool(html_body)
            )
            
            # Placeholder for actual email sending implementation
            # await self._send_email_via_service(email, subject, body, html_body)
            
            return True
            
        except Exception as e:
            logger.error("Error sending email notification", error=str(e), email=email)
            return False
    
    async def send_andon_notification(
        self,
        line_id: str,
        equipment_code: str,
        event_type: str,
        severity: str,
        message: str
    ) -> bool:
        """Send Andon-specific push notification."""
        try:
            # Get users who should receive Andon notifications
            user_ids = await self._get_andon_notification_users(line_id, equipment_code, severity)
            
            title = f"Andon Alert - {equipment_code}"
            body = f"{event_type}: {message}"
            
            data = {
                "line_id": line_id,
                "equipment_code": equipment_code,
                "event_type": event_type,
                "severity": severity,
                "action_required": True
            }
            
            if user_ids:
                result = await self.send_bulk_push_notification(
                    user_ids=user_ids,
                    title=title,
                    body=body,
                    data=data,
                    notification_type="andon"
                )
                return result["total_sent"] > 0
            else:
                logger.warning("No users to notify for Andon event", line_id=line_id, equipment_code=equipment_code)
                return False
                
        except Exception as e:
            logger.error("Error sending Andon notification", error=str(e), line_id=line_id)
            return False
    
    async def send_maintenance_reminder(
        self,
        equipment_code: str,
        maintenance_type: str,
        due_date: datetime,
        assigned_user_id: str
    ) -> bool:
        """Send maintenance reminder notification."""
        try:
            title = f"Maintenance Reminder - {equipment_code}"
            body = f"{maintenance_type} maintenance is due on {due_date.strftime('%Y-%m-%d')}"
            
            data = {
                "equipment_code": equipment_code,
                "maintenance_type": maintenance_type,
                "due_date": due_date.isoformat(),
                "action_required": True
            }
            
            return await self.send_push_notification(
                user_id=assigned_user_id,
                title=title,
                body=body,
                data=data,
                notification_type="maintenance"
            )
            
        except Exception as e:
            logger.error("Error sending maintenance reminder", error=str(e), equipment_code=equipment_code)
            return False
    
    async def send_quality_alert(
        self,
        line_id: str,
        defect_count: int,
        threshold: int,
        quality_manager_ids: List[str]
    ) -> bool:
        """Send quality alert notification."""
        try:
            title = f"Quality Alert - Line {line_id}"
            body = f"Defect count ({defect_count}) exceeded threshold ({threshold})"
            
            data = {
                "line_id": line_id,
                "defect_count": defect_count,
                "threshold": threshold,
                "action_required": True
            }
            
            result = await self.send_bulk_push_notification(
                user_ids=quality_manager_ids,
                title=title,
                body=body,
                data=data,
                notification_type="quality"
            )
            
            return result["total_sent"] > 0
            
        except Exception as e:
            logger.error("Error sending quality alert", error=str(e), line_id=line_id)
            return False
    
    async def _send_fcm_notification(self, payload: Dict[str, Any]) -> bool:
        """Send notification via Firebase Cloud Messaging."""
        try:
            # This would use the Firebase Admin SDK or HTTP API
            # For now, we'll simulate the notification sending
            
            import aiohttp
            
            headers = {
                "Authorization": f"key={self.fcm_server_key}",
                "Content-Type": "application/json"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://fcm.googleapis.com/fcm/send",
                    headers=headers,
                    json=payload,
                    timeout=10
                ) as response:
                    if response.status == 200:
                        return True
                    else:
                        logger.error("FCM API error", status=response.status, response=await response.text())
                        return False
                        
        except Exception as e:
            logger.error("FCM notification error", error=str(e))
            return False
    
    async def _get_user_fcm_token(self, user_id: str) -> Optional[str]:
        """Get user's FCM token from database."""
        # This would typically query the database for the user's FCM token
        # For now, return None as placeholder
        return None
    
    async def _get_users_by_role(self, role: str) -> List[str]:
        """Get all users with a specific role."""
        # This would typically query the database for users with the specified role
        # For now, return empty list as placeholder
        return []
    
    async def _get_andon_notification_users(
        self,
        line_id: str,
        equipment_code: str,
        severity: str
    ) -> List[str]:
        """Get users who should receive Andon notifications."""
        # This would typically query the database for:
        # - Line operators
        # - Equipment technicians
        # - Supervisors/managers based on severity
        # For now, return empty list as placeholder
        return []
    
    async def send_notification(
        self,
        user_id: str,
        title: str,
        message: str,
        notification_type: str = "info"
    ) -> bool:
        """Send generic notification to user via multiple channels."""
        try:
            # Get user's notification preferences
            user_prefs = await self._get_user_notification_preferences(user_id)
            
            success_count = 0
            
            # Send push notification if enabled
            if user_prefs.get("push_enabled", True):
                push_success = await self.send_push_notification(
                    user_id=user_id,
                    title=title,
                    body=message,
                    notification_type=notification_type
                )
                if push_success:
                    success_count += 1
            
            # Send email if enabled and email available
            if user_prefs.get("email_enabled", False) and user_prefs.get("email"):
                email_success = await self.send_email_notification(
                    email=user_prefs["email"],
                    subject=title,
                    body=message
                )
                if email_success:
                    success_count += 1
            
            # Send WebSocket notification if enabled
            if user_prefs.get("websocket_enabled", True):
                websocket_success = await self._send_websocket_notification(
                    user_id=user_id,
                    title=title,
                    message=message,
                    notification_type=notification_type
                )
                if websocket_success:
                    success_count += 1
            
            logger.info(
                "Generic notification sent",
                user_id=user_id,
                notification_type=notification_type,
                channels_used=success_count
            )
            
            return success_count > 0
            
        except Exception as e:
            logger.error(
                "Error sending generic notification",
                error=str(e),
                user_id=user_id,
                notification_type=notification_type
            )
            return False
    
    async def send_sms_notification(
        self,
        phone: str,
        message: str
    ) -> bool:
        """Send SMS notification."""
        try:
            # This would integrate with SMS service like Twilio, AWS SNS, etc.
            # For now, we'll simulate the SMS sending
            
            import aiohttp
            
            # Placeholder SMS API integration
            sms_payload = {
                "to": phone,
                "message": message,
                "from": getattr(settings, 'SMS_FROM_NUMBER', '+1234567890')
            }
            
            # In a real implementation, this would call the SMS service API
            logger.info(
                "SMS notification prepared",
                phone=phone,
                message_length=len(message)
            )
            
            # Simulate successful SMS sending
            return True
            
        except Exception as e:
            logger.error("Error sending SMS notification", error=str(e), phone=phone)
            return False
    
    async def _send_websocket_notification(
        self,
        user_id: str,
        title: str,
        message: str,
        notification_type: str = "info"
    ) -> bool:
        """Send WebSocket notification to user."""
        try:
            # This would integrate with the WebSocket manager
            # For now, we'll simulate the WebSocket notification
            
            websocket_payload = {
                "type": "notification",
                "user_id": user_id,
                "title": title,
                "message": message,
                "notification_type": notification_type,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # In a real implementation, this would send via WebSocket manager
            logger.info(
                "WebSocket notification prepared",
                user_id=user_id,
                notification_type=notification_type
            )
            
            return True
            
        except Exception as e:
            logger.error("Error sending WebSocket notification", error=str(e), user_id=user_id)
            return False
    
    async def _get_user_notification_preferences(self, user_id: str) -> Dict[str, Any]:
        """Get user's notification preferences from database."""
        # This would typically query the database for user preferences
        # For now, return default preferences
        return {
            "push_enabled": True,
            "email_enabled": False,
            "websocket_enabled": True,
            "sms_enabled": False,
            "email": None,
            "phone": None
        }

    async def _send_email_via_service(
        self,
        email: str,
        subject: str,
        body: str,
        html_body: Optional[str] = None
    ) -> bool:
        """Send email via configured email service."""
        # This would integrate with email service like SendGrid, AWS SES, etc.
        # For now, just log the email
        logger.info("Email would be sent", email=email, subject=subject)
        return True


# Global notification service instance
notification_service = NotificationService()


# Convenience functions for common notification types
async def send_andon_notification(
    line_id: str,
    equipment_code: str,
    event_type: str,
    severity: str,
    message: str
) -> bool:
    """Send Andon notification."""
    return await notification_service.send_andon_notification(
        line_id=line_id,
        equipment_code=equipment_code,
        event_type=event_type,
        severity=severity,
        message=message
    )


async def send_maintenance_reminder(
    equipment_code: str,
    maintenance_type: str,
    due_date: datetime,
    assigned_user_id: str
) -> bool:
    """Send maintenance reminder."""
    return await notification_service.send_maintenance_reminder(
        equipment_code=equipment_code,
        maintenance_type=maintenance_type,
        due_date=due_date,
        assigned_user_id=assigned_user_id
    )


async def send_quality_alert(
    line_id: str,
    defect_count: int,
    threshold: int,
    quality_manager_ids: List[str]
) -> bool:
    """Send quality alert."""
    return await notification_service.send_quality_alert(
        line_id=line_id,
        defect_count=defect_count,
        threshold=threshold,
        quality_manager_ids=quality_manager_ids
    )


# Phase 3 Implementation - Enhanced Notification Features

class EnhancedNotificationService(NotificationService):
    """Enhanced notification service with additional features."""
    
    async def send_scheduled_notification(
        self,
        user_id: str,
        title: str,
        message: str,
        scheduled_time: datetime,
        notification_type: str = "scheduled"
    ) -> bool:
        """Schedule a notification to be sent at a specific time."""
        try:
            delay_seconds = (scheduled_time - datetime.utcnow()).total_seconds()
            
            if delay_seconds <= 0:
                return await self.send_notification(
                    user_id=user_id,
                    title=title,
                    message=message,
                    notification_type=notification_type
                )
            
            asyncio.create_task(self._send_scheduled_notification_task(
                user_id=user_id,
                title=title,
                message=message,
                notification_type=notification_type,
                delay_seconds=delay_seconds
            ))
            
            logger.info("Notification scheduled", user_id=user_id, scheduled_time=scheduled_time)
            return True
            
        except Exception as e:
            logger.error("Error scheduling notification", error=str(e), user_id=user_id)
            return False
    
    async def _send_scheduled_notification_task(
        self,
        user_id: str,
        title: str,
        message: str,
        notification_type: str,
        delay_seconds: float
    ):
        """Background task to send scheduled notification."""
        try:
            await asyncio.sleep(delay_seconds)
            await self.send_notification(
                user_id=user_id,
                title=title,
                message=message,
                notification_type=notification_type
            )
            logger.info("Scheduled notification sent", user_id=user_id, title=title)
        except Exception as e:
            logger.error("Error sending scheduled notification", error=str(e), user_id=user_id)
    
    async def send_escalation_notification(
        self,
        event_id: str,
        escalation_level: int,
        event_type: str,
        equipment_code: str,
        message: str
    ) -> bool:
        """Send escalation notification for Andon events."""
        try:
            recipients = await self._get_escalation_recipients(escalation_level, event_type)
            
            if not recipients:
                logger.warning("No recipients found for escalation", level=escalation_level)
                return False
            
            title = f"Escalation Level {escalation_level} - {equipment_code}"
            body = f"{event_type}: {message}"
            
            data = {
                "event_id": event_id,
                "escalation_level": escalation_level,
                "event_type": event_type,
                "equipment_code": equipment_code,
                "action_required": True,
                "urgent": escalation_level >= 3
            }
            
            success_count = 0
            for recipient in recipients:
                if recipient["type"] == "user":
                    success = await self.send_push_notification(
                        user_id=recipient["id"],
                        title=title,
                        body=body,
                        data=data,
                        notification_type="escalation"
                    )
                    if success:
                        success_count += 1
                        
                elif recipient["type"] == "email":
                    success = await self.send_email_notification(
                        email=recipient["email"],
                        subject=title,
                        body=body
                    )
                    if success:
                        success_count += 1
            
            logger.info("Escalation notification sent", event_id=event_id, successful=success_count)
            return success_count > 0
            
        except Exception as e:
            logger.error("Error sending escalation notification", error=str(e), event_id=event_id)
            return False
    
    async def send_daily_summary_notification(
        self,
        user_id: str,
        summary_data: Dict[str, Any]
    ) -> bool:
        """Send daily summary notification to user."""
        try:
            title = f"Daily Summary - {datetime.utcnow().strftime('%Y-%m-%d')}"
            
            lines = []
            lines.append("📊 Daily Production Summary")
            lines.append("")
            
            if "oee" in summary_data:
                oee = summary_data["oee"]
                lines.append(f"🏭 Overall OEE: {oee:.1%}")
            
            if "production" in summary_data:
                prod = summary_data["production"]
                lines.append(f"📦 Production: {prod.get('completed', 0)}/{prod.get('target', 0)} units")
            
            if "andon_events" in summary_data:
                events = summary_data["andon_events"]
                lines.append(f"🚨 Andon Events: {events.get('total', 0)} ({events.get('resolved', 0)} resolved)")
            
            lines.append("")
            lines.append("View full dashboard for detailed analytics.")
            
            message = "\n".join(lines)
            
            data = {
                "summary_data": summary_data,
                "date": datetime.utcnow().isoformat(),
                "type": "daily_summary"
            }
            
            return await self.send_push_notification(
                user_id=user_id,
                title=title,
                body=message,
                data=data,
                notification_type="daily_summary"
            )
            
        except Exception as e:
            logger.error("Error sending daily summary", error=str(e), user_id=user_id)
            return False
    
    async def _get_escalation_recipients(
        self,
        escalation_level: int,
        event_type: str
    ) -> List[Dict[str, Any]]:
        """Get recipients for escalation based on level and event type."""
        try:
            recipients = []
            
            if escalation_level >= 1:
                recipients.append({"type": "user", "id": "operator_1", "role": "operator"})
            
            if escalation_level >= 2:
                recipients.append({"type": "user", "id": "supervisor_1", "role": "supervisor"})
                recipients.append({"type": "email", "email": "supervisor@company.com", "role": "supervisor"})
            
            if escalation_level >= 3:
                recipients.append({"type": "user", "id": "manager_1", "role": "manager"})
            
            if escalation_level >= 4:
                recipients.append({"type": "email", "email": "director@company.com", "role": "director"})
            
            return recipients
            
        except Exception as e:
            logger.error("Error getting escalation recipients", error=str(e), level=escalation_level)
            return []


# Enhanced notification service instance
enhanced_notification_service = EnhancedNotificationService()
