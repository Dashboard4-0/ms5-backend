"""
MS5.0 Floor Dashboard - Andon Escalation Monitor

This module provides background monitoring and automatic processing of
Andon escalations including timeout handling and automatic escalation.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from uuid import UUID
import structlog

from app.database import execute_query, execute_scalar
from app.services.andon_escalation_service import AndonEscalationService
from app.services.notification_service import NotificationService

logger = structlog.get_logger()


class AndonEscalationMonitor:
    """Background monitor for Andon escalations."""
    
    def __init__(self, check_interval_seconds: int = 60):
        self.check_interval = check_interval_seconds
        self.is_running = False
        self.task = None
    
    async def start(self) -> None:
        """Start the escalation monitor."""
        if self.is_running:
            logger.warning("Escalation monitor is already running")
            return
        
        self.is_running = True
        self.task = asyncio.create_task(self._monitor_loop())
        
        logger.info("Andon escalation monitor started", check_interval=self.check_interval)
    
    async def stop(self) -> None:
        """Stop the escalation monitor."""
        if not self.is_running:
            logger.warning("Escalation monitor is not running")
            return
        
        self.is_running = False
        
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        
        logger.info("Andon escalation monitor stopped")
    
    async def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while self.is_running:
            try:
                await self._process_escalations()
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in escalation monitor loop", error=str(e))
                await asyncio.sleep(self.check_interval)
    
    async def _process_escalations(self) -> None:
        """Process escalations for timeout and automatic escalation."""
        try:
            # Process automatic escalations
            processed_count = await AndonEscalationService.process_automatic_escalations()
            
            if processed_count > 0:
                logger.info("Processed automatic escalations", count=processed_count)
            
            # Check for overdue escalations
            overdue_count = await self._check_overdue_escalations()
            
            if overdue_count > 0:
                logger.warning("Found overdue escalations", count=overdue_count)
            
            # Send reminder notifications
            await self._send_reminder_notifications()
            
        except Exception as e:
            logger.error("Error processing escalations", error=str(e))
    
    async def _check_overdue_escalations(self) -> int:
        """Check for escalations that are overdue for acknowledgment or resolution."""
        try:
            # Get escalations that are overdue for acknowledgment
            overdue_ack_query = """
            SELECT COUNT(*) as overdue_count
            FROM factory_telemetry.andon_escalations ae
            WHERE ae.status = 'active'
            AND ae.acknowledged_at IS NULL
            AND ae.created_at < NOW() - INTERVAL '1 minute' * ae.acknowledgment_timeout_minutes
            """
            
            overdue_ack_result = await execute_scalar(overdue_ack_query)
            overdue_ack_count = overdue_ack_result or 0
            
            # Get escalations that are overdue for resolution
            overdue_res_query = """
            SELECT COUNT(*) as overdue_count
            FROM factory_telemetry.andon_escalations ae
            WHERE ae.status IN ('active', 'acknowledged')
            AND ae.resolved_at IS NULL
            AND ae.created_at < NOW() - INTERVAL '1 minute' * ae.resolution_timeout_minutes
            """
            
            overdue_res_result = await execute_scalar(overdue_res_query)
            overdue_res_count = overdue_res_result or 0
            
            total_overdue = overdue_ack_count + overdue_res_count
            
            if total_overdue > 0:
                logger.warning(
                    "Overdue escalations found",
                    overdue_acknowledgment=overdue_ack_count,
                    overdue_resolution=overdue_res_count,
                    total=total_overdue
                )
            
            return total_overdue
            
        except Exception as e:
            logger.error("Error checking overdue escalations", error=str(e))
            return 0
    
    async def _send_reminder_notifications(self) -> None:
        """Send reminder notifications for escalations approaching timeout."""
        try:
            # Get escalations approaching acknowledgment timeout (within 5 minutes)
            approaching_ack_query = """
            SELECT 
                ae.id as escalation_id,
                ae.event_id,
                ae.priority,
                ae.escalation_level,
                ae.escalation_recipients,
                ae_events.line_id,
                ae_events.equipment_code,
                ae_events.description,
                pl.line_code,
                pl.name as line_name,
                EXTRACT(EPOCH FROM (ae.created_at + INTERVAL '1 minute' * ae.acknowledgment_timeout_minutes - NOW()))/60 as minutes_remaining
            FROM factory_telemetry.andon_escalations ae
            JOIN factory_telemetry.andon_events ae_events ON ae.event_id = ae_events.id
            JOIN factory_telemetry.production_lines pl ON ae_events.line_id = pl.id
            WHERE ae.status = 'active'
            AND ae.acknowledged_at IS NULL
            AND ae.created_at + INTERVAL '1 minute' * ae.acknowledgment_timeout_minutes - NOW() BETWEEN 
                INTERVAL '0 minutes' AND INTERVAL '5 minutes'
            AND ae.last_reminder_sent_at IS NULL OR ae.last_reminder_sent_at < NOW() - INTERVAL '5 minutes'
            """
            
            approaching_ack = await execute_query(approaching_ack_query)
            
            for escalation in approaching_ack:
                await self._send_acknowledgment_reminder(escalation)
            
            # Get escalations approaching resolution timeout (within 10 minutes)
            approaching_res_query = """
            SELECT 
                ae.id as escalation_id,
                ae.event_id,
                ae.priority,
                ae.escalation_level,
                ae.escalation_recipients,
                ae_events.line_id,
                ae_events.equipment_code,
                ae_events.description,
                pl.line_code,
                pl.name as line_name,
                EXTRACT(EPOCH FROM (ae.created_at + INTERVAL '1 minute' * ae.resolution_timeout_minutes - NOW()))/60 as minutes_remaining
            FROM factory_telemetry.andon_escalations ae
            JOIN factory_telemetry.andon_events ae_events ON ae.event_id = ae_events.id
            JOIN factory_telemetry.production_lines pl ON ae_events.line_id = pl.id
            WHERE ae.status IN ('active', 'acknowledged')
            AND ae.resolved_at IS NULL
            AND ae.created_at + INTERVAL '1 minute' * ae.resolution_timeout_minutes - NOW() BETWEEN 
                INTERVAL '0 minutes' AND INTERVAL '10 minutes'
            AND ae.last_reminder_sent_at IS NULL OR ae.last_reminder_sent_at < NOW() - INTERVAL '10 minutes'
            """
            
            approaching_res = await execute_query(approaching_res_query)
            
            for escalation in approaching_res:
                await self._send_resolution_reminder(escalation)
            
        except Exception as e:
            logger.error("Error sending reminder notifications", error=str(e))
    
    async def _send_acknowledgment_reminder(self, escalation: Dict[str, Any]) -> None:
        """Send acknowledgment reminder notification."""
        try:
            minutes_remaining = int(escalation["minutes_remaining"])
            
            message = (
                f"URGENT: Andon escalation requires acknowledgment!\n\n"
                f"Line: {escalation['line_code']} - {escalation['line_name']}\n"
                f"Equipment: {escalation['equipment_code']}\n"
                f"Priority: {escalation['priority'].upper()}\n"
                f"Description: {escalation['description']}\n"
                f"Time remaining: {minutes_remaining} minutes\n\n"
                f"Please acknowledge this escalation immediately."
            )
            
            # Send to escalation recipients
            for recipient_role in escalation["escalation_recipients"]:
                await NotificationService.send_notification(
                    recipient_role=recipient_role,
                    notification_type="andon_escalation_reminder",
                    title="Andon Escalation Reminder",
                    message=message,
                    data={
                        "escalation_id": str(escalation["escalation_id"]),
                        "event_id": str(escalation["event_id"]),
                        "priority": escalation["priority"],
                        "minutes_remaining": minutes_remaining,
                        "reminder_type": "acknowledgment"
                    }
                )
            
            # Update last reminder sent timestamp
            update_query = """
            UPDATE factory_telemetry.andon_escalations 
            SET last_reminder_sent_at = NOW()
            WHERE id = :escalation_id
            """
            
            await execute_query(update_query, {"escalation_id": escalation["escalation_id"]})
            
            logger.info(
                "Acknowledgment reminder sent",
                escalation_id=escalation["escalation_id"],
                minutes_remaining=minutes_remaining
            )
            
        except Exception as e:
            logger.error("Error sending acknowledgment reminder", error=str(e), escalation_id=escalation["escalation_id"])
    
    async def _send_resolution_reminder(self, escalation: Dict[str, Any]) -> None:
        """Send resolution reminder notification."""
        try:
            minutes_remaining = int(escalation["minutes_remaining"])
            
            message = (
                f"URGENT: Andon escalation requires resolution!\n\n"
                f"Line: {escalation['line_code']} - {escalation['line_name']}\n"
                f"Equipment: {escalation['equipment_code']}\n"
                f"Priority: {escalation['priority'].upper()}\n"
                f"Description: {escalation['description']}\n"
                f"Time remaining: {minutes_remaining} minutes\n\n"
                f"Please resolve this escalation immediately or escalate to next level."
            )
            
            # Send to escalation recipients
            for recipient_role in escalation["escalation_recipients"]:
                await NotificationService.send_notification(
                    recipient_role=recipient_role,
                    notification_type="andon_escalation_reminder",
                    title="Andon Escalation Resolution Reminder",
                    message=message,
                    data={
                        "escalation_id": str(escalation["escalation_id"]),
                        "event_id": str(escalation["event_id"]),
                        "priority": escalation["priority"],
                        "minutes_remaining": minutes_remaining,
                        "reminder_type": "resolution"
                    }
                )
            
            # Update last reminder sent timestamp
            update_query = """
            UPDATE factory_telemetry.andon_escalations 
            SET last_reminder_sent_at = NOW()
            WHERE id = :escalation_id
            """
            
            await execute_query(update_query, {"escalation_id": escalation["escalation_id"]})
            
            logger.info(
                "Resolution reminder sent",
                escalation_id=escalation["escalation_id"],
                minutes_remaining=minutes_remaining
            )
            
        except Exception as e:
            logger.error("Error sending resolution reminder", error=str(e), escalation_id=escalation["escalation_id"])
    
    async def get_monitoring_status(self) -> Dict[str, Any]:
        """Get current monitoring status."""
        try:
            # Get active escalations count
            active_count_query = """
            SELECT COUNT(*) as active_count
            FROM factory_telemetry.andon_escalations
            WHERE status IN ('active', 'acknowledged', 'escalated')
            """
            
            active_count = await execute_scalar(active_count_query) or 0
            
            # Get overdue escalations count
            overdue_count = await self._check_overdue_escalations()
            
            # Get escalations processed in last hour
            processed_count_query = """
            SELECT COUNT(*) as processed_count
            FROM factory_telemetry.andon_escalations
            WHERE escalated_at >= NOW() - INTERVAL '1 hour'
            AND status = 'escalated'
            """
            
            processed_count = await execute_scalar(processed_count_query) or 0
            
            return {
                "monitor_running": self.is_running,
                "check_interval_seconds": self.check_interval,
                "active_escalations": active_count,
                "overdue_escalations": overdue_count,
                "escalations_processed_last_hour": processed_count,
                "last_check": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error("Error getting monitoring status", error=str(e))
            return {
                "monitor_running": self.is_running,
                "error": str(e)
            }


# Global monitor instance
escalation_monitor = AndonEscalationMonitor()


async def start_escalation_monitor() -> None:
    """Start the global escalation monitor."""
    await escalation_monitor.start()


async def stop_escalation_monitor() -> None:
    """Stop the global escalation monitor."""
    await escalation_monitor.stop()


async def get_escalation_monitor_status() -> Dict[str, Any]:
    """Get the global escalation monitor status."""
    return await escalation_monitor.get_monitoring_status()
