"""
MS5.0 Floor Dashboard - Andon Escalation Service

This module provides comprehensive Andon escalation management including
automatic escalation, notification management, and escalation monitoring.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from uuid import UUID
import asyncio
import structlog

from app.database import execute_query, execute_scalar, execute_update
from app.models.production import (
    AndonEventResponse, AndonPriority, AndonStatus
)
from app.utils.exceptions import (
    NotFoundError, ValidationError, BusinessLogicError, ConflictError
)

logger = structlog.get_logger()


class AndonEscalationService:
    """Service for Andon escalation management."""
    
    @staticmethod
    async def create_escalation(
        event_id: UUID,
        priority: AndonPriority,
        acknowledgment_timeout_minutes: int = None,
        resolution_timeout_minutes: int = None
    ) -> Dict[str, Any]:
        """Create a new escalation for an Andon event."""
        try:
            # Get escalation rules for the priority
            escalation_rules = await AndonEscalationService._get_escalation_rules(priority)
            
            if not escalation_rules:
                raise BusinessLogicError(f"No escalation rules found for priority: {priority.value}")
            
            # Use first level escalation rule
            first_rule = escalation_rules[0]
            
            # Set timeouts from rule or parameters
            ack_timeout = acknowledgment_timeout_minutes or first_rule["delay_minutes"]
            res_timeout = resolution_timeout_minutes or first_rule["delay_minutes"] * 4
            
            # Create escalation record
            create_query = """
            INSERT INTO factory_telemetry.andon_escalations 
            (event_id, priority, acknowledgment_timeout_minutes, resolution_timeout_minutes,
             escalation_recipients, escalation_level, status, created_at)
            VALUES (:event_id, :priority, :ack_timeout, :res_timeout,
                   :recipients, :escalation_level, :status, :created_at)
            RETURNING id, event_id, priority, escalation_level, status, created_at
            """
            
            result = await execute_query(create_query, {
                "event_id": event_id,
                "priority": priority.value,
                "ack_timeout": ack_timeout,
                "res_timeout": res_timeout,
                "recipients": first_rule["recipients"],
                "escalation_level": 1,
                "status": "active",
                "created_at": datetime.utcnow()
            })
            
            if not result:
                raise BusinessLogicError("Failed to create escalation")
            
            escalation = result[0]
            
            # Log escalation creation
            await AndonEscalationService._log_escalation_action(
                escalation["id"], "created", None, "Escalation created automatically"
            )
            
            # Send initial notifications
            await AndonEscalationService._send_escalation_notifications(
                escalation["id"], first_rule["recipients"], first_rule["notification_methods"]
            )
            
            logger.info(
                "Andon escalation created",
                escalation_id=escalation["id"],
                event_id=event_id,
                priority=priority.value,
                escalation_level=1
            )
            
            return escalation
            
        except Exception as e:
            logger.error("Failed to create escalation", error=str(e), event_id=event_id)
            raise BusinessLogicError("Failed to create escalation")
    
    @staticmethod
    async def acknowledge_escalation(
        escalation_id: UUID,
        acknowledged_by: UUID,
        notes: str = None
    ) -> Dict[str, Any]:
        """Acknowledge an escalation."""
        try:
            # Get escalation details
            escalation = await AndonEscalationService._get_escalation(escalation_id)
            
            if escalation["status"] != "active":
                raise BusinessLogicError("Escalation cannot be acknowledged in current status")
            
            # Update escalation status
            update_query = """
            UPDATE factory_telemetry.andon_escalations 
            SET status = :status, acknowledged_by = :acknowledged_by, 
                acknowledged_at = :acknowledged_at
            WHERE id = :escalation_id
            """
            
            await execute_update(update_query, {
                "escalation_id": escalation_id,
                "status": "acknowledged",
                "acknowledged_by": acknowledged_by,
                "acknowledged_at": datetime.utcnow()
            })
            
            # Log acknowledgment
            await AndonEscalationService._log_escalation_action(
                escalation_id, "acknowledged", acknowledged_by, notes
            )
            
            # Send acknowledgment notifications
            await AndonEscalationService._send_acknowledgment_notifications(escalation_id)
            
            logger.info(
                "Escalation acknowledged",
                escalation_id=escalation_id,
                acknowledged_by=acknowledged_by
            )
            
            return await AndonEscalationService._get_escalation(escalation_id)
            
        except Exception as e:
            logger.error("Failed to acknowledge escalation", error=str(e), escalation_id=escalation_id)
            raise BusinessLogicError("Failed to acknowledge escalation")
    
    @staticmethod
    async def resolve_escalation(
        escalation_id: UUID,
        resolved_by: UUID,
        resolution_notes: str
    ) -> Dict[str, Any]:
        """Resolve an escalation."""
        try:
            # Get escalation details
            escalation = await AndonEscalationService._get_escalation(escalation_id)
            
            if escalation["status"] not in ["active", "acknowledged"]:
                raise BusinessLogicError("Escalation cannot be resolved in current status")
            
            # Update escalation status
            update_query = """
            UPDATE factory_telemetry.andon_escalations 
            SET status = :status, resolved_at = :resolved_at
            WHERE id = :escalation_id
            """
            
            await execute_update(update_query, {
                "escalation_id": escalation_id,
                "status": "resolved",
                "resolved_at": datetime.utcnow()
            })
            
            # Log resolution
            await AndonEscalationService._log_escalation_action(
                escalation_id, "resolved", resolved_by, resolution_notes
            )
            
            # Send resolution notifications
            await AndonEscalationService._send_resolution_notifications(escalation_id)
            
            logger.info(
                "Escalation resolved",
                escalation_id=escalation_id,
                resolved_by=resolved_by
            )
            
            return await AndonEscalationService._get_escalation(escalation_id)
            
        except Exception as e:
            logger.error("Failed to resolve escalation", error=str(e), escalation_id=escalation_id)
            raise BusinessLogicError("Failed to resolve escalation")
    
    @staticmethod
    async def escalate_manually(
        escalation_id: UUID,
        escalated_by: UUID,
        escalation_notes: str,
        target_level: int = None
    ) -> Dict[str, Any]:
        """Manually escalate to next level or specific level."""
        try:
            # Get escalation details
            escalation = await AndonEscalationService._get_escalation(escalation_id)
            
            if escalation["status"] not in ["active", "acknowledged"]:
                raise BusinessLogicError("Escalation cannot be escalated in current status")
            
            # Determine target escalation level
            if target_level is None:
                target_level = escalation["escalation_level"] + 1
            
            if target_level <= escalation["escalation_level"]:
                raise ValidationError("Target escalation level must be higher than current level")
            
            # Get escalation rule for target level
            escalation_rule = await AndonEscalationService._get_escalation_rule(
                AndonPriority(escalation["priority"]), target_level
            )
            
            if not escalation_rule:
                raise BusinessLogicError(f"No escalation rule found for level {target_level}")
            
            # Update escalation
            update_query = """
            UPDATE factory_telemetry.andon_escalations 
            SET escalation_level = :target_level, escalation_recipients = :recipients,
                escalated_at = :escalated_at, escalated_by = :escalated_by,
                escalation_notes = :escalation_notes, status = :status
            WHERE id = :escalation_id
            """
            
            await execute_update(update_query, {
                "escalation_id": escalation_id,
                "target_level": target_level,
                "recipients": escalation_rule["recipients"],
                "escalated_at": datetime.utcnow(),
                "escalated_by": escalated_by,
                "escalation_notes": escalation_notes,
                "status": "escalated"
            })
            
            # Log escalation
            await AndonEscalationService._log_escalation_action(
                escalation_id, "escalated", escalated_by, 
                f"Manually escalated to level {target_level}: {escalation_notes}",
                target_level, escalation_rule["recipients"]
            )
            
            # Send escalation notifications
            await AndonEscalationService._send_escalation_notifications(
                escalation_id, escalation_rule["recipients"], escalation_rule["notification_methods"]
            )
            
            logger.info(
                "Escalation manually escalated",
                escalation_id=escalation_id,
                escalated_by=escalated_by,
                target_level=target_level
            )
            
            return await AndonEscalationService._get_escalation(escalation_id)
            
        except Exception as e:
            logger.error("Failed to escalate manually", error=str(e), escalation_id=escalation_id)
            raise BusinessLogicError("Failed to escalate manually")
    
    @staticmethod
    async def get_active_escalations(
        line_id: Optional[UUID] = None,
        priority: Optional[AndonPriority] = None
    ) -> List[Dict[str, Any]]:
        """Get active escalations with filtering."""
        try:
            where_conditions = ["ae.status IN ('active', 'acknowledged', 'escalated')"]
            query_params = {}
            
            if line_id:
                where_conditions.append("ae_events.line_id = :line_id")
                query_params["line_id"] = line_id
            
            if priority:
                where_conditions.append("ae.priority = :priority")
                query_params["priority"] = priority.value
            
            where_clause = "WHERE " + " AND ".join(where_conditions)
            
            query = f"""
            SELECT 
                ae.id as escalation_id,
                ae.event_id,
                ae.priority,
                ae.escalation_level,
                ae.status as escalation_status,
                ae.created_at as escalation_created_at,
                ae.acknowledged_at,
                ae.escalated_at,
                ae.acknowledgment_timeout_minutes,
                ae.resolution_timeout_minutes,
                ae.escalation_recipients,
                ae.escalation_notes,
                ae.acknowledged_by,
                ae.escalated_by,
                ae_events.line_id,
                ae_events.equipment_code,
                ae_events.event_type,
                ae_events.description as event_description,
                ae_events.reported_at,
                ae_events.reported_by,
                u1.username as reported_by_username,
                u2.username as acknowledged_by_username,
                u3.username as escalated_by_username,
                pl.line_code,
                pl.name as line_name,
                -- Calculate time remaining for acknowledgment
                CASE 
                    WHEN ae.status = 'active' AND ae.acknowledged_at IS NULL THEN
                        GREATEST(0, ae.acknowledgment_timeout_minutes - EXTRACT(EPOCH FROM (NOW() - ae.created_at))/60)
                    ELSE NULL
                END as acknowledgment_time_remaining_minutes,
                -- Calculate time remaining for resolution
                CASE 
                    WHEN ae.status IN ('active', 'acknowledged') AND ae.resolved_at IS NULL THEN
                        GREATEST(0, ae.resolution_timeout_minutes - EXTRACT(EPOCH FROM (NOW() - ae.created_at))/60)
                    ELSE NULL
                END as resolution_time_remaining_minutes
            FROM factory_telemetry.andon_escalations ae
            JOIN factory_telemetry.andon_events ae_events ON ae.event_id = ae_events.id
            JOIN factory_telemetry.production_lines pl ON ae_events.line_id = pl.id
            LEFT JOIN factory_telemetry.users u1 ON ae_events.reported_by = u1.id
            LEFT JOIN factory_telemetry.users u2 ON ae.acknowledged_by = u2.id
            LEFT JOIN factory_telemetry.users u3 ON ae.escalated_by = u3.id
            {where_clause}
            ORDER BY ae.priority DESC, ae.created_at ASC
            """
            
            result = await execute_query(query, query_params)
            return result
            
        except Exception as e:
            logger.error("Failed to get active escalations", error=str(e))
            raise BusinessLogicError("Failed to get active escalations")
    
    @staticmethod
    async def get_escalation_history(
        escalation_id: UUID
    ) -> List[Dict[str, Any]]:
        """Get escalation history and timeline."""
        try:
            query = """
            SELECT 
                aeh.id,
                aeh.action,
                aeh.performed_by,
                aeh.performed_at,
                aeh.notes,
                aeh.escalation_level,
                aeh.recipients_notified,
                aeh.notification_method,
                u.username as performed_by_username
            FROM factory_telemetry.andon_escalation_history aeh
            LEFT JOIN factory_telemetry.users u ON aeh.performed_by = u.id
            WHERE aeh.escalation_id = :escalation_id
            ORDER BY aeh.performed_at ASC
            """
            
            result = await execute_query(query, {"escalation_id": escalation_id})
            return result
            
        except Exception as e:
            logger.error("Failed to get escalation history", error=str(e), escalation_id=escalation_id)
            raise BusinessLogicError("Failed to get escalation history")
    
    @staticmethod
    async def get_escalation_statistics(
        line_id: Optional[UUID] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get escalation statistics and analytics."""
        try:
            if not start_date:
                start_date = datetime.utcnow() - timedelta(days=7)
            if not end_date:
                end_date = datetime.utcnow()
            
            where_conditions = ["ae.created_at >= :start_date", "ae.created_at <= :end_date"]
            query_params = {
                "start_date": start_date,
                "end_date": end_date
            }
            
            if line_id:
                where_conditions.append("ae_events.line_id = :line_id")
                query_params["line_id"] = line_id
            
            where_clause = "WHERE " + " AND ".join(where_conditions)
            
            # Get escalation statistics
            stats_query = f"""
            SELECT 
                ae.priority,
                COUNT(*) as total_escalations,
                COUNT(CASE WHEN ae.status = 'resolved' THEN 1 END) as resolved_escalations,
                COUNT(CASE WHEN ae.status = 'escalated' THEN 1 END) as escalated_escalations,
                COUNT(CASE WHEN ae.status = 'active' THEN 1 END) as active_escalations,
                AVG(CASE 
                    WHEN ae.acknowledged_at IS NOT NULL THEN 
                        EXTRACT(EPOCH FROM (ae.acknowledged_at - ae.created_at))/60 
                    ELSE NULL 
                END) as avg_acknowledgment_time_minutes,
                AVG(CASE 
                    WHEN ae.resolved_at IS NOT NULL THEN 
                        EXTRACT(EPOCH FROM (ae.resolved_at - ae.created_at))/60 
                    ELSE NULL 
                END) as avg_resolution_time_minutes,
                MAX(ae.escalation_level) as max_escalation_level_reached
            FROM factory_telemetry.andon_escalations ae
            JOIN factory_telemetry.andon_events ae_events ON ae.event_id = ae_events.id
            {where_clause}
            GROUP BY ae.priority
            ORDER BY ae.priority
            """
            
            stats_result = await execute_query(stats_query, query_params)
            
            # Get escalation level distribution
            level_query = f"""
            SELECT 
                ae.escalation_level,
                COUNT(*) as count
            FROM factory_telemetry.andon_escalations ae
            JOIN factory_telemetry.andon_events ae_events ON ae.event_id = ae_events.id
            {where_clause}
            GROUP BY ae.escalation_level
            ORDER BY ae.escalation_level
            """
            
            level_result = await execute_query(level_query, query_params)
            
            return {
                "period": {
                    "start_date": start_date,
                    "end_date": end_date
                },
                "priority_breakdown": {row["priority"]: {
                    "total_escalations": row["total_escalations"],
                    "resolved_escalations": row["resolved_escalations"],
                    "escalated_escalations": row["escalated_escalations"],
                    "active_escalations": row["active_escalations"],
                    "avg_acknowledgment_time_minutes": round(row["avg_acknowledgment_time_minutes"], 2) if row["avg_acknowledgment_time_minutes"] else 0,
                    "avg_resolution_time_minutes": round(row["avg_resolution_time_minutes"], 2) if row["avg_resolution_time_minutes"] else 0,
                    "max_escalation_level_reached": row["max_escalation_level_reached"]
                } for row in stats_result},
                "level_distribution": {row["escalation_level"]: row["count"] for row in level_result}
            }
            
        except Exception as e:
            logger.error("Failed to get escalation statistics", error=str(e))
            raise BusinessLogicError("Failed to get escalation statistics")
    
    @staticmethod
    async def process_automatic_escalations() -> int:
        """Process automatic escalations based on timeouts."""
        try:
            # Call the database function to process automatic escalations
            query = "SELECT factory_telemetry.auto_escalate_andon_events()"
            await execute_scalar(query)
            
            # Get count of escalations that were processed
            count_query = """
            SELECT COUNT(*) as processed_count
            FROM factory_telemetry.andon_escalations
            WHERE escalated_at >= NOW() - INTERVAL '1 minute'
            AND status = 'escalated'
            """
            
            result = await execute_scalar(count_query)
            processed_count = result or 0
            
            logger.info("Automatic escalations processed", count=processed_count)
            return processed_count
            
        except Exception as e:
            logger.error("Failed to process automatic escalations", error=str(e))
            raise BusinessLogicError("Failed to process automatic escalations")
    
    @staticmethod
    async def _get_escalation(escalation_id: UUID) -> Dict[str, Any]:
        """Get escalation details by ID."""
        query = """
        SELECT 
            ae.id, ae.event_id, ae.priority, ae.escalation_level, ae.status,
            ae.acknowledgment_timeout_minutes, ae.resolution_timeout_minutes,
            ae.escalation_recipients, ae.escalation_notes, ae.created_at,
            ae.acknowledged_at, ae.escalated_at, ae.resolved_at,
            ae.acknowledged_by, ae.escalated_by
        FROM factory_telemetry.andon_escalations ae
        WHERE ae.id = :escalation_id
        """
        
        result = await execute_query(query, {"escalation_id": escalation_id})
        
        if not result:
            raise NotFoundError("Escalation", str(escalation_id))
        
        return result[0]
    
    @staticmethod
    async def _get_escalation_rules(priority: AndonPriority) -> List[Dict[str, Any]]:
        """Get escalation rules for a priority."""
        query = """
        SELECT priority, escalation_level, delay_minutes, recipients, 
               notification_methods, escalation_message_template
        FROM factory_telemetry.andon_escalation_rules
        WHERE priority = :priority AND enabled = true
        ORDER BY escalation_level ASC
        """
        
        result = await execute_query(query, {"priority": priority.value})
        return result
    
    @staticmethod
    async def _get_escalation_rule(priority: AndonPriority, level: int) -> Optional[Dict[str, Any]]:
        """Get specific escalation rule for priority and level."""
        query = """
        SELECT priority, escalation_level, delay_minutes, recipients, 
               notification_methods, escalation_message_template
        FROM factory_telemetry.andon_escalation_rules
        WHERE priority = :priority AND escalation_level = :level AND enabled = true
        """
        
        result = await execute_query(query, {
            "priority": priority.value,
            "level": level
        })
        
        return result[0] if result else None
    
    @staticmethod
    async def _log_escalation_action(
        escalation_id: UUID,
        action: str,
        performed_by: Optional[UUID],
        notes: str = None,
        escalation_level: int = None,
        recipients_notified: List[str] = None,
        notification_method: str = None
    ) -> None:
        """Log an escalation action."""
        query = """
        INSERT INTO factory_telemetry.andon_escalation_history
        (escalation_id, action, performed_by, performed_at, notes, 
         escalation_level, recipients_notified, notification_method)
        VALUES (:escalation_id, :action, :performed_by, :performed_at, :notes,
                :escalation_level, :recipients_notified, :notification_method)
        """
        
        await execute_update(query, {
            "escalation_id": escalation_id,
            "action": action,
            "performed_by": performed_by,
            "performed_at": datetime.utcnow(),
            "notes": notes,
            "escalation_level": escalation_level,
            "recipients_notified": recipients_notified or [],
            "notification_method": notification_method
        })
    
    @staticmethod
    async def _send_escalation_notifications(
        escalation_id: UUID,
        recipients: List[str],
        notification_methods: List[str]
    ) -> None:
        """Send escalation notifications to recipients."""
        try:
            # Get recipient contact information
            recipients_query = """
            SELECT role, name, email, phone, sms_enabled, email_enabled, 
                   websocket_enabled, push_enabled
            FROM factory_telemetry.andon_escalation_recipients
            WHERE role = ANY(:recipients) AND enabled = true
            """
            
            recipient_data = await execute_query(recipients_query, {"recipients": recipients})
            
            # Send notifications via each method
            for method in notification_methods:
                for recipient in recipient_data:
                    if method == "email" and recipient["email_enabled"] and recipient["email"]:
                        await AndonEscalationService._send_email_notification(
                            escalation_id, recipient, method
                        )
                    elif method == "sms" and recipient["sms_enabled"] and recipient["phone"]:
                        await AndonEscalationService._send_sms_notification(
                            escalation_id, recipient, method
                        )
                    elif method == "websocket" and recipient["websocket_enabled"]:
                        await AndonEscalationService._send_websocket_notification(
                            escalation_id, recipient, method
                        )
                    elif method == "push" and recipient["push_enabled"]:
                        await AndonEscalationService._send_push_notification(
                            escalation_id, recipient, method
                        )
            
            logger.info(
                "Escalation notifications sent",
                escalation_id=escalation_id,
                recipients=recipients,
                methods=notification_methods
            )
            
        except Exception as e:
            logger.error("Failed to send escalation notifications", error=str(e), escalation_id=escalation_id)
    
    @staticmethod
    async def _send_acknowledgment_notifications(escalation_id: UUID) -> None:
        """Send acknowledgment notifications."""
        # Implementation would send notifications about acknowledgment
        logger.info("Acknowledgment notifications sent", escalation_id=escalation_id)
    
    @staticmethod
    async def _send_resolution_notifications(escalation_id: UUID) -> None:
        """Send resolution notifications."""
        # Implementation would send notifications about resolution
        logger.info("Resolution notifications sent", escalation_id=escalation_id)
    
    @staticmethod
    async def _send_email_notification(escalation_id: UUID, recipient: Dict, method: str) -> None:
        """Send email notification."""
        # Implementation would send actual email
        logger.info("Email notification sent", escalation_id=escalation_id, recipient=recipient["role"])
    
    @staticmethod
    async def _send_sms_notification(escalation_id: UUID, recipient: Dict, method: str) -> None:
        """Send SMS notification."""
        # Implementation would send actual SMS
        logger.info("SMS notification sent", escalation_id=escalation_id, recipient=recipient["role"])
    
    @staticmethod
    async def _send_websocket_notification(escalation_id: UUID, recipient: Dict, method: str) -> None:
        """Send WebSocket notification."""
        # Implementation would send WebSocket message
        logger.info("WebSocket notification sent", escalation_id=escalation_id, recipient=recipient["role"])
    
    @staticmethod
    async def _send_push_notification(escalation_id: UUID, recipient: Dict, method: str) -> None:
        """Send push notification."""
        # Implementation would send actual push notification
        logger.info("Push notification sent", escalation_id=escalation_id, recipient=recipient["role"])
