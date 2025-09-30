"""
MS5.0 Floor Dashboard - WebSocket Handler

This module provides WebSocket endpoints for real-time updates
including production status, OEE data, and Andon events.
"""

import json
import asyncio
from typing import Dict, List, Set, Optional
from uuid import UUID

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query, HTTPException
from fastapi.websockets import WebSocketState
import structlog

from app.auth.jwt_handler import verify_access_token, JWTError
from app.utils.exceptions import AuthenticationError
from app.services.websocket_manager import websocket_manager

logger = structlog.get_logger()

router = APIRouter()




# Global connection manager is now imported from websocket_manager


async def authenticate_websocket(websocket: WebSocket, token: str) -> Optional[str]:
    """Authenticate WebSocket connection using JWT token."""
    try:
        payload = verify_access_token(token)
        user_id = payload.get("user_id")
        
        if not user_id:
            await websocket.close(code=1008, reason="Invalid token")
            return None
        
        return user_id
        
    except JWTError:
        await websocket.close(code=1008, reason="Invalid token")
        return None
    except Exception as e:
        logger.error("WebSocket authentication failed", error=str(e))
        await websocket.close(code=1011, reason="Authentication error")
        return None


@router.websocket("/")
async def websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(..., description="JWT authentication token")
):
    """Main WebSocket endpoint for real-time updates."""
    user_id = await authenticate_websocket(websocket, token)
    if not user_id:
        return
    
    connection_id = await websocket_manager.add_connection(websocket, user_id)
    
    try:
        while True:
            # Wait for messages from client
            data = await websocket.receive_text()
            
            try:
                message = json.loads(data)
                await handle_websocket_message(connection_id, message)
            except json.JSONDecodeError:
                await websocket_manager.send_personal_message({
                    "type": "error",
                    "message": "Invalid JSON message"
                }, connection_id)
            except Exception as e:
                logger.error("Error handling WebSocket message", error=str(e), connection_id=connection_id)
                await websocket_manager.send_personal_message({
                    "type": "error",
                    "message": "Error processing message"
                }, connection_id)
    
    except WebSocketDisconnect:
        websocket_manager.remove_connection(connection_id)
    except Exception as e:
        logger.error("WebSocket error", error=str(e), connection_id=connection_id)
        websocket_manager.remove_connection(connection_id)


async def handle_websocket_message(connection_id: str, message: dict):
    """Handle incoming WebSocket messages."""
    message_type = message.get("type")
    
    if message_type == "subscribe":
        await handle_subscribe_message(connection_id, message)
    elif message_type == "unsubscribe":
        await handle_unsubscribe_message(connection_id, message)
    elif message_type == "ping":
        await handle_ping_message(connection_id)
    else:
        await websocket_manager.send_personal_message({
            "type": "error",
            "message": f"Unknown message type: {message_type}"
        }, connection_id)


async def handle_subscribe_message(connection_id: str, message: dict):
    """Handle subscription requests."""
    subscription_type = message.get("subscription_type")
    target_id = message.get("target_id")
    
    if not subscription_type or not target_id:
        await websocket_manager.send_personal_message({
            "type": "error",
            "message": "Missing subscription_type or target_id"
        }, connection_id)
        return
    
    if subscription_type == "line":
        websocket_manager.subscribe_to_line(connection_id, target_id)
        await websocket_manager.send_personal_message({
            "type": "subscription_confirmed",
            "subscription_type": "line",
            "target_id": target_id
        }, connection_id)
    
    elif subscription_type == "equipment":
        websocket_manager.subscribe_to_equipment(connection_id, target_id)
        await websocket_manager.send_personal_message({
            "type": "subscription_confirmed",
            "subscription_type": "equipment",
            "target_id": target_id
        }, connection_id)
    
    elif subscription_type == "downtime":
        # For downtime subscriptions, target_id can be "all", line_id, or equipment_code
        if target_id == "all":
            websocket_manager.subscribe_to_downtime(connection_id)
        elif target_id.startswith("line:"):
            line_id = target_id[5:]  # Remove "line:" prefix
            websocket_manager.subscribe_to_downtime(connection_id, line_id=line_id)
        elif target_id.startswith("equipment:"):
            equipment_code = target_id[10:]  # Remove "equipment:" prefix
            websocket_manager.subscribe_to_downtime(connection_id, equipment_code=equipment_code)
        else:
            # Assume it's a line_id if no prefix
            websocket_manager.subscribe_to_downtime(connection_id, line_id=target_id)
        
        await websocket_manager.send_personal_message({
            "type": "subscription_confirmed",
            "subscription_type": "downtime",
            "target_id": target_id
        }, connection_id)
    
    elif subscription_type == "escalation":
        # For escalation subscriptions, target_id can be "all", escalation_id, or priority
        if target_id == "all":
            websocket_manager.subscribe_to_escalation(connection_id)
        elif target_id.startswith("escalation:"):
            escalation_id = target_id[11:]  # Remove "escalation:" prefix
            websocket_manager.subscribe_to_escalation(connection_id, escalation_id=escalation_id)
        elif target_id.startswith("priority:"):
            priority = target_id[9:]  # Remove "priority:" prefix
            websocket_manager.subscribe_to_escalation(connection_id, priority=priority)
        else:
            # Assume it's a priority if no prefix
            websocket_manager.subscribe_to_escalation(connection_id, priority=target_id)
        
        await websocket_manager.send_personal_message({
            "type": "subscription_confirmed",
            "subscription_type": "escalation",
            "target_id": target_id
        }, connection_id)
    
    else:
        await websocket_manager.send_personal_message({
            "type": "error",
            "message": f"Unknown subscription type: {subscription_type}"
        }, connection_id)


async def handle_unsubscribe_message(connection_id: str, message: dict):
    """Handle unsubscription requests."""
    subscription_type = message.get("subscription_type")
    target_id = message.get("target_id")
    
    if not subscription_type or not target_id:
        await websocket_manager.send_personal_message({
            "type": "error",
            "message": "Missing subscription_type or target_id"
        }, connection_id)
        return
    
    if subscription_type == "line":
        websocket_manager.unsubscribe_from_line(connection_id, target_id)
        await websocket_manager.send_personal_message({
            "type": "unsubscription_confirmed",
            "subscription_type": "line",
            "target_id": target_id
        }, connection_id)
    
    elif subscription_type == "equipment":
        websocket_manager.unsubscribe_from_equipment(connection_id, target_id)
        await websocket_manager.send_personal_message({
            "type": "unsubscription_confirmed",
            "subscription_type": "equipment",
            "target_id": target_id
        }, connection_id)
    
    elif subscription_type == "downtime":
        # For downtime unsubscriptions, target_id can be "all", line_id, or equipment_code
        if target_id == "all":
            websocket_manager.unsubscribe_from_downtime(connection_id)
        elif target_id.startswith("line:"):
            line_id = target_id[5:]  # Remove "line:" prefix
            websocket_manager.unsubscribe_from_downtime(connection_id, line_id=line_id)
        elif target_id.startswith("equipment:"):
            equipment_code = target_id[10:]  # Remove "equipment:" prefix
            websocket_manager.unsubscribe_from_downtime(connection_id, equipment_code=equipment_code)
        else:
            # Assume it's a line_id if no prefix
            websocket_manager.unsubscribe_from_downtime(connection_id, line_id=target_id)
        
        await websocket_manager.send_personal_message({
            "type": "unsubscription_confirmed",
            "subscription_type": "downtime",
            "target_id": target_id
        }, connection_id)
    
    elif subscription_type == "escalation":
        # For escalation unsubscriptions, target_id can be "all", escalation_id, or priority
        if target_id == "all":
            websocket_manager.unsubscribe_from_escalation(connection_id)
        elif target_id.startswith("escalation:"):
            escalation_id = target_id[11:]  # Remove "escalation:" prefix
            websocket_manager.unsubscribe_from_escalation(connection_id, escalation_id=escalation_id)
        elif target_id.startswith("priority:"):
            priority = target_id[9:]  # Remove "priority:" prefix
            websocket_manager.unsubscribe_from_escalation(connection_id, priority=priority)
        else:
            # Assume it's a priority if no prefix
            websocket_manager.unsubscribe_from_escalation(connection_id, priority=target_id)
        
        await websocket_manager.send_personal_message({
            "type": "unsubscription_confirmed",
            "subscription_type": "escalation",
            "target_id": target_id
        }, connection_id)
    
    else:
        await websocket_manager.send_personal_message({
            "type": "error",
            "message": f"Unknown subscription type: {subscription_type}"
        }, connection_id)


async def handle_ping_message(connection_id: str):
    """Handle ping messages for connection health checks."""
    await websocket_manager.send_personal_message({
        "type": "pong",
        "timestamp": "2025-01-20T10:00:00Z"
    }, connection_id)


# Event broadcasting functions
async def broadcast_line_status_update(line_id: str, status_data: dict):
    """Broadcast line status update to all subscribers."""
    message = {
        "type": "line_status_update",
        "line_id": line_id,
        "data": status_data,
        "timestamp": "2025-01-20T10:00:00Z"
    }
    await websocket_manager.send_to_line(message, line_id)


async def broadcast_equipment_status_update(equipment_code: str, status_data: dict):
    """Broadcast equipment status update to all subscribers."""
    message = {
        "type": "equipment_status_update",
        "equipment_code": equipment_code,
        "data": status_data,
        "timestamp": "2025-01-20T10:00:00Z"
    }
    await websocket_manager.send_to_equipment(message, equipment_code)


async def broadcast_andon_event(andon_event: dict):
    """Broadcast Andon event to relevant subscribers."""
    message = {
        "type": "andon_event",
        "data": andon_event,
        "timestamp": "2025-01-20T10:00:00Z"
    }
    
    # Send to line subscribers
    line_id = andon_event.get("line_id")
    if line_id:
        await websocket_manager.send_to_line(message, line_id)
    
    # Send to equipment subscribers
    equipment_code = andon_event.get("equipment_code")
    if equipment_code:
        await websocket_manager.send_to_equipment(message, equipment_code)


async def broadcast_oee_update(line_id: str, oee_data: dict):
    """Broadcast OEE update to line subscribers."""
    message = {
        "type": "oee_update",
        "line_id": line_id,
        "data": oee_data,
        "timestamp": "2025-01-20T10:00:00Z"
    }
    await websocket_manager.send_to_line(message, line_id)


async def broadcast_downtime_event(downtime_event: dict):
    """Broadcast downtime event to relevant subscribers."""
    from datetime import datetime
    
    message = {
        "type": "downtime_event",
        "data": downtime_event,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    # Send to downtime-specific subscribers
    line_id = downtime_event.get("line_id")
    equipment_code = downtime_event.get("equipment_code")
    
    await websocket_manager.send_to_downtime_subscribers(message, line_id, equipment_code)
    
    # Also send to general line and equipment subscribers for backward compatibility
    if line_id:
        await websocket_manager.send_to_line(message, line_id)
    
    if equipment_code:
        await websocket_manager.send_to_equipment(message, equipment_code)


async def broadcast_downtime_statistics_update(statistics_data: dict, line_id: str = None, equipment_code: str = None):
    """Broadcast downtime statistics update to relevant subscribers."""
    from datetime import datetime
    
    message = {
        "type": "downtime_statistics_update",
        "data": statistics_data,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    # Send to downtime-specific subscribers
    await websocket_manager.send_to_downtime_subscribers(message, line_id, equipment_code)
    
    # Also send to general line and equipment subscribers for backward compatibility
    if line_id:
        await websocket_manager.send_to_line(message, line_id)
    
    if equipment_code:
        await websocket_manager.send_to_equipment(message, equipment_code)


async def broadcast_job_update(job_data: dict):
    """Broadcast job assignment update to relevant subscribers."""
    message = {
        "type": "job_update",
        "data": job_data,
        "timestamp": "2025-01-20T10:00:00Z"
    }
    
    # Send to line subscribers
    line_id = job_data.get("line_id")
    if line_id:
        await websocket_manager.send_to_line(message, line_id)
    
    # Send to user if they have an active connection
    user_id = job_data.get("user_id")
    if user_id:
        # Find connection for user
        for connection_id, websocket in websocket_manager.connections.items():
            if connection_id.startswith(user_id):
                await websocket_manager.send_personal_message(message, connection_id)
                break


async def broadcast_system_alert(alert_data: dict):
    """Broadcast system-wide alert to all connections."""
    message = {
        "type": "system_alert",
        "data": alert_data,
        "timestamp": "2025-01-20T10:00:00Z"
    }
    await websocket_manager.broadcast(message)


async def broadcast_escalation_event(escalation_data: dict):
    """Broadcast escalation event to relevant subscribers."""
    from datetime import datetime
    
    message = {
        "type": "escalation_event",
        "data": escalation_data,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    # Send to escalation-specific subscribers
    escalation_id = escalation_data.get("escalation_id")
    priority = escalation_data.get("priority")
    
    await websocket_manager.send_to_escalation_subscribers(message, escalation_id, priority)
    
    # Also send to line subscribers for context
    line_id = escalation_data.get("line_id")
    if line_id:
        await websocket_manager.send_to_line(message, line_id)


async def broadcast_escalation_status_update(escalation_id: str, status_data: dict):
    """Broadcast escalation status update to subscribers."""
    from datetime import datetime
    
    message = {
        "type": "escalation_status_update",
        "escalation_id": escalation_id,
        "data": status_data,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    await websocket_manager.send_to_escalation_subscribers(message, escalation_id)


async def broadcast_escalation_reminder(reminder_data: dict):
    """Broadcast escalation reminder to relevant subscribers."""
    from datetime import datetime
    
    message = {
        "type": "escalation_reminder",
        "data": reminder_data,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    # Send to escalation-specific subscribers
    escalation_id = reminder_data.get("escalation_id")
    priority = reminder_data.get("priority")
    
    await websocket_manager.send_to_escalation_subscribers(message, escalation_id, priority)
    
    # Also send to line subscribers for context
    line_id = reminder_data.get("line_id")
    if line_id:
        await websocket_manager.send_to_line(message, line_id)


# Additional broadcasting functions for enhanced real-time features
async def broadcast_equipment_status_change(equipment_code: str, status_data: dict):
    """Broadcast equipment status change to subscribers."""
    message = {
        "type": "equipment_status_change",
        "equipment_code": equipment_code,
        "data": status_data,
        "timestamp": "2025-01-20T10:00:00Z"
    }
    await websocket_manager.send_to_equipment(message, equipment_code)


async def broadcast_andon_alert(andon_alert: dict):
    """Broadcast Andon alert to relevant subscribers."""
    message = {
        "type": "andon_alert",
        "data": andon_alert,
        "timestamp": "2025-01-20T10:00:00Z"
    }
    
    # Send to line subscribers
    line_id = andon_alert.get("line_id")
    if line_id:
        await websocket_manager.send_to_line(message, line_id)
    
    # Send to equipment subscribers
    equipment_code = andon_alert.get("equipment_code")
    if equipment_code:
        await websocket_manager.send_to_equipment(message, equipment_code)


async def broadcast_quality_update(quality_data: dict):
    """Broadcast quality update to relevant subscribers."""
    message = {
        "type": "quality_update",
        "data": quality_data,
        "timestamp": "2025-01-20T10:00:00Z"
    }
    
    # Send to line subscribers
    line_id = quality_data.get("line_id")
    if line_id:
        await websocket_manager.send_to_line(message, line_id)


async def broadcast_changeover_update(changeover_data: dict):
    """Broadcast changeover update to relevant subscribers."""
    message = {
        "type": "changeover_update",
        "data": changeover_data,
        "timestamp": "2025-01-20T10:00:00Z"
    }
    
    # Send to line subscribers
    line_id = changeover_data.get("line_id")
    if line_id:
        await websocket_manager.send_to_line(message, line_id)


# Health check endpoint
@router.get("/health")
async def websocket_health():
    """WebSocket health check endpoint."""
    stats = websocket_manager.get_connection_stats()
    return {
        "status": "healthy",
        "service": "websocket",
        "stats": stats
    }
