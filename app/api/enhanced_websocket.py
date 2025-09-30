"""
MS5.0 Floor Dashboard - Enhanced WebSocket Handler

This module provides enhanced WebSocket endpoints for real-time production updates
including job management, OEE data, downtime tracking, and Andon events.
"""

import json
import asyncio
from typing import Dict, List, Set, Optional, Any
from uuid import UUID

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query, HTTPException
from fastapi.websockets import WebSocketState
import structlog

from app.auth.jwt_handler import verify_access_token, JWTError
from app.utils.exceptions import AuthenticationError, WebSocketError
from app.services.enhanced_websocket_manager import EnhancedWebSocketManager

logger = structlog.get_logger()

router = APIRouter()

# Global enhanced connection manager
enhanced_manager = EnhancedWebSocketManager()


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


@router.websocket("/production")
async def production_websocket(
    websocket: WebSocket,
    token: str = Query(..., description="JWT authentication token")
):
    """Enhanced WebSocket endpoint for production-specific real-time updates."""
    user_id = await authenticate_websocket(websocket, token)
    if not user_id:
        return
    
    connection_id = await enhanced_manager.connect(websocket, user_id)
    
    try:
        while True:
            # Wait for messages from client
            data = await websocket.receive_text()
            
            try:
                message = json.loads(data)
                await handle_enhanced_websocket_message(connection_id, message)
            except json.JSONDecodeError:
                await enhanced_manager.send_personal_message({
                    "type": "error",
                    "message": "Invalid JSON message"
                }, connection_id)
            except Exception as e:
                logger.error("Error handling enhanced WebSocket message", 
                           error=str(e), connection_id=connection_id)
                await enhanced_manager.send_personal_message({
                    "type": "error",
                    "message": "Error processing message"
                }, connection_id)
    
    except WebSocketDisconnect:
        enhanced_manager.disconnect(connection_id)
    except Exception as e:
        logger.error("Enhanced WebSocket error", error=str(e), connection_id=connection_id)
        enhanced_manager.disconnect(connection_id)


async def handle_enhanced_websocket_message(connection_id: str, message: dict):
    """Handle incoming enhanced WebSocket messages."""
    message_type = message.get("type")
    
    if message_type == "subscribe":
        await handle_enhanced_subscribe_message(connection_id, message)
    elif message_type == "unsubscribe":
        await handle_enhanced_unsubscribe_message(connection_id, message)
    elif message_type == "ping":
        await handle_ping_message(connection_id)
    elif message_type == "get_stats":
        await handle_get_stats_message(connection_id)
    elif message_type == "get_subscriptions":
        await handle_get_subscriptions_message(connection_id)
    else:
        await enhanced_manager.send_personal_message({
            "type": "error",
            "message": f"Unknown message type: {message_type}"
        }, connection_id)


async def handle_enhanced_subscribe_message(connection_id: str, message: dict):
    """Handle enhanced subscription requests."""
    subscription_type = message.get("subscription_type")
    target_id = message.get("target_id")
    
    if not subscription_type or not target_id:
        await enhanced_manager.send_personal_message({
            "type": "error",
            "message": "Missing subscription_type or target_id"
        }, connection_id)
        return
    
    try:
        if subscription_type == "line":
            enhanced_manager.subscribe_to_production_line(connection_id, target_id)
            await enhanced_manager.send_personal_message({
                "type": "subscription_confirmed",
                "subscription_type": "line",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "equipment":
            enhanced_manager.subscribe_to_equipment(connection_id, target_id)
            await enhanced_manager.send_personal_message({
                "type": "subscription_confirmed",
                "subscription_type": "equipment",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "job":
            enhanced_manager.subscribe_to_job(connection_id, target_id)
            await enhanced_manager.send_personal_message({
                "type": "subscription_confirmed",
                "subscription_type": "job",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "production":
            # For production subscriptions, target_id can be "all" or line_id
            if target_id == "all":
                enhanced_manager.subscribe_to_production_events(connection_id)
            else:
                enhanced_manager.subscribe_to_production_events(connection_id, target_id)
            
            await enhanced_manager.send_personal_message({
                "type": "subscription_confirmed",
                "subscription_type": "production",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "oee":
            # For OEE subscriptions, target_id can be "all" or line_id
            if target_id == "all":
                enhanced_manager.subscribe_to_oee_updates(connection_id)
            else:
                enhanced_manager.subscribe_to_oee_updates(connection_id, target_id)
            
            await enhanced_manager.send_personal_message({
                "type": "subscription_confirmed",
                "subscription_type": "oee",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "downtime":
            # For downtime subscriptions, target_id can be "all", "line:line_id", or "equipment:equipment_code"
            if target_id == "all":
                enhanced_manager.subscribe_to_downtime_events(connection_id)
            elif target_id.startswith("line:"):
                line_id = target_id[5:]  # Remove "line:" prefix
                enhanced_manager.subscribe_to_downtime_events(connection_id, line_id=line_id)
            elif target_id.startswith("equipment:"):
                equipment_code = target_id[10:]  # Remove "equipment:" prefix
                enhanced_manager.subscribe_to_downtime_events(connection_id, equipment_code=equipment_code)
            else:
                # Assume it's a line_id if no prefix
                enhanced_manager.subscribe_to_downtime_events(connection_id, line_id=target_id)
            
            await enhanced_manager.send_personal_message({
                "type": "subscription_confirmed",
                "subscription_type": "downtime",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "andon":
            # For Andon subscriptions, target_id can be "all" or line_id
            if target_id == "all":
                enhanced_manager.subscribe_to_andon_events(connection_id)
            else:
                enhanced_manager.subscribe_to_andon_events(connection_id, target_id)
            
            await enhanced_manager.send_personal_message({
                "type": "subscription_confirmed",
                "subscription_type": "andon",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "escalation":
            # For escalation subscriptions, target_id can be "all", "escalation:escalation_id", or "priority:priority"
            if target_id == "all":
                enhanced_manager.subscribe_to_escalation_events(connection_id)
            elif target_id.startswith("escalation:"):
                escalation_id = target_id[11:]  # Remove "escalation:" prefix
                enhanced_manager.subscribe_to_escalation_events(connection_id, escalation_id=escalation_id)
            elif target_id.startswith("priority:"):
                priority = target_id[9:]  # Remove "priority:" prefix
                enhanced_manager.subscribe_to_escalation_events(connection_id, priority=priority)
            else:
                # Assume it's a priority if no prefix
                enhanced_manager.subscribe_to_escalation_events(connection_id, priority=target_id)
            
            await enhanced_manager.send_personal_message({
                "type": "subscription_confirmed",
                "subscription_type": "escalation",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "quality":
            # For quality subscriptions, target_id can be "all" or line_id
            if target_id == "all":
                enhanced_manager.subscribe_to_quality_alerts(connection_id)
            else:
                enhanced_manager.subscribe_to_quality_alerts(connection_id, target_id)
            
            await enhanced_manager.send_personal_message({
                "type": "subscription_confirmed",
                "subscription_type": "quality",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "changeover":
            # For changeover subscriptions, target_id can be "all" or line_id
            if target_id == "all":
                enhanced_manager.subscribe_to_changeover_events(connection_id)
            else:
                enhanced_manager.subscribe_to_changeover_events(connection_id, target_id)
            
            await enhanced_manager.send_personal_message({
                "type": "subscription_confirmed",
                "subscription_type": "changeover",
                "target_id": target_id
            }, connection_id)
        
        else:
            await enhanced_manager.send_personal_message({
                "type": "error",
                "message": f"Unknown subscription type: {subscription_type}"
            }, connection_id)
    
    except Exception as e:
        logger.error("Error handling subscription", error=str(e), connection_id=connection_id)
        await enhanced_manager.send_personal_message({
            "type": "error",
            "message": "Error processing subscription"
        }, connection_id)


async def handle_enhanced_unsubscribe_message(connection_id: str, message: dict):
    """Handle enhanced unsubscription requests."""
    subscription_type = message.get("subscription_type")
    target_id = message.get("target_id")
    
    if not subscription_type or not target_id:
        await enhanced_manager.send_personal_message({
            "type": "error",
            "message": "Missing subscription_type or target_id"
        }, connection_id)
        return
    
    try:
        if subscription_type == "line":
            enhanced_manager.unsubscribe_from_production_line(connection_id, target_id)
            await enhanced_manager.send_personal_message({
                "type": "unsubscription_confirmed",
                "subscription_type": "line",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "equipment":
            enhanced_manager.unsubscribe_from_equipment(connection_id, target_id)
            await enhanced_manager.send_personal_message({
                "type": "unsubscription_confirmed",
                "subscription_type": "equipment",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "job":
            enhanced_manager.unsubscribe_from_job(connection_id, target_id)
            await enhanced_manager.send_personal_message({
                "type": "unsubscription_confirmed",
                "subscription_type": "job",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "production":
            # For production unsubscriptions, target_id can be "all" or line_id
            if target_id == "all":
                enhanced_manager.unsubscribe_from_production_events(connection_id)
            else:
                enhanced_manager.unsubscribe_from_production_events(connection_id, target_id)
            
            await enhanced_manager.send_personal_message({
                "type": "unsubscription_confirmed",
                "subscription_type": "production",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "oee":
            # For OEE unsubscriptions, target_id can be "all" or line_id
            if target_id == "all":
                enhanced_manager.unsubscribe_from_oee_updates(connection_id)
            else:
                enhanced_manager.unsubscribe_from_oee_updates(connection_id, target_id)
            
            await enhanced_manager.send_personal_message({
                "type": "unsubscription_confirmed",
                "subscription_type": "oee",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "downtime":
            # For downtime unsubscriptions, target_id can be "all", "line:line_id", or "equipment:equipment_code"
            if target_id == "all":
                enhanced_manager.unsubscribe_from_downtime_events(connection_id)
            elif target_id.startswith("line:"):
                line_id = target_id[5:]  # Remove "line:" prefix
                enhanced_manager.unsubscribe_from_downtime_events(connection_id, line_id=line_id)
            elif target_id.startswith("equipment:"):
                equipment_code = target_id[10:]  # Remove "equipment:" prefix
                enhanced_manager.unsubscribe_from_downtime_events(connection_id, equipment_code=equipment_code)
            else:
                # Assume it's a line_id if no prefix
                enhanced_manager.unsubscribe_from_downtime_events(connection_id, line_id=target_id)
            
            await enhanced_manager.send_personal_message({
                "type": "unsubscription_confirmed",
                "subscription_type": "downtime",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "andon":
            # For Andon unsubscriptions, target_id can be "all" or line_id
            if target_id == "all":
                enhanced_manager.unsubscribe_from_andon_events(connection_id)
            else:
                enhanced_manager.unsubscribe_from_andon_events(connection_id, target_id)
            
            await enhanced_manager.send_personal_message({
                "type": "unsubscription_confirmed",
                "subscription_type": "andon",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "escalation":
            # For escalation unsubscriptions, target_id can be "all", "escalation:escalation_id", or "priority:priority"
            if target_id == "all":
                enhanced_manager.unsubscribe_from_escalation_events(connection_id)
            elif target_id.startswith("escalation:"):
                escalation_id = target_id[11:]  # Remove "escalation:" prefix
                enhanced_manager.unsubscribe_from_escalation_events(connection_id, escalation_id=escalation_id)
            elif target_id.startswith("priority:"):
                priority = target_id[9:]  # Remove "priority:" prefix
                enhanced_manager.unsubscribe_from_escalation_events(connection_id, priority=priority)
            else:
                # Assume it's a priority if no prefix
                enhanced_manager.unsubscribe_from_escalation_events(connection_id, priority=target_id)
            
            await enhanced_manager.send_personal_message({
                "type": "unsubscription_confirmed",
                "subscription_type": "escalation",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "quality":
            # For quality unsubscriptions, target_id can be "all" or line_id
            if target_id == "all":
                enhanced_manager.unsubscribe_from_quality_alerts(connection_id)
            else:
                enhanced_manager.unsubscribe_from_quality_alerts(connection_id, target_id)
            
            await enhanced_manager.send_personal_message({
                "type": "unsubscription_confirmed",
                "subscription_type": "quality",
                "target_id": target_id
            }, connection_id)
        
        elif subscription_type == "changeover":
            # For changeover unsubscriptions, target_id can be "all" or line_id
            if target_id == "all":
                enhanced_manager.unsubscribe_from_changeover_events(connection_id)
            else:
                enhanced_manager.unsubscribe_from_changeover_events(connection_id, target_id)
            
            await enhanced_manager.send_personal_message({
                "type": "unsubscription_confirmed",
                "subscription_type": "changeover",
                "target_id": target_id
            }, connection_id)
        
        else:
            await enhanced_manager.send_personal_message({
                "type": "error",
                "message": f"Unknown subscription type: {subscription_type}"
            }, connection_id)
    
    except Exception as e:
        logger.error("Error handling unsubscription", error=str(e), connection_id=connection_id)
        await enhanced_manager.send_personal_message({
            "type": "error",
            "message": "Error processing unsubscription"
        }, connection_id)


async def handle_ping_message(connection_id: str):
    """Handle ping messages for connection health checks."""
    await enhanced_manager.send_personal_message({
        "type": "pong",
        "timestamp": "2025-01-20T10:00:00Z"
    }, connection_id)


async def handle_get_stats_message(connection_id: str):
    """Handle get stats message."""
    stats = enhanced_manager.get_connection_stats()
    await enhanced_manager.send_personal_message({
        "type": "connection_stats",
        "data": stats,
        "timestamp": "2025-01-20T10:00:00Z"
    }, connection_id)


async def handle_get_subscriptions_message(connection_id: str):
    """Handle get subscriptions message."""
    subscriptions = enhanced_manager.get_subscription_details(connection_id)
    await enhanced_manager.send_personal_message({
        "type": "subscription_details",
        "data": subscriptions,
        "timestamp": "2025-01-20T10:00:00Z"
    }, connection_id)


# Enhanced broadcasting functions for production events
async def broadcast_production_update(line_id: str, data: dict):
    """Broadcast production update to all subscribers."""
    await enhanced_manager.broadcast_production_update(line_id, data)


async def broadcast_job_assigned(job_data: dict):
    """Broadcast job assignment event."""
    await enhanced_manager.broadcast_job_assigned(job_data)


async def broadcast_job_started(job_data: dict):
    """Broadcast job started event."""
    await enhanced_manager.broadcast_job_started(job_data)


async def broadcast_job_completed(job_data: dict):
    """Broadcast job completed event."""
    await enhanced_manager.broadcast_job_completed(job_data)


async def broadcast_job_cancelled(job_data: dict):
    """Broadcast job cancelled event."""
    await enhanced_manager.broadcast_job_cancelled(job_data)


async def broadcast_oee_update(line_id: str, oee_data: dict):
    """Broadcast OEE update to line subscribers."""
    await enhanced_manager.broadcast_oee_update(line_id, oee_data)


async def broadcast_downtime_event(downtime_event: dict):
    """Broadcast downtime event to relevant subscribers."""
    await enhanced_manager.broadcast_downtime_event(downtime_event)


async def broadcast_andon_event(andon_event: dict):
    """Broadcast Andon event to relevant subscribers."""
    await enhanced_manager.broadcast_andon_event(andon_event)


async def broadcast_escalation_update(escalation_data: dict):
    """Broadcast escalation update to relevant subscribers."""
    await enhanced_manager.broadcast_escalation_update(escalation_data)


async def broadcast_quality_alert(quality_data: dict):
    """Broadcast quality alert to relevant subscribers."""
    await enhanced_manager.broadcast_quality_alert(quality_data)


async def broadcast_changeover_started(changeover_data: dict):
    """Broadcast changeover started event."""
    await enhanced_manager.broadcast_changeover_started(changeover_data)


async def broadcast_changeover_completed(changeover_data: dict):
    """Broadcast changeover completed event."""
    await enhanced_manager.broadcast_changeover_completed(changeover_data)


# Health check endpoint
@router.get("/health")
async def enhanced_websocket_health():
    """Enhanced WebSocket health check endpoint."""
    stats = enhanced_manager.get_connection_stats()
    return {
        "status": "healthy",
        "service": "enhanced_websocket",
        "stats": stats
    }


# Get available event types
@router.get("/events")
async def get_available_events():
    """Get available production event types."""
    return {
        "production_events": enhanced_manager.PRODUCTION_EVENTS,
        "subscription_types": [
            "line", "equipment", "job", "production", "oee", 
            "downtime", "andon", "escalation", "quality", "changeover"
        ]
    }
