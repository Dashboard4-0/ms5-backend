"""
MS5.0 Floor Dashboard - Enhanced Production WebSocket API Routes

This module provides enhanced WebSocket endpoints for production management
with PLC integration, including real-time production updates, event subscriptions,
and comprehensive production event broadcasting.
"""

from typing import Dict, Any, List, Optional
from uuid import UUID
from datetime import datetime
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query, HTTPException, status
from fastapi.responses import JSONResponse
import structlog

from app.auth.permissions import get_current_user_websocket, UserContext
from app.services.enhanced_websocket_manager import EnhancedWebSocketManager
from app.services.real_time_integration_service import RealTimeIntegrationService
from app.utils.exceptions import ValidationError, BusinessLogicError

logger = structlog.get_logger()

router = APIRouter()

# Initialize WebSocket manager and real-time service
websocket_manager = EnhancedWebSocketManager()
real_time_service = RealTimeIntegrationService()


@router.websocket("/ws/production")
async def production_websocket(
    websocket: WebSocket,
    line_id: Optional[str] = Query(None, description="Production line ID for filtering"),
    user_id: Optional[str] = Query(None, description="User ID for authentication"),
    subscription_types: Optional[str] = Query(None, description="Comma-separated subscription types")
):
    """Enhanced WebSocket endpoint for production-specific real-time updates with PLC integration."""
    try:
        # Accept WebSocket connection
        await websocket.accept()
        
        # Authenticate user if user_id provided
        current_user = None
        if user_id:
            try:
                current_user = await get_current_user_websocket(user_id)
                logger.info("WebSocket connection authenticated", user_id=user_id)
            except Exception as e:
                logger.warning("WebSocket authentication failed", user_id=user_id, error=str(e))
                await websocket.close(code=4001, reason="Authentication failed")
                return
        
        # Register connection
        connection_id = await websocket_manager.register_connection(
            websocket=websocket,
            user_id=user_id,
            connection_type="production"
        )
        
        # Parse subscription types
        subscriptions = []
        if subscription_types:
            subscriptions = [sub.strip() for sub in subscription_types.split(",")]
        else:
            # Default subscriptions
            subscriptions = ["production_update", "oee_update", "downtime_event", "andon_event"]
        
        # Subscribe to production events
        for subscription_type in subscriptions:
            await websocket_manager.subscribe_to_events(
                connection_id=connection_id,
                event_type=subscription_type,
                line_id=line_id,
                user_id=user_id
            )
        
        logger.info(
            "Production WebSocket connection established",
            connection_id=connection_id,
            user_id=user_id,
            line_id=line_id,
            subscriptions=subscriptions
        )
        
        # Send connection confirmation
        await websocket_manager.send_message_to_connection(
            connection_id=connection_id,
            message={
                "type": "connection_established",
                "connection_id": connection_id,
                "subscriptions": subscriptions,
                "line_id": line_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        # Handle WebSocket messages
        try:
            while True:
                # Receive message from client
                data = await websocket.receive_text()
                
                try:
                    message = json.loads(data)
                    await _handle_websocket_message(connection_id, message, line_id, user_id)
                except json.JSONDecodeError:
                    await websocket_manager.send_message_to_connection(
                        connection_id=connection_id,
                        message={
                            "type": "error",
                            "error": "Invalid JSON format",
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )
                except Exception as e:
                    logger.error("Error handling WebSocket message", error=str(e))
                    await websocket_manager.send_message_to_connection(
                        connection_id=connection_id,
                        message={
                            "type": "error",
                            "error": "Message processing failed",
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )
        
        except WebSocketDisconnect:
            logger.info("Production WebSocket disconnected", connection_id=connection_id)
        except Exception as e:
            logger.error("Production WebSocket error", connection_id=connection_id, error=str(e))
        finally:
            # Clean up connection
            await websocket_manager.unregister_connection(connection_id)
            logger.info("Production WebSocket connection cleaned up", connection_id=connection_id)
    
    except Exception as e:
        logger.error("Failed to establish production WebSocket connection", error=str(e))
        try:
            await websocket.close(code=4000, reason="Connection failed")
        except:
            pass


@router.websocket("/ws/production/{line_id}")
async def line_production_websocket(
    websocket: WebSocket,
    line_id: str,
    user_id: Optional[str] = Query(None, description="User ID for authentication"),
    subscription_types: Optional[str] = Query(None, description="Comma-separated subscription types")
):
    """Line-specific production WebSocket endpoint with PLC integration."""
    try:
        # Accept WebSocket connection
        await websocket.accept()
        
        # Authenticate user if user_id provided
        current_user = None
        if user_id:
            try:
                current_user = await get_current_user_websocket(user_id)
                logger.info("Line WebSocket connection authenticated", user_id=user_id, line_id=line_id)
            except Exception as e:
                logger.warning("Line WebSocket authentication failed", user_id=user_id, line_id=line_id, error=str(e))
                await websocket.close(code=4001, reason="Authentication failed")
                return
        
        # Register connection
        connection_id = await websocket_manager.register_connection(
            websocket=websocket,
            user_id=user_id,
            connection_type="line_production",
            line_id=line_id
        )
        
        # Parse subscription types
        subscriptions = []
        if subscription_types:
            subscriptions = [sub.strip() for sub in subscription_types.split(",")]
        else:
            # Default subscriptions for line
            subscriptions = [
                "production_update", "oee_update", "downtime_event", "andon_event",
                "job_assigned", "job_started", "job_completed", "escalation_update",
                "quality_alert", "changeover_started", "changeover_completed"
            ]
        
        # Subscribe to line-specific events
        for subscription_type in subscriptions:
            await websocket_manager.subscribe_to_events(
                connection_id=connection_id,
                event_type=subscription_type,
                line_id=line_id,
                user_id=user_id
            )
        
        logger.info(
            "Line production WebSocket connection established",
            connection_id=connection_id,
            user_id=user_id,
            line_id=line_id,
            subscriptions=subscriptions
        )
        
        # Send connection confirmation
        await websocket_manager.send_message_to_connection(
            connection_id=connection_id,
            message={
                "type": "line_connection_established",
                "connection_id": connection_id,
                "line_id": line_id,
                "subscriptions": subscriptions,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        # Send initial line status
        try:
            initial_status = await _get_line_initial_status(line_id)
            await websocket_manager.send_message_to_connection(
                connection_id=connection_id,
                message={
                    "type": "initial_line_status",
                    "line_id": line_id,
                    "data": initial_status,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
        except Exception as e:
            logger.warning("Failed to send initial line status", line_id=line_id, error=str(e))
        
        # Handle WebSocket messages
        try:
            while True:
                # Receive message from client
                data = await websocket.receive_text()
                
                try:
                    message = json.loads(data)
                    await _handle_websocket_message(connection_id, message, line_id, user_id)
                except json.JSONDecodeError:
                    await websocket_manager.send_message_to_connection(
                        connection_id=connection_id,
                        message={
                            "type": "error",
                            "error": "Invalid JSON format",
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )
                except Exception as e:
                    logger.error("Error handling line WebSocket message", error=str(e))
                    await websocket_manager.send_message_to_connection(
                        connection_id=connection_id,
                        message={
                            "type": "error",
                            "error": "Message processing failed",
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )
        
        except WebSocketDisconnect:
            logger.info("Line production WebSocket disconnected", connection_id=connection_id, line_id=line_id)
        except Exception as e:
            logger.error("Line production WebSocket error", connection_id=connection_id, line_id=line_id, error=str(e))
        finally:
            # Clean up connection
            await websocket_manager.unregister_connection(connection_id)
            logger.info("Line production WebSocket connection cleaned up", connection_id=connection_id, line_id=line_id)
    
    except Exception as e:
        logger.error("Failed to establish line production WebSocket connection", line_id=line_id, error=str(e))
        try:
            await websocket.close(code=4000, reason="Connection failed")
        except:
            pass


@router.websocket("/ws/equipment/{equipment_code}")
async def equipment_production_websocket(
    websocket: WebSocket,
    equipment_code: str,
    user_id: Optional[str] = Query(None, description="User ID for authentication"),
    subscription_types: Optional[str] = Query(None, description="Comma-separated subscription types")
):
    """Equipment-specific production WebSocket endpoint with PLC integration."""
    try:
        # Accept WebSocket connection
        await websocket.accept()
        
        # Authenticate user if user_id provided
        current_user = None
        if user_id:
            try:
                current_user = await get_current_user_websocket(user_id)
                logger.info("Equipment WebSocket connection authenticated", user_id=user_id, equipment_code=equipment_code)
            except Exception as e:
                logger.warning("Equipment WebSocket authentication failed", user_id=user_id, equipment_code=equipment_code, error=str(e))
                await websocket.close(code=4001, reason="Authentication failed")
                return
        
        # Register connection
        connection_id = await websocket_manager.register_connection(
            websocket=websocket,
            user_id=user_id,
            connection_type="equipment_production",
            equipment_code=equipment_code
        )
        
        # Parse subscription types
        subscriptions = []
        if subscription_types:
            subscriptions = [sub.strip() for sub in subscription_types.split(",")]
        else:
            # Default subscriptions for equipment
            subscriptions = [
                "production_update", "oee_update", "downtime_event", "andon_event",
                "job_assigned", "job_started", "job_completed", "plc_metrics_update"
            ]
        
        # Subscribe to equipment-specific events
        for subscription_type in subscriptions:
            await websocket_manager.subscribe_to_events(
                connection_id=connection_id,
                event_type=subscription_type,
                equipment_code=equipment_code,
                user_id=user_id
            )
        
        logger.info(
            "Equipment production WebSocket connection established",
            connection_id=connection_id,
            user_id=user_id,
            equipment_code=equipment_code,
            subscriptions=subscriptions
        )
        
        # Send connection confirmation
        await websocket_manager.send_message_to_connection(
            connection_id=connection_id,
            message={
                "type": "equipment_connection_established",
                "connection_id": connection_id,
                "equipment_code": equipment_code,
                "subscriptions": subscriptions,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        # Send initial equipment status
        try:
            initial_status = await _get_equipment_initial_status(equipment_code)
            await websocket_manager.send_message_to_connection(
                connection_id=connection_id,
                message={
                    "type": "initial_equipment_status",
                    "equipment_code": equipment_code,
                    "data": initial_status,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
        except Exception as e:
            logger.warning("Failed to send initial equipment status", equipment_code=equipment_code, error=str(e))
        
        # Handle WebSocket messages
        try:
            while True:
                # Receive message from client
                data = await websocket.receive_text()
                
                try:
                    message = json.loads(data)
                    await _handle_websocket_message(connection_id, message, None, user_id, equipment_code)
                except json.JSONDecodeError:
                    await websocket_manager.send_message_to_connection(
                        connection_id=connection_id,
                        message={
                            "type": "error",
                            "error": "Invalid JSON format",
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )
                except Exception as e:
                    logger.error("Error handling equipment WebSocket message", error=str(e))
                    await websocket_manager.send_message_to_connection(
                        connection_id=connection_id,
                        message={
                            "type": "error",
                            "error": "Message processing failed",
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )
        
        except WebSocketDisconnect:
            logger.info("Equipment production WebSocket disconnected", connection_id=connection_id, equipment_code=equipment_code)
        except Exception as e:
            logger.error("Equipment production WebSocket error", connection_id=connection_id, equipment_code=equipment_code, error=str(e))
        finally:
            # Clean up connection
            await websocket_manager.unregister_connection(connection_id)
            logger.info("Equipment production WebSocket connection cleaned up", connection_id=connection_id, equipment_code=equipment_code)
    
    except Exception as e:
        logger.error("Failed to establish equipment production WebSocket connection", equipment_code=equipment_code, error=str(e))
        try:
            await websocket.close(code=4000, reason="Connection failed")
        except:
            pass


@router.get("/ws/production/events/types", status_code=status.HTTP_200_OK)
async def get_production_event_types() -> Dict[str, Any]:
    """Get available production event types for WebSocket subscriptions."""
    try:
        event_types = {
            "production_events": {
                "production_update": "Production metrics updated",
                "job_assigned": "Job assigned to operator",
                "job_started": "Job execution started",
                "job_completed": "Job completed",
                "job_cancelled": "Job cancelled",
                "changeover_started": "Changeover process started",
                "changeover_completed": "Changeover process completed"
            },
            "oee_events": {
                "oee_update": "OEE calculation updated",
                "oee_alert": "OEE threshold exceeded"
            },
            "downtime_events": {
                "downtime_event": "Downtime event detected",
                "downtime_resolved": "Downtime event resolved"
            },
            "andon_events": {
                "andon_event": "Andon event created",
                "escalation_update": "Andon escalation updated",
                "andon_resolved": "Andon event resolved"
            },
            "quality_events": {
                "quality_alert": "Quality threshold exceeded",
                "quality_issue": "Quality issue detected"
            },
            "plc_events": {
                "plc_metrics_update": "PLC metrics updated",
                "plc_fault": "PLC fault detected",
                "plc_connection_status": "PLC connection status changed"
            },
            "system_events": {
                "connection_established": "WebSocket connection established",
                "subscription_updated": "Event subscription updated",
                "ping": "Ping message",
                "error": "Error message"
            }
        }
        
        return {
            "event_types": event_types,
            "total_event_types": sum(len(events) for events in event_types.values()),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error("Failed to get production event types", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/ws/production/subscriptions", status_code=status.HTTP_200_OK)
async def get_production_subscriptions(
    connection_id: Optional[str] = Query(None, description="Specific connection ID"),
    user_id: Optional[str] = Query(None, description="User ID filter"),
    line_id: Optional[str] = Query(None, description="Line ID filter")
) -> Dict[str, Any]:
    """Get current WebSocket subscriptions for production events."""
    try:
        subscriptions = await websocket_manager.get_subscriptions(
            connection_id=connection_id,
            user_id=user_id,
            line_id=line_id
        )
        
        return {
            "subscriptions": subscriptions,
            "total_connections": len(subscriptions),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error("Failed to get production subscriptions", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/ws/production/stats", status_code=status.HTTP_200_OK)
async def get_production_websocket_stats() -> Dict[str, Any]:
    """Get WebSocket statistics for production events."""
    try:
        stats = await websocket_manager.get_websocket_stats()
        
        return {
            "websocket_stats": stats,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error("Failed to get production WebSocket stats", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


# Helper functions
async def _handle_websocket_message(
    connection_id: str,
    message: Dict[str, Any],
    line_id: Optional[str] = None,
    user_id: Optional[str] = None,
    equipment_code: Optional[str] = None
) -> None:
    """Handle incoming WebSocket messages."""
    message_type = message.get("type")
    
    if message_type == "ping":
        # Respond to ping
        await websocket_manager.send_message_to_connection(
            connection_id=connection_id,
            message={
                "type": "pong",
                "timestamp": datetime.utcnow().isoformat()
            }
        )
    
    elif message_type == "subscribe":
        # Handle subscription request
        event_type = message.get("event_type")
        if event_type:
            await websocket_manager.subscribe_to_events(
                connection_id=connection_id,
                event_type=event_type,
                line_id=line_id,
                equipment_code=equipment_code,
                user_id=user_id
            )
            
            await websocket_manager.send_message_to_connection(
                connection_id=connection_id,
                message={
                    "type": "subscription_confirmed",
                    "event_type": event_type,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
    
    elif message_type == "unsubscribe":
        # Handle unsubscription request
        event_type = message.get("event_type")
        if event_type:
            await websocket_manager.unsubscribe_from_events(
                connection_id=connection_id,
                event_type=event_type
            )
            
            await websocket_manager.send_message_to_connection(
                connection_id=connection_id,
                message={
                    "type": "unsubscription_confirmed",
                    "event_type": event_type,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
    
    elif message_type == "get_status":
        # Handle status request
        status_data = await _get_connection_status(connection_id, line_id, equipment_code)
        await websocket_manager.send_message_to_connection(
            connection_id=connection_id,
            message={
                "type": "status_response",
                "data": status_data,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
    
    else:
        # Unknown message type
        await websocket_manager.send_message_to_connection(
            connection_id=connection_id,
            message={
                "type": "error",
                "error": f"Unknown message type: {message_type}",
                "timestamp": datetime.utcnow().isoformat()
            }
        )


async def _get_line_initial_status(line_id: str) -> Dict[str, Any]:
    """Get initial status for a production line."""
    try:
        # This would get actual line status from services
        # For now, return mock data
        return {
            "line_id": line_id,
            "status": "running",
            "equipment_count": 2,
            "active_jobs": 1,
            "current_oee": 0.85,
            "active_andon_events": 0,
            "downtime_events": 0
        }
    except Exception as e:
        logger.warning("Failed to get line initial status", line_id=line_id, error=str(e))
        return {"error": "Failed to get initial status"}


async def _get_equipment_initial_status(equipment_code: str) -> Dict[str, Any]:
    """Get initial status for equipment."""
    try:
        # This would get actual equipment status from services
        # For now, return mock data
        return {
            "equipment_code": equipment_code,
            "status": "running",
            "current_job": None,
            "oee": 0.85,
            "availability": 0.90,
            "performance": 0.95,
            "quality": 0.95,
            "active_faults": 0,
            "last_update": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.warning("Failed to get equipment initial status", equipment_code=equipment_code, error=str(e))
        return {"error": "Failed to get initial status"}


async def _get_connection_status(
    connection_id: str,
    line_id: Optional[str] = None,
    equipment_code: Optional[str] = None
) -> Dict[str, Any]:
    """Get status for a WebSocket connection."""
    try:
        connection_info = await websocket_manager.get_connection_info(connection_id)
        
        status_data = {
            "connection_id": connection_id,
            "connection_info": connection_info,
            "subscriptions": await websocket_manager.get_connection_subscriptions(connection_id),
            "line_id": line_id,
            "equipment_code": equipment_code,
            "status": "connected"
        }
        
        return status_data
    except Exception as e:
        logger.warning("Failed to get connection status", connection_id=connection_id, error=str(e))
        return {"error": "Failed to get connection status"}
