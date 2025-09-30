"""
MS5.0 Floor Dashboard - WebSocket Manager

This module provides WebSocket management with production-specific
event support, real-time broadcasting, and comprehensive subscription management.
"""

import json
import asyncio
from typing import Dict, List, Set, Optional, Any
from uuid import UUID
from datetime import datetime
import structlog

from app.utils.exceptions import WebSocketError

logger = structlog.get_logger()


class WebSocketManager:
    """WebSocket manager with production management support."""
    
    def __init__(self):
        # Active connections by connection ID
        self.connections: Dict[str, Any] = {}
        # User connections mapping
        self.user_connections: Dict[str, Set[str]] = {}
        # Subscriptions by connection ID
        self.subscriptions: Dict[str, Set[str]] = {}
        
        # Production-specific subscriptions
        self.line_subscriptions: Dict[str, Set[str]] = {}
        self.equipment_subscriptions: Dict[str, Set[str]] = {}
        self.job_subscriptions: Dict[str, Set[str]] = {}
        self.production_subscriptions: Dict[str, Set[str]] = {}
        self.oee_subscriptions: Dict[str, Set[str]] = {}
        self.downtime_subscriptions: Dict[str, Set[str]] = {}
        self.andon_subscriptions: Dict[str, Set[str]] = {}
        self.escalation_subscriptions: Dict[str, Set[str]] = {}
        self.quality_subscriptions: Dict[str, Set[str]] = {}
        self.changeover_subscriptions: Dict[str, Set[str]] = {}
        
        # Production event types
        self.PRODUCTION_EVENTS = {
            "line_status_update": "Production line status updated",
            "production_update": "Production metrics updated",
            "andon_event": "Andon event created",
            "oee_update": "OEE calculation updated",
            "downtime_event": "Downtime event detected",
            "job_assigned": "Job assigned to operator",
            "job_started": "Job execution started", 
            "job_completed": "Job completed",
            "job_cancelled": "Job cancelled",
            "escalation_update": "Andon escalation updated",
            "quality_alert": "Quality threshold exceeded",
            "changeover_started": "Changeover process started",
            "changeover_completed": "Changeover process completed"
        }
    
    async def add_connection(self, websocket: Any, user_id: str) -> str:
        """Add a WebSocket connection and register it."""
        connection_id = f"{user_id}_{len(self.connections)}"
        
        self.connections[connection_id] = websocket
        self.subscriptions[connection_id] = set()
        
        # Add to user connections
        if user_id not in self.user_connections:
            self.user_connections[user_id] = set()
        self.user_connections[user_id].add(connection_id)
        
        logger.info("WebSocket connection added", 
                   connection_id=connection_id, user_id=user_id)
        return connection_id
    
    def remove_connection(self, connection_id: str):
        """Remove a WebSocket connection."""
        if connection_id in self.connections:
            del self.connections[connection_id]
        
        if connection_id in self.subscriptions:
            del self.subscriptions[connection_id]
        
        # Remove from all subscription types
        self._remove_from_all_subscriptions(connection_id)
        
        # Remove from user connections
        for user_id, connections in self.user_connections.items():
            connections.discard(connection_id)
        
        logger.info("WebSocket connection removed", connection_id=connection_id)
    
    def _remove_from_all_subscriptions(self, connection_id: str):
        """Remove connection from all subscription types."""
        subscription_lists = [
            self.line_subscriptions,
            self.equipment_subscriptions,
            self.job_subscriptions,
            self.production_subscriptions,
            self.oee_subscriptions,
            self.downtime_subscriptions,
            self.andon_subscriptions,
            self.escalation_subscriptions,
            self.quality_subscriptions,
            self.changeover_subscriptions
        ]
        
        for subscription_dict in subscription_lists:
            for key, connections in subscription_dict.items():
                connections.discard(connection_id)
    
    # Production-specific subscription methods
    def subscribe_to_line(self, connection_id: str, line_id: str):
        """Subscribe a connection to a production line."""
        if connection_id not in self.subscriptions:
            return
        
        self.subscriptions[connection_id].add(f"line:{line_id}")
        
        if line_id not in self.line_subscriptions:
            self.line_subscriptions[line_id] = set()
        self.line_subscriptions[line_id].add(connection_id)
        
        logger.debug("Subscribed to production line", 
                   connection_id=connection_id, line_id=line_id)
    
    def subscribe_to_equipment(self, connection_id: str, equipment_code: str):
        """Subscribe a connection to equipment updates."""
        if connection_id not in self.subscriptions:
            return
        
        self.subscriptions[connection_id].add(f"equipment:{equipment_code}")
        
        if equipment_code not in self.equipment_subscriptions:
            self.equipment_subscriptions[equipment_code] = set()
        self.equipment_subscriptions[equipment_code].add(connection_id)
        
        logger.debug("Subscribed to equipment", 
                   connection_id=connection_id, equipment_code=equipment_code)
    
    def subscribe_to_job(self, connection_id: str, job_id: str):
        """Subscribe a connection to job updates."""
        if connection_id not in self.subscriptions:
            return
        
        self.subscriptions[connection_id].add(f"job:{job_id}")
        
        if job_id not in self.job_subscriptions:
            self.job_subscriptions[job_id] = set()
        self.job_subscriptions[job_id].add(connection_id)
        
        logger.debug("Subscribed to job", 
                   connection_id=connection_id, job_id=job_id)
    
    def subscribe_to_production_events(self, connection_id: str, line_id: str = None):
        """Subscribe a connection to production events."""
        if connection_id not in self.subscriptions:
            return
        
        subscription_key = f"production:{line_id or 'all'}"
        self.subscriptions[connection_id].add(subscription_key)
        
        if line_id:
            if line_id not in self.production_subscriptions:
                self.production_subscriptions[line_id] = set()
            self.production_subscriptions[line_id].add(connection_id)
        
        logger.debug("Subscribed to production events", 
                   connection_id=connection_id, line_id=line_id)
    
    def subscribe_to_oee_updates(self, connection_id: str, line_id: str = None):
        """Subscribe a connection to OEE updates."""
        if connection_id not in self.subscriptions:
            return
        
        subscription_key = f"oee:{line_id or 'all'}"
        self.subscriptions[connection_id].add(subscription_key)
        
        if line_id:
            if line_id not in self.oee_subscriptions:
                self.oee_subscriptions[line_id] = set()
            self.oee_subscriptions[line_id].add(connection_id)
        
        logger.debug("Subscribed to OEE updates", 
                   connection_id=connection_id, line_id=line_id)
    
    def subscribe_to_downtime_events(self, connection_id: str, line_id: str = None, equipment_code: str = None):
        """Subscribe a connection to downtime events."""
        if connection_id not in self.subscriptions:
            return
        
        subscription_key = f"downtime:{line_id or 'all'}:{equipment_code or 'all'}"
        self.subscriptions[connection_id].add(subscription_key)
        
        if line_id:
            if line_id not in self.downtime_subscriptions:
                self.downtime_subscriptions[line_id] = set()
            self.downtime_subscriptions[line_id].add(connection_id)
        
        if equipment_code:
            if equipment_code not in self.downtime_subscriptions:
                self.downtime_subscriptions[equipment_code] = set()
            self.downtime_subscriptions[equipment_code].add(connection_id)
        
        logger.debug("Subscribed to downtime events", 
                   connection_id=connection_id, line_id=line_id, equipment_code=equipment_code)
    
    def subscribe_to_andon_events(self, connection_id: str, line_id: str = None):
        """Subscribe a connection to Andon events."""
        if connection_id not in self.subscriptions:
            return
        
        subscription_key = f"andon:{line_id or 'all'}"
        self.subscriptions[connection_id].add(subscription_key)
        
        if line_id:
            if line_id not in self.andon_subscriptions:
                self.andon_subscriptions[line_id] = set()
            self.andon_subscriptions[line_id].add(connection_id)
        
        logger.debug("Subscribed to Andon events", 
                   connection_id=connection_id, line_id=line_id)
    
    def subscribe_to_escalation_events(self, connection_id: str, escalation_id: str = None, priority: str = None):
        """Subscribe a connection to escalation events."""
        if connection_id not in self.subscriptions:
            return
        
        if escalation_id:
            subscription_key = f"escalation:{escalation_id}"
            self.subscriptions[connection_id].add(subscription_key)
            
            if escalation_id not in self.escalation_subscriptions:
                self.escalation_subscriptions[escalation_id] = set()
            self.escalation_subscriptions[escalation_id].add(connection_id)
        
        if priority:
            subscription_key = f"escalation_priority:{priority}"
            self.subscriptions[connection_id].add(subscription_key)
            
            if priority not in self.escalation_subscriptions:
                self.escalation_subscriptions[priority] = set()
            self.escalation_subscriptions[priority].add(connection_id)
        
        logger.debug("Subscribed to escalation events", 
                   connection_id=connection_id, escalation_id=escalation_id, priority=priority)
    
    def subscribe_to_quality_alerts(self, connection_id: str, line_id: str = None):
        """Subscribe a connection to quality alerts."""
        if connection_id not in self.subscriptions:
            return
        
        subscription_key = f"quality:{line_id or 'all'}"
        self.subscriptions[connection_id].add(subscription_key)
        
        if line_id:
            if line_id not in self.quality_subscriptions:
                self.quality_subscriptions[line_id] = set()
            self.quality_subscriptions[line_id].add(connection_id)
        
        logger.debug("Subscribed to quality alerts", 
                   connection_id=connection_id, line_id=line_id)
    
    def subscribe_to_changeover_events(self, connection_id: str, line_id: str = None):
        """Subscribe a connection to changeover events."""
        if connection_id not in self.subscriptions:
            return
        
        subscription_key = f"changeover:{line_id or 'all'}"
        self.subscriptions[connection_id].add(subscription_key)
        
        if line_id:
            if line_id not in self.changeover_subscriptions:
                self.changeover_subscriptions[line_id] = set()
            self.changeover_subscriptions[line_id].add(connection_id)
        
        logger.debug("Subscribed to changeover events", 
                   connection_id=connection_id, line_id=line_id)
    
    # Unsubscription methods
    def unsubscribe_from_line(self, connection_id: str, line_id: str):
        """Unsubscribe a connection from a production line."""
        if connection_id not in self.subscriptions:
            return
        
        self.subscriptions[connection_id].discard(f"line:{line_id}")
        
        if line_id in self.line_subscriptions:
            self.line_subscriptions[line_id].discard(connection_id)
        
        logger.debug("Unsubscribed from production line", 
                   connection_id=connection_id, line_id=line_id)
    
    def unsubscribe_from_equipment(self, connection_id: str, equipment_code: str):
        """Unsubscribe a connection from equipment updates."""
        if connection_id not in self.subscriptions:
            return
        
        self.subscriptions[connection_id].discard(f"equipment:{equipment_code}")
        
        if equipment_code in self.equipment_subscriptions:
            self.equipment_subscriptions[equipment_code].discard(connection_id)
        
        logger.debug("Unsubscribed from equipment", 
                   connection_id=connection_id, equipment_code=equipment_code)
    
    def unsubscribe_from_job(self, connection_id: str, job_id: str):
        """Unsubscribe a connection from job updates."""
        if connection_id not in self.subscriptions:
            return
        
        self.subscriptions[connection_id].discard(f"job:{job_id}")
        
        if job_id in self.job_subscriptions:
            self.job_subscriptions[job_id].discard(connection_id)
        
        logger.debug("Unsubscribed from job", 
                   connection_id=connection_id, job_id=job_id)
    
    def unsubscribe_from_production_events(self, connection_id: str, line_id: str = None):
        """Unsubscribe a connection from production events."""
        if connection_id not in self.subscriptions:
            return
        
        subscription_key = f"production:{line_id or 'all'}"
        self.subscriptions[connection_id].discard(subscription_key)
        
        if line_id and line_id in self.production_subscriptions:
            self.production_subscriptions[line_id].discard(connection_id)
        
        logger.debug("Unsubscribed from production events", 
                   connection_id=connection_id, line_id=line_id)
    
    def unsubscribe_from_oee_updates(self, connection_id: str, line_id: str = None):
        """Unsubscribe a connection from OEE updates."""
        if connection_id not in self.subscriptions:
            return
        
        subscription_key = f"oee:{line_id or 'all'}"
        self.subscriptions[connection_id].discard(subscription_key)
        
        if line_id and line_id in self.oee_subscriptions:
            self.oee_subscriptions[line_id].discard(connection_id)
        
        logger.debug("Unsubscribed from OEE updates", 
                   connection_id=connection_id, line_id=line_id)
    
    def unsubscribe_from_downtime_events(self, connection_id: str, line_id: str = None, equipment_code: str = None):
        """Unsubscribe a connection from downtime events."""
        if connection_id not in self.subscriptions:
            return
        
        subscription_key = f"downtime:{line_id or 'all'}:{equipment_code or 'all'}"
        self.subscriptions[connection_id].discard(subscription_key)
        
        if line_id and line_id in self.downtime_subscriptions:
            self.downtime_subscriptions[line_id].discard(connection_id)
        
        if equipment_code and equipment_code in self.downtime_subscriptions:
            self.downtime_subscriptions[equipment_code].discard(connection_id)
        
        logger.debug("Unsubscribed from downtime events", 
                   connection_id=connection_id, line_id=line_id, equipment_code=equipment_code)
    
    def unsubscribe_from_andon_events(self, connection_id: str, line_id: str = None):
        """Unsubscribe a connection from Andon events."""
        if connection_id not in self.subscriptions:
            return
        
        subscription_key = f"andon:{line_id or 'all'}"
        self.subscriptions[connection_id].discard(subscription_key)
        
        if line_id and line_id in self.andon_subscriptions:
            self.andon_subscriptions[line_id].discard(connection_id)
        
        logger.debug("Unsubscribed from Andon events", 
                   connection_id=connection_id, line_id=line_id)
    
    def unsubscribe_from_escalation_events(self, connection_id: str, escalation_id: str = None, priority: str = None):
        """Unsubscribe a connection from escalation events."""
        if connection_id not in self.subscriptions:
            return
        
        if escalation_id:
            subscription_key = f"escalation:{escalation_id}"
            self.subscriptions[connection_id].discard(subscription_key)
            
            if escalation_id in self.escalation_subscriptions:
                self.escalation_subscriptions[escalation_id].discard(connection_id)
        
        if priority:
            subscription_key = f"escalation_priority:{priority}"
            self.subscriptions[connection_id].discard(subscription_key)
            
            if priority in self.escalation_subscriptions:
                self.escalation_subscriptions[priority].discard(connection_id)
        
        logger.debug("Unsubscribed from escalation events", 
                   connection_id=connection_id, escalation_id=escalation_id, priority=priority)
    
    def unsubscribe_from_quality_alerts(self, connection_id: str, line_id: str = None):
        """Unsubscribe a connection from quality alerts."""
        if connection_id not in self.subscriptions:
            return
        
        subscription_key = f"quality:{line_id or 'all'}"
        self.subscriptions[connection_id].discard(subscription_key)
        
        if line_id and line_id in self.quality_subscriptions:
            self.quality_subscriptions[line_id].discard(connection_id)
        
        logger.debug("Unsubscribed from quality alerts", 
                   connection_id=connection_id, line_id=line_id)
    
    def unsubscribe_from_changeover_events(self, connection_id: str, line_id: str = None):
        """Unsubscribe a connection from changeover events."""
        if connection_id not in self.subscriptions:
            return
        
        subscription_key = f"changeover:{line_id or 'all'}"
        self.subscriptions[connection_id].discard(subscription_key)
        
        if line_id and line_id in self.changeover_subscriptions:
            self.changeover_subscriptions[line_id].discard(connection_id)
        
        logger.debug("Unsubscribed from changeover events", 
                   connection_id=connection_id, line_id=line_id)
    
    # Message sending methods
    async def send_personal_message(self, message: dict, connection_id: str):
        """Send a message to a specific connection."""
        if connection_id in self.connections:
            websocket = self.connections[connection_id]
            try:
                await websocket.send_text(json.dumps(message))
            except Exception as e:
                logger.error("Failed to send personal message", 
                           error=str(e), connection_id=connection_id)
                self.remove_connection(connection_id)
    
    async def send_to_user(self, message: dict, user_id: str):
        """Send a message to all connections for a specific user."""
        if user_id in self.user_connections:
            for connection_id in self.user_connections[user_id].copy():
                await self.send_personal_message(message, connection_id)
    
    async def send_to_line(self, message: dict, line_id: str):
        """Send a message to all connections subscribed to a line."""
        if line_id in self.line_subscriptions:
            for connection_id in self.line_subscriptions[line_id].copy():
                await self.send_personal_message(message, connection_id)
    
    async def send_to_equipment(self, message: dict, equipment_code: str):
        """Send a message to all connections subscribed to equipment."""
        if equipment_code in self.equipment_subscriptions:
            for connection_id in self.equipment_subscriptions[equipment_code].copy():
                await self.send_personal_message(message, connection_id)
    
    async def send_to_job(self, message: dict, job_id: str):
        """Send a message to all connections subscribed to a job."""
        if job_id in self.job_subscriptions:
            for connection_id in self.job_subscriptions[job_id].copy():
                await self.send_personal_message(message, connection_id)
    
    async def send_to_production_subscribers(self, message: dict, line_id: str = None):
        """Send a message to all connections subscribed to production events."""
        if line_id and line_id in self.production_subscriptions:
            for connection_id in self.production_subscriptions[line_id].copy():
                await self.send_personal_message(message, connection_id)
        
        # Send to general production subscribers
        for connection_id, subscriptions in self.subscriptions.items():
            if any(sub.startswith("production:all") for sub in subscriptions):
                await self.send_personal_message(message, connection_id)
    
    async def send_to_oee_subscribers(self, message: dict, line_id: str = None):
        """Send a message to all connections subscribed to OEE updates."""
        if line_id and line_id in self.oee_subscriptions:
            for connection_id in self.oee_subscriptions[line_id].copy():
                await self.send_personal_message(message, connection_id)
        
        # Send to general OEE subscribers
        for connection_id, subscriptions in self.subscriptions.items():
            if any(sub.startswith("oee:all") for sub in subscriptions):
                await self.send_personal_message(message, connection_id)
    
    async def send_to_downtime_subscribers(self, message: dict, line_id: str = None, equipment_code: str = None):
        """Send a message to all connections subscribed to downtime events."""
        # Send to line-specific downtime subscribers
        if line_id and line_id in self.downtime_subscriptions:
            for connection_id in self.downtime_subscriptions[line_id].copy():
                await self.send_personal_message(message, connection_id)
        
        # Send to equipment-specific downtime subscribers
        if equipment_code and equipment_code in self.downtime_subscriptions:
            for connection_id in self.downtime_subscriptions[equipment_code].copy():
                await self.send_personal_message(message, connection_id)
        
        # Send to general downtime subscribers
        for connection_id, subscriptions in self.subscriptions.items():
            if any(sub.startswith("downtime:all:all") for sub in subscriptions):
                await self.send_personal_message(message, connection_id)
    
    async def send_to_andon_subscribers(self, message: dict, line_id: str = None):
        """Send a message to all connections subscribed to Andon events."""
        if line_id and line_id in self.andon_subscriptions:
            for connection_id in self.andon_subscriptions[line_id].copy():
                await self.send_personal_message(message, connection_id)
        
        # Send to general Andon subscribers
        for connection_id, subscriptions in self.subscriptions.items():
            if any(sub.startswith("andon:all") for sub in subscriptions):
                await self.send_personal_message(message, connection_id)
    
    async def send_to_escalation_subscribers(self, message: dict, escalation_id: str = None, priority: str = None):
        """Send a message to all connections subscribed to escalation events."""
        # Send to specific escalation subscribers
        if escalation_id and escalation_id in self.escalation_subscriptions:
            for connection_id in self.escalation_subscriptions[escalation_id].copy():
                await self.send_personal_message(message, connection_id)
        
        # Send to priority-based escalation subscribers
        if priority and priority in self.escalation_subscriptions:
            for connection_id in self.escalation_subscriptions[priority].copy():
                await self.send_personal_message(message, connection_id)
        
        # Send to general escalation subscribers
        for connection_id, subscriptions in self.subscriptions.items():
            if any(sub.startswith("escalation:") for sub in subscriptions):
                await self.send_personal_message(message, connection_id)
    
    async def send_to_quality_subscribers(self, message: dict, line_id: str = None):
        """Send a message to all connections subscribed to quality alerts."""
        if line_id and line_id in self.quality_subscriptions:
            for connection_id in self.quality_subscriptions[line_id].copy():
                await self.send_personal_message(message, connection_id)
        
        # Send to general quality subscribers
        for connection_id, subscriptions in self.subscriptions.items():
            if any(sub.startswith("quality:all") for sub in subscriptions):
                await self.send_personal_message(message, connection_id)
    
    async def send_to_changeover_subscribers(self, message: dict, line_id: str = None):
        """Send a message to all connections subscribed to changeover events."""
        if line_id and line_id in self.changeover_subscriptions:
            for connection_id in self.changeover_subscriptions[line_id].copy():
                await self.send_personal_message(message, connection_id)
        
        # Send to general changeover subscribers
        for connection_id, subscriptions in self.subscriptions.items():
            if any(sub.startswith("changeover:all") for sub in subscriptions):
                await self.send_personal_message(message, connection_id)
    
    async def broadcast(self, message: dict):
        """Broadcast a message to all active connections."""
        for connection_id in list(self.connections.keys()):
            await self.send_personal_message(message, connection_id)
    
    # Production-specific broadcasting methods as specified in Phase 7
    async def broadcast_line_status_update(self, line_id: str, data: dict):
        """Broadcast line status update."""
        message = {
            "type": "line_status_update",
            "line_id": line_id,
            "data": data,
            "timestamp": datetime.utcnow().isoformat()
        }
        await self.send_to_line(message, line_id)
    
    async def broadcast_production_update(self, line_id: str, data: dict):
        """Broadcast production update."""
        message = {
            "type": "production_update",
            "line_id": line_id,
            "data": data,
            "timestamp": datetime.utcnow().isoformat()
        }
        await self.send_to_production_subscribers(message, line_id)
        await self.send_to_line(message, line_id)
    
    async def broadcast_andon_event(self, event: dict):
        """Broadcast Andon event."""
        message = {
            "type": "andon_event",
            "data": event,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Send to Andon-specific subscribers
        line_id = event.get("line_id")
        await self.send_to_andon_subscribers(message, line_id)
        
        # Also send to line and equipment subscribers
        if line_id:
            await self.send_to_line(message, line_id)
        
        equipment_code = event.get("equipment_code")
        if equipment_code:
            await self.send_to_equipment(message, equipment_code)
    
    async def broadcast_oee_update(self, line_id: str, oee_data: dict):
        """Broadcast OEE update."""
        message = {
            "type": "oee_update",
            "line_id": line_id,
            "data": oee_data,
            "timestamp": datetime.utcnow().isoformat()
        }
        await self.send_to_oee_subscribers(message, line_id)
        await self.send_to_line(message, line_id)
    
    async def broadcast_downtime_event(self, event: dict):
        """Broadcast downtime event."""
        message = {
            "type": "downtime_event",
            "data": event,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Send to downtime-specific subscribers
        line_id = event.get("line_id")
        equipment_code = event.get("equipment_code")
        
        await self.send_to_downtime_subscribers(message, line_id, equipment_code)
        
        # Also send to general line and equipment subscribers
        if line_id:
            await self.send_to_line(message, line_id)
        
        if equipment_code:
            await self.send_to_equipment(message, equipment_code)
    
    async def broadcast_job_assigned(self, job_data: dict):
        """Broadcast job assignment event."""
        message = {
            "type": "job_assigned",
            "data": job_data,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Send to job subscribers
        job_id = job_data.get("job_id")
        if job_id:
            await self.send_to_job(message, job_id)
        
        # Send to line subscribers
        line_id = job_data.get("line_id")
        if line_id:
            await self.send_to_line(message, line_id)
        
        # Send to user if they have an active connection
        user_id = job_data.get("user_id")
        if user_id:
            await self.send_to_user(message, user_id)
    
    async def broadcast_job_started(self, job_data: dict):
        """Broadcast job started event."""
        message = {
            "type": "job_started",
            "data": job_data,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Send to job subscribers
        job_id = job_data.get("job_id")
        if job_id:
            await self.send_to_job(message, job_id)
        
        # Send to line subscribers
        line_id = job_data.get("line_id")
        if line_id:
            await self.send_to_line(message, line_id)
    
    async def broadcast_job_completed(self, job_data: dict):
        """Broadcast job completed event."""
        message = {
            "type": "job_completed",
            "data": job_data,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Send to job subscribers
        job_id = job_data.get("job_id")
        if job_id:
            await self.send_to_job(message, job_id)
        
        # Send to line subscribers
        line_id = job_data.get("line_id")
        if line_id:
            await self.send_to_line(message, line_id)
    
    async def broadcast_job_cancelled(self, job_data: dict):
        """Broadcast job cancelled event."""
        message = {
            "type": "job_cancelled",
            "data": job_data,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Send to job subscribers
        job_id = job_data.get("job_id")
        if job_id:
            await self.send_to_job(message, job_id)
        
        # Send to line subscribers
        line_id = job_data.get("line_id")
        if line_id:
            await self.send_to_line(message, line_id)
    
    async def broadcast_escalation_update(self, escalation_data: dict):
        """Broadcast escalation update."""
        message = {
            "type": "escalation_update",
            "data": escalation_data,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Send to escalation-specific subscribers
        escalation_id = escalation_data.get("escalation_id")
        priority = escalation_data.get("priority")
        
        await self.send_to_escalation_subscribers(message, escalation_id, priority)
        
        # Also send to line subscribers for context
        line_id = escalation_data.get("line_id")
        if line_id:
            await self.send_to_line(message, line_id)
    
    async def broadcast_quality_alert(self, quality_data: dict):
        """Broadcast quality alert."""
        message = {
            "type": "quality_alert",
            "data": quality_data,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Send to quality-specific subscribers
        line_id = quality_data.get("line_id")
        await self.send_to_quality_subscribers(message, line_id)
        
        # Also send to line subscribers
        if line_id:
            await self.send_to_line(message, line_id)
    
    async def broadcast_changeover_started(self, changeover_data: dict):
        """Broadcast changeover started event."""
        message = {
            "type": "changeover_started",
            "data": changeover_data,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Send to changeover-specific subscribers
        line_id = changeover_data.get("line_id")
        await self.send_to_changeover_subscribers(message, line_id)
        
        # Also send to line subscribers
        if line_id:
            await self.send_to_line(message, line_id)
    
    async def broadcast_changeover_completed(self, changeover_data: dict):
        """Broadcast changeover completed event."""
        message = {
            "type": "changeover_completed",
            "data": changeover_data,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Send to changeover-specific subscribers
        line_id = changeover_data.get("line_id")
        await self.send_to_changeover_subscribers(message, line_id)
        
        # Also send to line subscribers
        if line_id:
            await self.send_to_line(message, line_id)
    
    # Health and statistics methods
    def get_connection_stats(self) -> dict:
        """Get connection statistics."""
        return {
            "active_connections": len(self.connections),
            "user_connections": len(self.user_connections),
            "line_subscriptions": len(self.line_subscriptions),
            "equipment_subscriptions": len(self.equipment_subscriptions),
            "job_subscriptions": len(self.job_subscriptions),
            "production_subscriptions": len(self.production_subscriptions),
            "oee_subscriptions": len(self.oee_subscriptions),
            "downtime_subscriptions": len(self.downtime_subscriptions),
            "andon_subscriptions": len(self.andon_subscriptions),
            "escalation_subscriptions": len(self.escalation_subscriptions),
            "quality_subscriptions": len(self.quality_subscriptions),
            "changeover_subscriptions": len(self.changeover_subscriptions)
        }
    
    def get_subscription_details(self, connection_id: str) -> dict:
        """Get subscription details for a specific connection."""
        if connection_id not in self.subscriptions:
            return {}
        
        return {
            "connection_id": connection_id,
            "subscriptions": list(self.subscriptions[connection_id]),
            "is_active": connection_id in self.connections
        }
    
    async def _broadcast_to_subscribers(self, subscription_type: str, target_id: str, message: dict):
        """Internal method to broadcast to specific subscriber types."""
        if subscription_type == "line":
            await self.send_to_line(message, target_id)
        elif subscription_type == "equipment":
            await self.send_to_equipment(message, target_id)
        elif subscription_type == "job":
            await self.send_to_job(message, target_id)
        elif subscription_type == "production":
            await self.send_to_production_subscribers(message, target_id)
        elif subscription_type == "oee":
            await self.send_to_oee_subscribers(message, target_id)
        elif subscription_type == "downtime":
            await self.send_to_downtime_subscribers(message, target_id)
        elif subscription_type == "andon":
            await self.send_to_andon_subscribers(message, target_id)
        elif subscription_type == "escalation":
            await self.send_to_escalation_subscribers(message, target_id)
        elif subscription_type == "quality":
            await self.send_to_quality_subscribers(message, target_id)
        elif subscription_type == "changeover":
            await self.send_to_changeover_subscribers(message, target_id)


# Global WebSocket manager instance
websocket_manager = WebSocketManager()
