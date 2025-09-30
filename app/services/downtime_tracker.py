"""
MS5.0 Floor Dashboard - Downtime Tracking Service

This module provides comprehensive downtime tracking and analysis services
for production equipment, including real-time detection, categorization,
and integration with the existing PLC telemetry system.
"""

from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Any, Tuple
from uuid import UUID
import structlog
from enum import Enum

from app.database import execute_query, execute_scalar, execute_update
from app.models.production import (
    DowntimeEventCreate, DowntimeEventUpdate, DowntimeEventResponse,
    DowntimeCategory, DowntimeReasonCode
)
from app.utils.exceptions import ValidationError, BusinessLogicError, NotFoundError
from app.api.websocket import broadcast_downtime_event, broadcast_downtime_statistics_update

logger = structlog.get_logger()


class DowntimeReasonCode(str, Enum):
    """Standardized downtime reason codes."""
    # Mechanical faults
    MECHANICAL_FAULT = "MECH_FAULT"
    BEARING_FAILURE = "BEARING_FAIL"
    BELT_BREAKAGE = "BELT_BREAK"
    GEAR_FAILURE = "GEAR_FAIL"
    MOTOR_FAILURE = "MOTOR_FAIL"
    
    # Electrical faults
    ELECTRICAL_FAULT = "ELEC_FAULT"
    SENSOR_FAILURE = "SENSOR_FAIL"
    PLC_FAULT = "PLC_FAULT"
    POWER_LOSS = "POWER_LOSS"
    WIRING_FAULT = "WIRING_FAULT"
    
    # Material issues
    MATERIAL_SHORTAGE = "MAT_SHORTAGE"
    MATERIAL_JAM = "MAT_JAM"
    WRONG_MATERIAL = "WRONG_MAT"
    MATERIAL_QUALITY = "MAT_QUALITY"
    
    # Upstream/Downstream issues
    UPSTREAM_STOP = "UPSTREAM_STOP"
    DOWNSTREAM_STOP = "DOWNSTREAM_STOP"
    CONVEYOR_STOP = "CONVEYOR_STOP"
    
    # Planned stops
    MAINTENANCE = "MAINTENANCE"
    CHANGEOVER = "CHANGEOVER"
    CLEANING = "CLEANING"
    INSPECTION = "INSPECTION"
    BREAK_TIME = "BREAK_TIME"
    
    # Quality issues
    QUALITY_ISSUE = "QUALITY_ISSUE"
    REJECTION = "REJECTION"
    REWORK = "REWORK"
    
    # Unknown/Other
    UNKNOWN = "UNKNOWN"
    OTHER = "OTHER"


class DowntimeTracker:
    """Comprehensive downtime tracking and analysis service."""
    
    def __init__(self):
        """Initialize downtime tracker with fault catalog."""
        self.active_events = {}  # equipment_code -> event_data
        self.fault_catalog = self._load_fault_catalog()
        self.reason_codes = self._load_reason_codes()
    
    async def detect_downtime_event(
        self, 
        line_id: UUID, 
        equipment_code: str, 
        current_status: Dict[str, Any],
        timestamp: datetime = None
    ) -> Optional[Dict[str, Any]]:
        """
        Detect and categorize downtime events based on PLC data.
        
        Args:
            line_id: Production line ID
            equipment_code: Equipment identifier
            current_status: Current equipment status from PLC
            timestamp: Event timestamp (defaults to now)
            
        Returns:
            Downtime event data if detected, None otherwise
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        try:
            # Check if equipment is currently running
            is_running = current_status.get("running", False)
            speed = current_status.get("speed", 0.0)
            
            # Determine if equipment is actually running
            is_actually_running = is_running and speed > 0.1
            
            if is_actually_running:
                # Equipment is running, check if we need to close an active event
                if equipment_code in self.active_events:
                    return await self._close_downtime_event(
                        line_id, equipment_code, timestamp
                    )
                return None
            
            # Equipment is stopped, determine reason and handle event
            if equipment_code not in self.active_events:
                # Start new downtime event
                return await self._start_downtime_event(
                    line_id, equipment_code, current_status, timestamp
                )
            else:
                # Update existing event with additional information
                return await self._update_downtime_event(
                    line_id, equipment_code, current_status, timestamp
                )
                
        except Exception as e:
            logger.error(
                "Failed to detect downtime event",
                error=str(e),
                line_id=line_id,
                equipment_code=equipment_code
            )
            raise BusinessLogicError("Failed to detect downtime event")
    
    async def _start_downtime_event(
        self,
        line_id: UUID,
        equipment_code: str,
        status: Dict[str, Any],
        timestamp: datetime
    ) -> Dict[str, Any]:
        """Start a new downtime event."""
        try:
            # Determine downtime reason
            reason_code, reason_description, category = await self._determine_downtime_reason(
                equipment_code, status
            )
            
            # Create downtime event
            event_data = {
                "line_id": line_id,
                "equipment_code": equipment_code,
                "start_time": timestamp,
                "reason_code": reason_code,
                "reason_description": reason_description,
                "category": category,
                "subcategory": self._get_subcategory(reason_code, status),
                "reported_by": None,  # Will be set when user reports
                "status": "open",
                "fault_data": self._extract_fault_data(status),
                "context_data": self._extract_context_data(status)
            }
            
            # Store in active events
            self.active_events[equipment_code] = event_data
            
            # Store in database
            event_id = await self._store_downtime_event(event_data)
            event_data["id"] = event_id
            
            logger.info(
                "Downtime event started",
                event_id=event_id,
                line_id=line_id,
                equipment_code=equipment_code,
                reason_code=reason_code,
                category=category
            )
            
            return event_data
            
        except Exception as e:
            logger.error("Failed to start downtime event", error=str(e))
            raise BusinessLogicError("Failed to start downtime event")
    
    async def _close_downtime_event(
        self,
        line_id: UUID,
        equipment_code: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """Close an active downtime event."""
        try:
            if equipment_code not in self.active_events:
                return None
            
            event_data = self.active_events[equipment_code]
            event_id = event_data.get("id")
            
            if not event_id:
                # Event was not stored in database, remove from active events
                del self.active_events[equipment_code]
                return None
            
            # Calculate duration
            start_time = event_data["start_time"]
            duration_seconds = int((timestamp - start_time).total_seconds())
            
            # Update event in database
            await self._update_downtime_event_in_db(
                event_id, 
                end_time=timestamp,
                duration_seconds=duration_seconds,
                status="closed"
            )
            
            # Remove from active events
            del self.active_events[equipment_code]
            
            # Update event data
            event_data.update({
                "end_time": timestamp,
                "duration_seconds": duration_seconds,
                "status": "closed"
            })
            
            logger.info(
                "Downtime event closed",
                event_id=event_id,
                line_id=line_id,
                equipment_code=equipment_code,
                duration_seconds=duration_seconds
            )
            
            # Broadcast real-time update
            try:
                await broadcast_downtime_event({
                    "id": str(event_id),
                    "line_id": str(line_id),
                    "equipment_code": equipment_code,
                    "start_time": start_time.isoformat(),
                    "end_time": timestamp.isoformat(),
                    "duration_seconds": duration_seconds,
                    "reason_code": event_data["reason_code"],
                    "reason_description": event_data["reason_description"],
                    "category": event_data["category"],
                    "subcategory": event_data["subcategory"],
                    "status": "closed"
                })
            except Exception as e:
                logger.warning("Failed to broadcast downtime event closure", error=str(e))
            
            return event_data
            
        except Exception as e:
            logger.error("Failed to close downtime event", error=str(e))
            raise BusinessLogicError("Failed to close downtime event")
    
    async def _update_downtime_event(
        self,
        line_id: UUID,
        equipment_code: str,
        status: Dict[str, Any],
        timestamp: datetime
    ) -> Dict[str, Any]:
        """Update an existing downtime event with additional data."""
        try:
            if equipment_code not in self.active_events:
                return None
            
            event_data = self.active_events[equipment_code]
            event_id = event_data.get("id")
            
            if not event_id:
                return None
            
            # Update fault data and context
            new_fault_data = self._extract_fault_data(status)
            new_context_data = self._extract_context_data(status)
            
            # Merge with existing data
            event_data["fault_data"].update(new_fault_data)
            event_data["context_data"].update(new_context_data)
            
            # Update in database
            await self._update_downtime_event_in_db(
                event_id,
                fault_data=event_data["fault_data"],
                context_data=event_data["context_data"]
            )
            
            return event_data
            
        except Exception as e:
            logger.error("Failed to update downtime event", error=str(e))
            raise BusinessLogicError("Failed to update downtime event")
    
    async def _determine_downtime_reason(
        self, 
        equipment_code: str, 
        status: Dict[str, Any]
    ) -> Tuple[str, str, str]:
        """Determine downtime reason from equipment status."""
        try:
            # Check for active faults first
            fault_bits = status.get("fault_bits", [])
            active_faults = []
            
            for i, bit_active in enumerate(fault_bits):
                if bit_active:
                    fault_info = self.fault_catalog.get(i, {})
                    if fault_info:
                        active_faults.append({
                            "bit_index": i,
                            "name": fault_info.get("name", f"Fault {i}"),
                            "description": fault_info.get("description", "Unknown fault"),
                            "marker": fault_info.get("marker", "INTERNAL"),
                            "severity": fault_info.get("severity", "medium")
                        })
            
            if active_faults:
                # Prioritize faults by marker and severity
                internal_faults = [f for f in active_faults if f["marker"] == "INTERNAL"]
                upstream_faults = [f for f in active_faults if f["marker"] == "UPSTREAM"]
                downstream_faults = [f for f in active_faults if f["marker"] == "DOWNSTREAM"]
                
                if internal_faults:
                    fault = internal_faults[0]  # Take first internal fault
                    return (
                        self._map_fault_to_reason_code(fault["name"]),
                        fault["description"],
                        "unplanned"
                    )
                elif upstream_faults:
                    fault = upstream_faults[0]
                    return (
                        DowntimeReasonCode.UPSTREAM_STOP,
                        f"Upstream: {fault['description']}",
                        "unplanned"
                    )
                elif downstream_faults:
                    fault = downstream_faults[0]
                    return (
                        DowntimeReasonCode.DOWNSTREAM_STOP,
                        f"Downstream: {fault['description']}",
                        "unplanned"
                    )
            
            # Check for planned stops
            if status.get("planned_stop", False):
                return (
                    DowntimeReasonCode.MAINTENANCE,
                    "Planned maintenance stop",
                    "planned"
                )
            
            # Check for speed-based stops
            speed = status.get("speed", 0.0)
            if speed == 0.0:
                return (
                    DowntimeReasonCode.UNKNOWN,
                    "Equipment stopped - no active faults detected",
                    "unplanned"
                )
            
            # Check for material issues
            if status.get("material_shortage", False):
                return (
                    DowntimeReasonCode.MATERIAL_SHORTAGE,
                    "Material shortage",
                    "unplanned"
                )
            
            if status.get("material_jam", False):
                return (
                    DowntimeReasonCode.MATERIAL_JAM,
                    "Material jam",
                    "unplanned"
                )
            
            # Default to unknown
            return (
                DowntimeReasonCode.UNKNOWN,
                "Unknown reason - equipment stopped",
                "unplanned"
            )
            
        except Exception as e:
            logger.error("Failed to determine downtime reason", error=str(e))
            return (
                DowntimeReasonCode.UNKNOWN,
                "Error determining reason",
                "unplanned"
            )
    
    def _map_fault_to_reason_code(self, fault_name: str) -> str:
        """Map fault name to standardized reason code."""
        fault_name_lower = fault_name.lower()
        
        if "bearing" in fault_name_lower:
            return DowntimeReasonCode.BEARING_FAILURE
        elif "belt" in fault_name_lower:
            return DowntimeReasonCode.BELT_BREAKAGE
        elif "gear" in fault_name_lower:
            return DowntimeReasonCode.GEAR_FAILURE
        elif "motor" in fault_name_lower:
            return DowntimeReasonCode.MOTOR_FAILURE
        elif "sensor" in fault_name_lower:
            return DowntimeReasonCode.SENSOR_FAILURE
        elif "plc" in fault_name_lower:
            return DowntimeReasonCode.PLC_FAULT
        elif "power" in fault_name_lower:
            return DowntimeReasonCode.POWER_LOSS
        elif "wiring" in fault_name_lower:
            return DowntimeReasonCode.WIRING_FAULT
        elif "quality" in fault_name_lower:
            return DowntimeReasonCode.QUALITY_ISSUE
        else:
            return DowntimeReasonCode.MECHANICAL_FAULT
    
    def _get_subcategory(self, reason_code: str, status: Dict[str, Any]) -> Optional[str]:
        """Get subcategory for downtime reason."""
        if reason_code == DowntimeReasonCode.MAINTENANCE:
            return "preventive" if status.get("preventive_maintenance", False) else "corrective"
        elif reason_code == DowntimeReasonCode.CHANGEOVER:
            return "product_change" if status.get("product_change", False) else "tooling_change"
        elif reason_code in [DowntimeReasonCode.MATERIAL_SHORTAGE, DowntimeReasonCode.MATERIAL_JAM]:
            return "raw_material" if status.get("raw_material", False) else "packaging"
        else:
            return None
    
    def _extract_fault_data(self, status: Dict[str, Any]) -> Dict[str, Any]:
        """Extract fault-related data from status."""
        return {
            "fault_bits": status.get("fault_bits", []),
            "active_alarms": status.get("active_alarms", []),
            "error_codes": status.get("error_codes", []),
            "fault_count": status.get("fault_count", 0),
            "last_fault_time": status.get("last_fault_time"),
            "fault_history": status.get("fault_history", [])
        }
    
    def _extract_context_data(self, status: Dict[str, Any]) -> Dict[str, Any]:
        """Extract contextual data from status."""
        return {
            "speed": status.get("speed", 0.0),
            "temperature": status.get("temperature"),
            "pressure": status.get("pressure"),
            "vibration": status.get("vibration"),
            "current_product": status.get("current_product"),
            "production_count": status.get("production_count", 0),
            "shift": status.get("shift"),
            "operator": status.get("operator"),
            "environmental_conditions": status.get("environmental_conditions", {})
        }
    
    async def _store_downtime_event(self, event_data: Dict[str, Any]) -> UUID:
        """Store downtime event in database."""
        try:
            insert_query = """
            INSERT INTO factory_telemetry.downtime_events 
            (line_id, equipment_code, start_time, reason_code, reason_description, 
             category, subcategory, reported_by, fault_data, context_data)
            VALUES (:line_id, :equipment_code, :start_time, :reason_code, 
                   :reason_description, :category, :subcategory, :reported_by, 
                   :fault_data, :context_data)
            RETURNING id
            """
            
            result = await execute_query(insert_query, {
                "line_id": event_data["line_id"],
                "equipment_code": event_data["equipment_code"],
                "start_time": event_data["start_time"],
                "reason_code": event_data["reason_code"],
                "reason_description": event_data["reason_description"],
                "category": event_data["category"],
                "subcategory": event_data["subcategory"],
                "reported_by": event_data["reported_by"],
                "fault_data": event_data["fault_data"],
                "context_data": event_data["context_data"]
            })
            
            if not result:
                raise BusinessLogicError("Failed to store downtime event")
            
            event_id = result[0]["id"]
            
            # Broadcast real-time update
            try:
                await broadcast_downtime_event({
                    "id": str(event_id),
                    "line_id": str(event_data["line_id"]),
                    "equipment_code": event_data["equipment_code"],
                    "start_time": event_data["start_time"].isoformat(),
                    "reason_code": event_data["reason_code"],
                    "reason_description": event_data["reason_description"],
                    "category": event_data["category"],
                    "subcategory": event_data["subcategory"],
                    "status": "open"
                })
            except Exception as e:
                logger.warning("Failed to broadcast downtime event", error=str(e))
            
            return event_id
            
        except Exception as e:
            logger.error("Failed to store downtime event", error=str(e))
            raise BusinessLogicError("Failed to store downtime event")
    
    async def _update_downtime_event_in_db(
        self,
        event_id: UUID,
        end_time: datetime = None,
        duration_seconds: int = None,
        status: str = None,
        fault_data: Dict[str, Any] = None,
        context_data: Dict[str, Any] = None
    ):
        """Update downtime event in database."""
        try:
            update_fields = []
            params = {"event_id": event_id}
            
            if end_time is not None:
                update_fields.append("end_time = :end_time")
                params["end_time"] = end_time
            
            if duration_seconds is not None:
                update_fields.append("duration_seconds = :duration_seconds")
                params["duration_seconds"] = duration_seconds
            
            if status is not None:
                update_fields.append("status = :status")
                params["status"] = status
            
            if fault_data is not None:
                update_fields.append("fault_data = :fault_data")
                params["fault_data"] = fault_data
            
            if context_data is not None:
                update_fields.append("context_data = :context_data")
                params["context_data"] = context_data
            
            if not update_fields:
                return
            
            update_query = f"""
            UPDATE factory_telemetry.downtime_events 
            SET {', '.join(update_fields)}
            WHERE id = :event_id
            """
            
            await execute_update(update_query, params)
            
        except Exception as e:
            logger.error("Failed to update downtime event", error=str(e))
            raise BusinessLogicError("Failed to update downtime event")
    
    async def get_downtime_events(
        self,
        line_id: Optional[UUID] = None,
        equipment_code: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        category: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[DowntimeEventResponse]:
        """Get downtime events with filtering."""
        try:
            where_conditions = []
            params = {"limit": limit, "offset": offset}
            
            if line_id:
                where_conditions.append("de.line_id = :line_id")
                params["line_id"] = line_id
            
            if equipment_code:
                where_conditions.append("de.equipment_code = :equipment_code")
                params["equipment_code"] = equipment_code
            
            if start_date:
                where_conditions.append("DATE(de.start_time) >= :start_date")
                params["start_date"] = start_date
            
            if end_date:
                where_conditions.append("DATE(de.start_time) <= :end_date")
                params["end_date"] = end_date
            
            if category:
                where_conditions.append("de.category = :category")
                params["category"] = category
            
            if status:
                where_conditions.append("de.status = :status")
                params["status"] = status
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            query = f"""
            SELECT de.id, de.line_id, de.equipment_code, de.start_time, de.end_time,
                   de.duration_seconds, de.reason_code, de.reason_description,
                   de.category, de.subcategory, de.reported_by, de.confirmed_by,
                   de.confirmed_at, de.notes, de.fault_data, de.context_data,
                   pl.line_code, pl.name as line_name,
                   u1.username as reported_by_username,
                   u2.username as confirmed_by_username
            FROM factory_telemetry.downtime_events de
            JOIN factory_telemetry.production_lines pl ON de.line_id = pl.id
            LEFT JOIN factory_telemetry.users u1 ON de.reported_by = u1.id
            LEFT JOIN factory_telemetry.users u2 ON de.confirmed_by = u2.id
            WHERE {where_clause}
            ORDER BY de.start_time DESC
            LIMIT :limit OFFSET :offset
            """
            
            result = await execute_query(query, params)
            
            events = []
            for row in result:
                events.append(DowntimeEventResponse(
                    id=row["id"],
                    line_id=row["line_id"],
                    equipment_code=row["equipment_code"],
                    start_time=row["start_time"],
                    end_time=row["end_time"],
                    duration_seconds=row["duration_seconds"],
                    reason_code=row["reason_code"],
                    reason_description=row["reason_description"],
                    category=row["category"],
                    subcategory=row["subcategory"],
                    reported_by=row["reported_by"],
                    confirmed_by=row["confirmed_by"],
                    confirmed_at=row["confirmed_at"],
                    notes=row["notes"],
                    fault_data=row["fault_data"],
                    context_data=row["context_data"],
                    line_code=row["line_code"],
                    line_name=row["line_name"],
                    reported_by_username=row["reported_by_username"],
                    confirmed_by_username=row["confirmed_by_username"]
                ))
            
            return events
            
        except Exception as e:
            logger.error("Failed to get downtime events", error=str(e))
            raise BusinessLogicError("Failed to get downtime events")
    
    async def get_downtime_statistics(
        self,
        line_id: Optional[UUID] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """Get downtime statistics and analysis."""
        try:
            where_conditions = []
            params = {}
            
            if line_id:
                where_conditions.append("de.line_id = :line_id")
                params["line_id"] = line_id
            
            if start_date:
                where_conditions.append("DATE(de.start_time) >= :start_date")
                params["start_date"] = start_date
            
            if end_date:
                where_conditions.append("DATE(de.start_time) <= :end_date")
                params["end_date"] = end_date
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            # Get overall statistics
            stats_query = f"""
            SELECT 
                COUNT(*) as total_events,
                COALESCE(SUM(duration_seconds), 0) as total_downtime_seconds,
                COALESCE(AVG(duration_seconds), 0) as avg_duration_seconds,
                COUNT(CASE WHEN category = 'unplanned' THEN 1 END) as unplanned_events,
                COUNT(CASE WHEN category = 'planned' THEN 1 END) as planned_events,
                COUNT(CASE WHEN category = 'maintenance' THEN 1 END) as maintenance_events,
                COUNT(CASE WHEN category = 'changeover' THEN 1 END) as changeover_events
            FROM factory_telemetry.downtime_events de
            WHERE {where_clause}
            """
            
            stats_result = await execute_query(stats_query, params)
            stats = stats_result[0] if stats_result else {}
            
            # Get top reasons
            reasons_query = f"""
            SELECT 
                reason_code,
                reason_description,
                COUNT(*) as event_count,
                COALESCE(SUM(duration_seconds), 0) as total_duration_seconds
            FROM factory_telemetry.downtime_events de
            WHERE {where_clause}
            GROUP BY reason_code, reason_description
            ORDER BY event_count DESC, total_duration_seconds DESC
            LIMIT 10
            """
            
            reasons_result = await execute_query(reasons_query, params)
            
            # Get daily breakdown
            daily_query = f"""
            SELECT 
                DATE(de.start_time) as event_date,
                COUNT(*) as event_count,
                COALESCE(SUM(duration_seconds), 0) as total_duration_seconds
            FROM factory_telemetry.downtime_events de
            WHERE {where_clause}
            GROUP BY DATE(de.start_time)
            ORDER BY event_date DESC
            LIMIT 30
            """
            
            daily_result = await execute_query(daily_query, params)
            
            return {
                "total_events": stats.get("total_events", 0),
                "total_downtime_seconds": stats.get("total_downtime_seconds", 0),
                "total_downtime_minutes": round(stats.get("total_downtime_seconds", 0) / 60, 2),
                "total_downtime_hours": round(stats.get("total_downtime_seconds", 0) / 3600, 2),
                "avg_duration_seconds": round(stats.get("avg_duration_seconds", 0), 2),
                "avg_duration_minutes": round(stats.get("avg_duration_seconds", 0) / 60, 2),
                "unplanned_events": stats.get("unplanned_events", 0),
                "planned_events": stats.get("planned_events", 0),
                "maintenance_events": stats.get("maintenance_events", 0),
                "changeover_events": stats.get("changeover_events", 0),
                "top_reasons": [
                    {
                        "reason_code": row["reason_code"],
                        "reason_description": row["reason_description"],
                        "event_count": row["event_count"],
                        "total_duration_seconds": row["total_duration_seconds"],
                        "total_duration_minutes": round(row["total_duration_seconds"] / 60, 2)
                    }
                    for row in reasons_result
                ],
                "daily_breakdown": [
                    {
                        "date": row["event_date"],
                        "event_count": row["event_count"],
                        "total_duration_seconds": row["total_duration_seconds"],
                        "total_duration_minutes": round(row["total_duration_seconds"] / 60, 2)
                    }
                    for row in daily_result
                ]
            }
            
        except Exception as e:
            logger.error("Failed to get downtime statistics", error=str(e))
            raise BusinessLogicError("Failed to get downtime statistics")
    
    async def confirm_downtime_event(
        self,
        event_id: UUID,
        confirmed_by: UUID,
        notes: Optional[str] = None
    ) -> DowntimeEventResponse:
        """Confirm a downtime event."""
        try:
            # Update event in database
            update_query = """
            UPDATE factory_telemetry.downtime_events 
            SET confirmed_by = :confirmed_by, confirmed_at = NOW(), 
                notes = COALESCE(:notes, notes)
            WHERE id = :event_id
            RETURNING id
            """
            
            result = await execute_query(update_query, {
                "event_id": event_id,
                "confirmed_by": confirmed_by,
                "notes": notes
            })
            
            if not result:
                raise NotFoundError("Downtime event", str(event_id))
            
            # Get updated event
            events = await self.get_downtime_events(limit=1)
            if not events:
                raise NotFoundError("Downtime event", str(event_id))
            
            logger.info(
                "Downtime event confirmed",
                event_id=event_id,
                confirmed_by=confirmed_by
            )
            
            # Broadcast real-time update
            try:
                confirmed_event = events[0]
                await broadcast_downtime_event({
                    "id": str(confirmed_event.id),
                    "line_id": str(confirmed_event.line_id),
                    "equipment_code": confirmed_event.equipment_code,
                    "start_time": confirmed_event.start_time.isoformat(),
                    "end_time": confirmed_event.end_time.isoformat() if confirmed_event.end_time else None,
                    "duration_seconds": confirmed_event.duration_seconds,
                    "reason_code": confirmed_event.reason_code,
                    "reason_description": confirmed_event.reason_description,
                    "category": confirmed_event.category,
                    "subcategory": confirmed_event.subcategory,
                    "status": "confirmed",
                    "confirmed_by": str(confirmed_event.confirmed_by),
                    "confirmed_at": confirmed_event.confirmed_at.isoformat() if confirmed_event.confirmed_at else None
                })
            except Exception as e:
                logger.warning("Failed to broadcast downtime event confirmation", error=str(e))
            
            return events[0]
            
        except (NotFoundError, BusinessLogicError):
            raise
        except Exception as e:
            logger.error("Failed to confirm downtime event", error=str(e))
            raise BusinessLogicError("Failed to confirm downtime event")
    
    def _load_fault_catalog(self) -> Dict[int, Dict[str, Any]]:
        """Load fault catalog from database or configuration."""
        # This would typically load from a database table or configuration file
        # For now, return a basic fault catalog
        return {
            0: {"name": "Emergency Stop", "description": "Emergency stop activated", "marker": "INTERNAL", "severity": "critical"},
            1: {"name": "Safety Gate Open", "description": "Safety gate is open", "marker": "INTERNAL", "severity": "high"},
            2: {"name": "Motor Overload", "description": "Motor overload protection triggered", "marker": "INTERNAL", "severity": "high"},
            3: {"name": "Temperature High", "description": "Equipment temperature too high", "marker": "INTERNAL", "severity": "medium"},
            4: {"name": "Pressure Low", "description": "System pressure below threshold", "marker": "INTERNAL", "severity": "medium"},
            5: {"name": "Upstream Stop", "description": "Upstream equipment stopped", "marker": "UPSTREAM", "severity": "medium"},
            6: {"name": "Downstream Stop", "description": "Downstream equipment stopped", "marker": "DOWNSTREAM", "severity": "medium"},
            7: {"name": "Material Jam", "description": "Material jam detected", "marker": "INTERNAL", "severity": "medium"},
            8: {"name": "Sensor Fault", "description": "Sensor malfunction", "marker": "INTERNAL", "severity": "low"},
            9: {"name": "Communication Error", "description": "Communication with PLC lost", "marker": "INTERNAL", "severity": "high"}
        }
    
    def _load_reason_codes(self) -> Dict[str, Dict[str, Any]]:
        """Load reason codes and their descriptions."""
        return {
            code.value: {
                "description": code.value.replace("_", " ").title(),
                "category": self._get_reason_category(code.value)
            }
            for code in DowntimeReasonCode
        }
    
    def _get_reason_category(self, reason_code: str) -> str:
        """Get category for reason code."""
        if reason_code.startswith("MECH_") or reason_code.startswith("ELEC_"):
            return "unplanned"
        elif reason_code.startswith("MAT_"):
            return "unplanned"
        elif reason_code in ["MAINTENANCE", "CHANGEOVER", "CLEANING", "INSPECTION", "BREAK_TIME"]:
            return "planned"
        elif reason_code in ["UPSTREAM_STOP", "DOWNSTREAM_STOP", "CONVEYOR_STOP"]:
            return "unplanned"
        elif reason_code.startswith("QUALITY_"):
            return "unplanned"
        else:
            return "unplanned"
