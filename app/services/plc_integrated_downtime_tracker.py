"""
MS5.0 Floor Dashboard - PLC Integrated Downtime Tracker

This module extends the existing DowntimeTracker to integrate with PLC fault
detection systems, providing enhanced downtime tracking based on real-time
PLC data and automated fault-to-downtime event mapping.
"""

from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Any, Tuple
from uuid import UUID
import structlog

from app.services.downtime_tracker import DowntimeTracker, DowntimeReasonCode
from app.services.andon_service import AndonService
from app.database import execute_query, execute_scalar, execute_update
from app.utils.exceptions import BusinessLogicError, NotFoundError

logger = structlog.get_logger()


class PLCIntegratedDowntimeTracker(DowntimeTracker):
    """Downtime tracker integrated with PLC fault detection."""
    
    def __init__(self):
        super().__init__()
        self.andon_service = AndonService()
        self.plc_fault_cache = {}
        self.fault_to_downtime_mapping = self._initialize_fault_mapping()
        self.auto_andon_enabled = True
        self.andon_thresholds = self._initialize_andon_thresholds()
    
    async def detect_downtime_event_from_plc(
        self,
        line_id: UUID,
        equipment_code: str,
        plc_data: Dict[str, Any],
        context_data: Dict[str, Any] = None,
        timestamp: datetime = None
    ) -> Optional[Dict[str, Any]]:
        """Detect downtime events from PLC data and production context."""
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        try:
            # Analyze PLC data for downtime indicators
            downtime_indicators = await self._analyze_plc_downtime_indicators(plc_data, context_data)
            
            if not downtime_indicators["is_downtime"]:
                # Equipment is running normally, check if we need to close active events
                return await self._check_for_downtime_resolution(equipment_code, timestamp)
            
            # Equipment is down, determine reason and create/update event
            return await self._handle_plc_downtime_detection(
                line_id, equipment_code, downtime_indicators, plc_data, context_data, timestamp
            )
            
        except Exception as e:
            logger.error(
                "Failed to detect downtime event from PLC data",
                error=str(e),
                line_id=line_id,
                equipment_code=equipment_code
            )
            raise BusinessLogicError("Failed to detect downtime event from PLC data")
    
    async def _analyze_plc_downtime_indicators(
        self, 
        plc_data: Dict[str, Any], 
        context_data: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Analyze PLC data to identify downtime indicators."""
        try:
            processed = plc_data.get("processed", {})
            
            # Check running status
            is_running = processed.get("running_status", False)
            speed = processed.get("speed_real", 0.0)
            has_faults = processed.get("has_active_faults", False)
            
            # Determine if equipment is actually running
            is_actually_running = is_running and speed > 0.1
            
            # Get fault information
            fault_bits = processed.get("fault_bits", [False] * 64)
            active_alarms = processed.get("active_alarms", [])
            
            # Analyze faults
            fault_analysis = self._analyze_plc_faults(fault_bits, active_alarms)
            
            # Check for planned stops
            planned_stop = context_data.get("planned_stop", False) if context_data else False
            planned_stop_reason = context_data.get("planned_stop_reason", "") if context_data else ""
            
            # Check for material issues
            material_shortage = processed.get("material_shortage", False)
            material_jam = processed.get("material_jam", False)
            
            # Determine downtime status
            is_downtime = not is_actually_running
            
            return {
                "is_downtime": is_downtime,
                "is_running": is_running,
                "speed": speed,
                "has_faults": has_faults,
                "planned_stop": planned_stop,
                "planned_stop_reason": planned_stop_reason,
                "material_shortage": material_shortage,
                "material_jam": material_jam,
                "fault_analysis": fault_analysis,
                "downtime_category": self._determine_downtime_category(
                    is_downtime, has_faults, planned_stop, material_shortage, material_jam
                ),
                "downtime_reason": self._determine_downtime_reason(
                    fault_analysis, planned_stop_reason, material_shortage, material_jam
                )
            }
            
        except Exception as e:
            logger.error("Failed to analyze PLC downtime indicators", error=str(e))
            return {
                "is_downtime": False,
                "is_running": False,
                "speed": 0.0,
                "has_faults": False,
                "planned_stop": False,
                "planned_stop_reason": "",
                "material_shortage": False,
                "material_jam": False,
                "fault_analysis": {},
                "downtime_category": "unknown",
                "downtime_reason": "Unknown"
            }
    
    def _analyze_plc_faults(self, fault_bits: List[bool], active_alarms: List[str]) -> Dict[str, Any]:
        """Analyze PLC fault bits and active alarms."""
        try:
            fault_analysis = {
                "active_fault_bits": [],
                "active_alarms": active_alarms,
                "fault_count": 0,
                "critical_faults": [],
                "internal_faults": [],
                "upstream_faults": [],
                "downstream_faults": [],
                "fault_categories": {}
            }
            
            # Analyze fault bits
            for i, bit_active in enumerate(fault_bits):
                if bit_active:
                    fault_analysis["active_fault_bits"].append(i)
                    fault_analysis["fault_count"] += 1
                    
                    # Get fault information from catalog
                    fault_info = self.fault_catalog.get(i, {
                        "name": f"Fault {i}",
                        "description": "Unknown fault",
                        "marker": "INTERNAL",
                        "severity": "medium"
                    })
                    
                    # Categorize faults
                    marker = fault_info.get("marker", "INTERNAL")
                    severity = fault_info.get("severity", "medium")
                    
                    if severity == "critical":
                        fault_analysis["critical_faults"].append(fault_info)
                    
                    if marker == "INTERNAL":
                        fault_analysis["internal_faults"].append(fault_info)
                    elif marker == "UPSTREAM":
                        fault_analysis["upstream_faults"].append(fault_info)
                    elif marker == "DOWNSTREAM":
                        fault_analysis["downstream_faults"].append(fault_info)
                    
                    # Count by category
                    category = self._get_fault_category(marker, severity)
                    fault_analysis["fault_categories"][category] = fault_analysis["fault_categories"].get(category, 0) + 1
            
            return fault_analysis
            
        except Exception as e:
            logger.error("Failed to analyze PLC faults", error=str(e))
            return {
                "active_fault_bits": [],
                "active_alarms": [],
                "fault_count": 0,
                "critical_faults": [],
                "internal_faults": [],
                "upstream_faults": [],
                "downstream_faults": [],
                "fault_categories": {}
            }
    
    def _determine_downtime_category(
        self, 
        is_downtime: bool, 
        has_faults: bool, 
        planned_stop: bool, 
        material_shortage: bool, 
        material_jam: bool
    ) -> str:
        """Determine downtime category based on indicators."""
        if not is_downtime:
            return "none"
        
        if planned_stop:
            return "planned"
        elif has_faults:
            return "unplanned"
        elif material_shortage or material_jam:
            return "material"
        else:
            return "unplanned"
    
    def _determine_downtime_reason(
        self, 
        fault_analysis: Dict, 
        planned_stop_reason: str, 
        material_shortage: bool, 
        material_jam: bool
    ) -> Tuple[str, str]:
        """Determine downtime reason code and description."""
        try:
            # Check for critical faults first
            critical_faults = fault_analysis.get("critical_faults", [])
            if critical_faults:
                fault = critical_faults[0]
                return self._map_fault_to_reason_code(fault["name"]), fault["description"]
            
            # Check for internal faults
            internal_faults = fault_analysis.get("internal_faults", [])
            if internal_faults:
                fault = internal_faults[0]
                return self._map_fault_to_reason_code(fault["name"]), fault["description"]
            
            # Check for upstream/downstream faults
            upstream_faults = fault_analysis.get("upstream_faults", [])
            if upstream_faults:
                return DowntimeReasonCode.UPSTREAM_STOP, f"Upstream: {upstream_faults[0]['description']}"
            
            downstream_faults = fault_analysis.get("downstream_faults", [])
            if downstream_faults:
                return DowntimeReasonCode.DOWNSTREAM_STOP, f"Downstream: {downstream_faults[0]['description']}"
            
            # Check for material issues
            if material_shortage:
                return DowntimeReasonCode.MATERIAL_SHORTAGE, "Material shortage detected"
            
            if material_jam:
                return DowntimeReasonCode.MATERIAL_JAM, "Material jam detected"
            
            # Check for planned stops
            if planned_stop_reason:
                return DowntimeReasonCode.MAINTENANCE, planned_stop_reason
            
            # Default to unknown
            return DowntimeReasonCode.UNKNOWN, "Equipment stopped - reason unknown"
            
        except Exception as e:
            logger.error("Failed to determine downtime reason", error=str(e))
            return DowntimeReasonCode.UNKNOWN, "Error determining downtime reason"
    
    async def _handle_plc_downtime_detection(
        self,
        line_id: UUID,
        equipment_code: str,
        downtime_indicators: Dict,
        plc_data: Dict,
        context_data: Dict,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """Handle PLC downtime detection and event creation."""
        try:
            # Check if we already have an active downtime event
            active_event = self.active_events.get(equipment_code)
            
            if active_event:
                # Update existing event
                return await self._update_plc_downtime_event(
                    active_event, downtime_indicators, plc_data, context_data, timestamp
                )
            else:
                # Create new downtime event
                return await self._create_plc_downtime_event(
                    line_id, equipment_code, downtime_indicators, plc_data, context_data, timestamp
                )
                
        except Exception as e:
            logger.error("Failed to handle PLC downtime detection", error=str(e))
            raise BusinessLogicError("Failed to handle PLC downtime detection")
    
    async def _create_plc_downtime_event(
        self,
        line_id: UUID,
        equipment_code: str,
        downtime_indicators: Dict,
        plc_data: Dict,
        context_data: Dict,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """Create new downtime event from PLC data."""
        try:
            # Determine downtime reason
            reason_code, reason_description = self._determine_downtime_reason(
                downtime_indicators["fault_analysis"],
                downtime_indicators["planned_stop_reason"],
                downtime_indicators["material_shortage"],
                downtime_indicators["material_jam"]
            )
            
            # Create downtime event data
            event_data = {
                "line_id": line_id,
                "equipment_code": equipment_code,
                "start_time": timestamp,
                "reason_code": reason_code,
                "reason_description": reason_description,
                "category": downtime_indicators["downtime_category"],
                "subcategory": self._get_downtime_subcategory(reason_code, downtime_indicators),
                "reported_by": None,  # Auto-generated from PLC
                "status": "open",
                "fault_data": self._extract_plc_fault_data(plc_data, downtime_indicators),
                "context_data": self._extract_plc_context_data(plc_data, context_data),
                "plc_source": True,  # Mark as PLC-generated
                "auto_detected": True
            }
            
            # Store in active events
            self.active_events[equipment_code] = event_data
            
            # Store in database
            event_id = await self._store_downtime_event(event_data)
            event_data["id"] = event_id
            
            # Trigger Andon event if configured
            if self.auto_andon_enabled:
                await self._trigger_andon_for_downtime(event_data, downtime_indicators)
            
            logger.info(
                "PLC downtime event created",
                event_id=event_id,
                line_id=line_id,
                equipment_code=equipment_code,
                reason_code=reason_code,
                category=downtime_indicators["downtime_category"]
            )
            
            return event_data
            
        except Exception as e:
            logger.error("Failed to create PLC downtime event", error=str(e))
            raise BusinessLogicError("Failed to create PLC downtime event")
    
    async def _update_plc_downtime_event(
        self,
        active_event: Dict,
        downtime_indicators: Dict,
        plc_data: Dict,
        context_data: Dict,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """Update existing downtime event with new PLC data."""
        try:
            event_id = active_event.get("id")
            if not event_id:
                return active_event
            
            # Update fault data and context
            new_fault_data = self._extract_plc_fault_data(plc_data, downtime_indicators)
            new_context_data = self._extract_plc_context_data(plc_data, context_data)
            
            # Merge with existing data
            active_event["fault_data"].update(new_fault_data)
            active_event["context_data"].update(new_context_data)
            
            # Update in database
            await self._update_downtime_event_in_db(
                event_id,
                fault_data=active_event["fault_data"],
                context_data=active_event["context_data"]
            )
            
            # Check if we should trigger additional Andon events
            if self.auto_andon_enabled:
                await self._check_andon_escalation(active_event, downtime_indicators)
            
            return active_event
            
        except Exception as e:
            logger.error("Failed to update PLC downtime event", error=str(e))
            return active_event
    
    async def _check_for_downtime_resolution(
        self, 
        equipment_code: str, 
        timestamp: datetime
    ) -> Optional[Dict[str, Any]]:
        """Check if we need to resolve an active downtime event."""
        try:
            if equipment_code not in self.active_events:
                return None
            
            # Equipment is running again, close the downtime event
            return await self._close_downtime_event(
                equipment_code, timestamp
            )
            
        except Exception as e:
            logger.error("Failed to check for downtime resolution", error=str(e))
            return None
    
    def _extract_plc_fault_data(self, plc_data: Dict, downtime_indicators: Dict) -> Dict[str, Any]:
        """Extract fault-related data from PLC data."""
        try:
            processed = plc_data.get("processed", {})
            fault_analysis = downtime_indicators["fault_analysis"]
            
            return {
                "fault_bits": processed.get("fault_bits", []),
                "active_alarms": processed.get("active_alarms", []),
                "fault_count": fault_analysis.get("fault_count", 0),
                "critical_faults": fault_analysis.get("critical_faults", []),
                "internal_faults": fault_analysis.get("internal_faults", []),
                "upstream_faults": fault_analysis.get("upstream_faults", []),
                "downstream_faults": fault_analysis.get("downstream_faults", []),
                "fault_categories": fault_analysis.get("fault_categories", {}),
                "plc_timestamp": processed.get("timestamp"),
                "plc_communication_status": processed.get("communication_status", "ok")
            }
            
        except Exception as e:
            logger.error("Failed to extract PLC fault data", error=str(e))
            return {}
    
    def _extract_plc_context_data(self, plc_data: Dict, context_data: Dict = None) -> Dict[str, Any]:
        """Extract contextual data from PLC data and production context."""
        try:
            processed = plc_data.get("processed", {})
            
            context = {
                "speed": processed.get("speed_real", 0.0),
                "running_status": processed.get("running_status", False),
                "product_count": processed.get("product_count", 0),
                "current_product": processed.get("current_product", 0),
                "temperature": processed.get("temperature"),
                "pressure": processed.get("pressure"),
                "vibration": processed.get("vibration"),
                "plc_timestamp": processed.get("timestamp"),
                "plc_communication_status": processed.get("communication_status", "ok")
            }
            
            # Add production context if available
            if context_data:
                context.update({
                    "production_line_id": context_data.get("production_line_id"),
                    "current_job_id": context_data.get("current_job_id"),
                    "target_quantity": context_data.get("target_quantity"),
                    "actual_quantity": context_data.get("actual_quantity"),
                    "current_operator": context_data.get("current_operator"),
                    "current_shift": context_data.get("current_shift"),
                    "planned_stop": context_data.get("planned_stop"),
                    "planned_stop_reason": context_data.get("planned_stop_reason")
                })
            
            return context
            
        except Exception as e:
            logger.error("Failed to extract PLC context data", error=str(e))
            return {}
    
    def _get_downtime_subcategory(self, reason_code: str, downtime_indicators: Dict) -> Optional[str]:
        """Get subcategory for downtime reason based on PLC indicators."""
        try:
            if reason_code == DowntimeReasonCode.MAINTENANCE:
                return "preventive" if downtime_indicators.get("planned_stop") else "corrective"
            elif reason_code == DowntimeReasonCode.CHANGEOVER:
                return "product_change" if downtime_indicators.get("product_change") else "tooling_change"
            elif reason_code in [DowntimeReasonCode.MATERIAL_SHORTAGE, DowntimeReasonCode.MATERIAL_JAM]:
                return "raw_material" if downtime_indicators.get("raw_material") else "packaging"
            elif reason_code in [DowntimeReasonCode.MECHANICAL_FAULT, DowntimeReasonCode.ELEC_FAULT]:
                fault_categories = downtime_indicators.get("fault_analysis", {}).get("fault_categories", {})
                if "mechanical" in fault_categories:
                    return "mechanical"
                elif "electrical" in fault_categories:
                    return "electrical"
                else:
                    return "general"
            else:
                return None
                
        except Exception as e:
            logger.error("Failed to get downtime subcategory", error=str(e))
            return None
    
    async def _trigger_andon_for_downtime(self, downtime_event: Dict, downtime_indicators: Dict):
        """Trigger Andon event for downtime if it meets criteria."""
        try:
            if not self.andon_service:
                return
            
            # Check if downtime should trigger Andon event
            category = downtime_event.get("category", "")
            reason_code = downtime_event.get("reason_code", "")
            
            # Only trigger Andon for unplanned downtime
            if category != "unplanned":
                return
            
            # Check if reason meets Andon threshold
            if not self._should_trigger_andon(reason_code, downtime_indicators):
                return
            
            # Determine Andon event type and priority
            event_type, priority = self._classify_downtime_for_andon(reason_code, downtime_indicators)
            
            if event_type and priority:
                # Create Andon event
                andon_data = {
                    "line_id": downtime_event["line_id"],
                    "equipment_code": downtime_event["equipment_code"],
                    "event_type": event_type,
                    "priority": priority,
                    "description": f"PLC Downtime: {downtime_event.get('reason_description', 'Unknown')}",
                    "auto_generated": True,
                    "related_downtime_event_id": downtime_event.get("id")
                }
                
                logger.info(
                    "Andon event triggered for PLC downtime",
                    equipment_code=downtime_event["equipment_code"],
                    line_id=downtime_event["line_id"],
                    event_type=event_type,
                    priority=priority,
                    downtime_event_id=downtime_event.get("id")
                )
                
        except Exception as e:
            logger.error("Failed to trigger Andon for downtime", error=str(e))
    
    async def _check_andon_escalation(self, downtime_event: Dict, downtime_indicators: Dict):
        """Check if downtime event should trigger Andon escalation."""
        try:
            # Check if downtime has been active for too long
            start_time = downtime_event.get("start_time")
            if not start_time:
                return
            
            duration_minutes = (datetime.utcnow() - start_time).total_seconds() / 60
            
            # Check escalation thresholds
            reason_code = downtime_event.get("reason_code", "")
            escalation_threshold = self.andon_thresholds.get(reason_code, {}).get("escalation_minutes", 30)
            
            if duration_minutes >= escalation_threshold:
                # Trigger escalation
                logger.warning(
                    "Downtime event escalated due to duration",
                    equipment_code=downtime_event["equipment_code"],
                    duration_minutes=duration_minutes,
                    escalation_threshold=escalation_threshold,
                    reason_code=reason_code
                )
                
        except Exception as e:
            logger.error("Failed to check Andon escalation", error=str(e))
    
    def _should_trigger_andon(self, reason_code: str, downtime_indicators: Dict) -> bool:
        """Determine if downtime should trigger Andon event."""
        try:
            # Check Andon configuration for reason code
            andon_config = self.andon_thresholds.get(reason_code, {})
            
            # Check if Andon is enabled for this reason
            if not andon_config.get("trigger_andon", True):
                return False
            
            # Check if fault severity meets threshold
            fault_analysis = downtime_indicators.get("fault_analysis", {})
            critical_faults = fault_analysis.get("critical_faults", [])
            
            if critical_faults:
                return True  # Always trigger for critical faults
            
            # Check fault count threshold
            fault_count = fault_analysis.get("fault_count", 0)
            fault_threshold = andon_config.get("fault_threshold", 1)
            
            return fault_count >= fault_threshold
            
        except Exception as e:
            logger.error("Failed to determine Andon trigger", error=str(e))
            return False
    
    def _classify_downtime_for_andon(
        self, 
        reason_code: str, 
        downtime_indicators: Dict
    ) -> Tuple[str, str]:
        """Classify downtime for Andon event creation."""
        try:
            # Map reason codes to Andon event types and priorities
            andon_mapping = {
                "MECH_FAULT": ("maintenance", "high"),
                "ELEC_FAULT": ("maintenance", "high"),
                "BEARING_FAIL": ("maintenance", "high"),
                "BELT_BREAK": ("maintenance", "medium"),
                "MOTOR_FAIL": ("maintenance", "high"),
                "SENSOR_FAIL": ("maintenance", "medium"),
                "PLC_FAULT": ("maintenance", "critical"),
                "POWER_LOSS": ("maintenance", "critical"),
                "MAT_SHORTAGE": ("material", "medium"),
                "MAT_JAM": ("material", "medium"),
                "UPSTREAM_STOP": ("upstream", "low"),
                "DOWNSTREAM_STOP": ("downstream", "low"),
                "QUALITY_ISSUE": ("quality", "medium"),
                "UNKNOWN": ("maintenance", "medium")
            }
            
            # Get base classification
            event_type, priority = andon_mapping.get(reason_code, ("maintenance", "medium"))
            
            # Adjust priority based on fault severity
            fault_analysis = downtime_indicators.get("fault_analysis", {})
            critical_faults = fault_analysis.get("critical_faults", [])
            
            if critical_faults:
                priority = "critical"
            elif len(fault_analysis.get("internal_faults", [])) > 2:
                priority = "high"
            
            return event_type, priority
            
        except Exception as e:
            logger.error("Failed to classify downtime for Andon", error=str(e))
            return "maintenance", "medium"
    
    def _get_fault_category(self, marker: str, severity: str) -> str:
        """Get fault category based on marker and severity."""
        if marker == "INTERNAL":
            if severity == "critical":
                return "critical_internal"
            else:
                return "internal"
        elif marker == "UPSTREAM":
            return "upstream"
        elif marker == "DOWNSTREAM":
            return "downstream"
        else:
            return "unknown"
    
    def _initialize_fault_mapping(self) -> Dict[str, str]:
        """Initialize fault-to-downtime mapping."""
        return {
            "emergency_stop": "MECH_FAULT",
            "safety_gate": "MECH_FAULT",
            "motor_overload": "ELEC_FAULT",
            "temperature_high": "MECH_FAULT",
            "pressure_low": "MECH_FAULT",
            "upstream_stop": "UPSTREAM_STOP",
            "downstream_stop": "DOWNSTREAM_STOP",
            "material_jam": "MAT_JAM",
            "sensor_fault": "ELEC_FAULT",
            "communication_error": "PLC_FAULT"
        }
    
    def _initialize_andon_thresholds(self) -> Dict[str, Dict]:
        """Initialize Andon thresholds for different downtime reasons."""
        return {
            "MECH_FAULT": {
                "trigger_andon": True,
                "fault_threshold": 1,
                "escalation_minutes": 15
            },
            "ELEC_FAULT": {
                "trigger_andon": True,
                "fault_threshold": 1,
                "escalation_minutes": 10
            },
            "PLC_FAULT": {
                "trigger_andon": True,
                "fault_threshold": 1,
                "escalation_minutes": 5
            },
            "MAT_SHORTAGE": {
                "trigger_andon": True,
                "fault_threshold": 1,
                "escalation_minutes": 30
            },
            "MAT_JAM": {
                "trigger_andon": True,
                "fault_threshold": 1,
                "escalation_minutes": 20
            },
            "UPSTREAM_STOP": {
                "trigger_andon": False,
                "fault_threshold": 1,
                "escalation_minutes": 60
            },
            "DOWNSTREAM_STOP": {
                "trigger_andon": False,
                "fault_threshold": 1,
                "escalation_minutes": 60
            },
            "UNKNOWN": {
                "trigger_andon": True,
                "fault_threshold": 1,
                "escalation_minutes": 30
            }
        }
    
    def get_plc_downtime_statistics(
        self, 
        line_id: UUID, 
        equipment_code: str, 
        start_date: date, 
        end_date: date
    ) -> Dict[str, Any]:
        """Get PLC-based downtime statistics."""
        try:
            # This would typically aggregate PLC downtime data
            # For now, return basic statistics
            return {
                "line_id": line_id,
                "equipment_code": equipment_code,
                "period": {
                    "start_date": start_date,
                    "end_date": end_date
                },
                "plc_downtime_events": 0,
                "auto_detected_events": 0,
                "fault_based_events": 0,
                "material_based_events": 0,
                "planned_events": 0,
                "andon_events_triggered": 0
            }
            
        except Exception as e:
            logger.error("Failed to get PLC downtime statistics", error=str(e))
            return {}
    
    def configure_andon_settings(self, reason_code: str, settings: Dict):
        """Configure Andon settings for downtime reason."""
        try:
            self.andon_thresholds[reason_code] = settings
            logger.info("Andon settings configured", reason_code=reason_code, settings=settings)
            
        except Exception as e:
            logger.error("Failed to configure Andon settings", error=str(e))
    
    def enable_auto_andon(self, enabled: bool = True):
        """Enable or disable automatic Andon event creation."""
        self.auto_andon_enabled = enabled
        logger.info("Auto Andon enabled", enabled=enabled)
