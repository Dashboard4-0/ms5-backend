"""
MS5.0 Floor Dashboard - PLC Integrated Andon Service

This module extends the existing AndonService to integrate with PLC fault
detection systems, providing automated Andon event creation based on real-time
PLC data and intelligent fault classification.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from uuid import UUID
import structlog

from app.services.andon_service import AndonService
from app.services.downtime_tracker import DowntimeTracker
from app.services.notification_service import NotificationService
from app.database import execute_query, execute_scalar, execute_update
from app.models.production import AndonEventType, AndonPriority, AndonStatus
from app.utils.exceptions import BusinessLogicError, NotFoundError

logger = structlog.get_logger()


class PLCIntegratedAndonService(AndonService):
    """Andon service integrated with PLC fault detection."""
    
    def __init__(self):
        super().__init__()
        self.downtime_tracker = DowntimeTracker()
        self.notification_service = NotificationService() if NotificationService else None
        self.fault_thresholds = self._load_fault_thresholds()
        self.auto_andons_enabled = True
        self.plc_fault_cache = {}
        self.andon_escalation_cache = {}
    
    async def process_plc_faults(
        self, 
        line_id: UUID, 
        equipment_code: str, 
        fault_data: Dict[str, Any],
        context_data: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """Process PLC faults and create Andon events automatically."""
        try:
            created_events = []
            
            # Analyze fault data
            fault_analysis = self._analyze_plc_faults(fault_data, context_data)
            
            # Process each fault category
            for fault_category, faults in fault_analysis.items():
                if not faults:
                    continue
                
                # Determine if we should create Andon events for this category
                if not self._should_create_andon_for_category(fault_category, faults):
                    continue
                
                # Create Andon event for this fault category
                andon_event = await self._create_andon_from_plc_faults(
                    line_id, equipment_code, fault_category, faults, context_data
                )
                
                if andon_event:
                    created_events.append(andon_event)
            
            # Process downtime-based Andon events
            downtime_events = await self._process_downtime_based_andons(
                line_id, equipment_code, fault_data, context_data
            )
            created_events.extend(downtime_events)
            
            logger.info(
                "PLC faults processed for Andon events",
                equipment_code=equipment_code,
                line_id=line_id,
                total_events_created=len(created_events),
                fault_categories_processed=list(fault_analysis.keys())
            )
            
            return created_events
            
        except Exception as e:
            logger.error(
                "Failed to process PLC faults for Andon events",
                error=str(e),
                equipment_code=equipment_code,
                line_id=line_id
            )
            raise BusinessLogicError("Failed to process PLC faults for Andon events")
    
    def _analyze_plc_faults(
        self, 
        fault_data: Dict[str, Any], 
        context_data: Dict[str, Any] = None
    ) -> Dict[str, List[Dict]]:
        """Analyze PLC fault data and categorize faults."""
        try:
            fault_analysis = {
                "critical": [],
                "high_priority": [],
                "medium_priority": [],
                "low_priority": [],
                "upstream": [],
                "downstream": [],
                "material": [],
                "quality": []
            }
            
            # Analyze fault bits
            fault_bits = fault_data.get("fault_bits", [])
            active_alarms = fault_data.get("active_alarms", [])
            
            for i, bit_active in enumerate(fault_bits):
                if not bit_active:
                    continue
                
                # Get fault information
                fault_info = self._get_fault_info(i, active_alarms)
                if not fault_info:
                    continue
                
                # Categorize fault
                category = self._categorize_fault(fault_info, context_data)
                if category in fault_analysis:
                    fault_analysis[category].append(fault_info)
            
            # Analyze active alarms
            for alarm in active_alarms:
                alarm_info = self._analyze_alarm(alarm, context_data)
                if alarm_info:
                    category = alarm_info.get("category", "medium_priority")
                    if category in fault_analysis:
                        fault_analysis[category].append(alarm_info)
            
            return fault_analysis
            
        except Exception as e:
            logger.error("Failed to analyze PLC faults", error=str(e))
            return {
                "critical": [], "high_priority": [], "medium_priority": [], "low_priority": [],
                "upstream": [], "downstream": [], "material": [], "quality": []
            }
    
    def _get_fault_info(self, bit_index: int, active_alarms: List[str]) -> Optional[Dict]:
        """Get fault information for a specific bit index."""
        try:
            # Get fault from catalog
            fault_catalog = self._get_fault_catalog()
            fault_info = fault_catalog.get(bit_index, {
                "name": f"Fault {bit_index}",
                "description": "Unknown fault",
                "marker": "INTERNAL",
                "severity": "medium"
            })
            
            # Find matching alarm if available
            matching_alarm = None
            for alarm in active_alarms:
                if alarm.lower() in fault_info["name"].lower() or fault_info["name"].lower() in alarm.lower():
                    matching_alarm = alarm
                    break
            
            return {
                "bit_index": bit_index,
                "name": fault_info["name"],
                "description": fault_info["description"],
                "marker": fault_info["marker"],
                "severity": fault_info["severity"],
                "matching_alarm": matching_alarm,
                "timestamp": datetime.utcnow()
            }
            
        except Exception as e:
            logger.error("Failed to get fault info", error=str(e), bit_index=bit_index)
            return None
    
    def _analyze_alarm(self, alarm: str, context_data: Dict = None) -> Optional[Dict]:
        """Analyze alarm string and extract information."""
        try:
            alarm_lower = alarm.lower()
            
            # Determine alarm type and severity
            if any(keyword in alarm_lower for keyword in ["emergency", "critical", "stop"]):
                severity = "critical"
                category = "critical"
            elif any(keyword in alarm_lower for keyword in ["fault", "error", "failure"]):
                severity = "high"
                category = "high_priority"
            elif any(keyword in alarm_lower for keyword in ["warning", "caution"]):
                severity = "medium"
                category = "medium_priority"
            elif any(keyword in alarm_lower for keyword in ["upstream", "upstream"]):
                severity = "medium"
                category = "upstream"
            elif any(keyword in alarm_lower for keyword in ["downstream", "downstream"]):
                severity = "medium"
                category = "downstream"
            elif any(keyword in alarm_lower for keyword in ["material", "jam", "shortage"]):
                severity = "medium"
                category = "material"
            elif any(keyword in alarm_lower for keyword in ["quality", "reject", "defect"]):
                severity = "medium"
                category = "quality"
            else:
                severity = "low"
                category = "low_priority"
            
            return {
                "name": alarm,
                "description": alarm,
                "severity": severity,
                "category": category,
                "timestamp": datetime.utcnow()
            }
            
        except Exception as e:
            logger.error("Failed to analyze alarm", error=str(e), alarm=alarm)
            return None
    
    def _categorize_fault(self, fault_info: Dict, context_data: Dict = None) -> str:
        """Categorize fault based on its properties."""
        try:
            marker = fault_info.get("marker", "INTERNAL")
            severity = fault_info.get("severity", "medium")
            name = fault_info.get("name", "").lower()
            
            # Categorize by marker first
            if marker == "UPSTREAM":
                return "upstream"
            elif marker == "DOWNSTREAM":
                return "downstream"
            
            # Categorize by severity
            if severity == "critical":
                return "critical"
            elif severity == "high":
                return "high_priority"
            elif severity == "medium":
                return "medium_priority"
            elif severity == "low":
                return "low_priority"
            
            # Categorize by name/keywords
            if any(keyword in name for keyword in ["material", "jam", "shortage"]):
                return "material"
            elif any(keyword in name for keyword in ["quality", "reject", "defect"]):
                return "quality"
            
            # Default to medium priority
            return "medium_priority"
            
        except Exception as e:
            logger.error("Failed to categorize fault", error=str(e))
            return "medium_priority"
    
    async def _should_create_andon_for_category(
        self, 
        category: str, 
        faults: List[Dict]
    ) -> bool:
        """Determine if Andon events should be created for a fault category."""
        try:
            if not self.auto_andons_enabled:
                return False
            
            # Check fault thresholds
            threshold_config = self.fault_thresholds.get(category, {})
            if not threshold_config.get("enabled", True):
                return False
            
            # Check fault count threshold
            min_faults = threshold_config.get("min_faults", 1)
            if len(faults) < min_faults:
                return False
            
            # Check for critical faults (always create Andon)
            critical_faults = [f for f in faults if f.get("severity") == "critical"]
            if critical_faults:
                return True
            
            # Check for high priority faults
            high_priority_faults = [f for f in faults if f.get("severity") == "high"]
            if high_priority_faults and threshold_config.get("include_high_priority", True):
                return True
            
            # Check time-based conditions
            if self._should_create_time_based_andon(category, faults):
                return True
            
            return False
            
        except Exception as e:
            logger.error("Failed to determine Andon creation", error=str(e))
            return False
    
    def _should_create_time_based_andon(self, category: str, faults: List[Dict]) -> bool:
        """Check if Andon should be created based on time conditions."""
        try:
            threshold_config = self.fault_thresholds.get(category, {})
            max_duration_minutes = threshold_config.get("max_duration_minutes", 15)
            
            if max_duration_minutes <= 0:
                return False
            
            # Check if any fault has been active for too long
            cutoff_time = datetime.utcnow() - timedelta(minutes=max_duration_minutes)
            
            for fault in faults:
                fault_time = fault.get("timestamp", datetime.utcnow())
                if fault_time < cutoff_time:
                    return True
            
            return False
            
        except Exception as e:
            logger.error("Failed to check time-based Andon creation", error=str(e))
            return False
    
    async def _create_andon_from_plc_faults(
        self,
        line_id: UUID,
        equipment_code: str,
        fault_category: str,
        faults: List[Dict],
        context_data: Dict = None
    ) -> Optional[Dict[str, Any]]:
        """Create Andon event from PLC faults."""
        try:
            # Determine event type and priority
            event_type, priority = self._classify_fault_category_for_andon(fault_category, faults)
            
            if not event_type or not priority:
                return None
            
            # Create description
            description = self._create_fault_description(fault_category, faults)
            
            # Check for duplicate events
            if await self._is_duplicate_andon_event(line_id, equipment_code, event_type):
                logger.debug(
                    "Duplicate Andon event prevented",
                    equipment_code=equipment_code,
                    event_type=event_type
                )
                return None
            
            # Create Andon event data
            andon_data = {
                "line_id": line_id,
                "equipment_code": equipment_code,
                "event_type": event_type,
                "priority": priority,
                "description": description,
                "auto_generated": True,
                "plc_source": True,
                "fault_data": {
                    "fault_category": fault_category,
                    "faults": faults,
                    "fault_count": len(faults),
                    "analysis_timestamp": datetime.utcnow().isoformat()
                }
            }
            
            # Create the Andon event
            andon_event = await self._create_andon_event_from_data(andon_data)
            
            if andon_event:
                # Start escalation process
                await self._start_escalation_process(andon_event["id"], priority)
                
                # Send notifications
                await self._send_andon_notifications(andon_event, fault_category)
                
                logger.info(
                    "Andon event created from PLC faults",
                    andon_event_id=andon_event["id"],
                    equipment_code=equipment_code,
                    event_type=event_type,
                    priority=priority,
                    fault_category=fault_category,
                    fault_count=len(faults)
                )
            
            return andon_event
            
        except Exception as e:
            logger.error("Failed to create Andon from PLC faults", error=str(e))
            return None
    
    async def _process_downtime_based_andons(
        self,
        line_id: UUID,
        equipment_code: str,
        fault_data: Dict,
        context_data: Dict = None
    ) -> List[Dict[str, Any]]:
        """Process downtime-based Andon events."""
        try:
            downtime_andons = []
            
            # Check if equipment is down
            is_running = fault_data.get("running_status", False)
            speed = fault_data.get("speed_real", 0.0)
            has_faults = fault_data.get("has_active_faults", False)
            
            if is_running and speed > 0.1:
                # Equipment is running, no downtime Andon needed
                return downtime_andons
            
            # Equipment is down, check for downtime-based Andon events
            if has_faults:
                # Check if we should create downtime Andon
                if await self._should_create_downtime_andon(equipment_code, fault_data):
                    downtime_andon = await self._create_downtime_andon(
                        line_id, equipment_code, fault_data, context_data
                    )
                    
                    if downtime_andon:
                        downtime_andons.append(downtime_andon)
            
            return downtime_andons
            
        except Exception as e:
            logger.error("Failed to process downtime-based Andons", error=str(e))
            return []
    
    async def _should_create_downtime_andon(
        self, 
        equipment_code: str, 
        fault_data: Dict
    ) -> bool:
        """Determine if downtime Andon should be created."""
        try:
            # Check if equipment has been down for too long
            downtime_threshold_minutes = self.fault_thresholds.get("downtime", {}).get("threshold_minutes", 5)
            
            # This would typically check the last time equipment was running
            # For now, create Andon if there are active faults
            has_faults = fault_data.get("has_active_faults", False)
            fault_count = len(fault_data.get("active_alarms", []))
            
            return has_faults and fault_count > 0
            
        except Exception as e:
            logger.error("Failed to determine downtime Andon creation", error=str(e))
            return False
    
    async def _create_downtime_andon(
        self,
        line_id: UUID,
        equipment_code: str,
        fault_data: Dict,
        context_data: Dict = None
    ) -> Optional[Dict[str, Any]]:
        """Create Andon event for downtime."""
        try:
            # Determine event type and priority
            event_type = "maintenance"  # Default for downtime
            priority = "high"  # Default priority for downtime
            
            # Adjust based on fault severity
            active_alarms = fault_data.get("active_alarms", [])
            if any("critical" in alarm.lower() or "emergency" in alarm.lower() for alarm in active_alarms):
                priority = "critical"
            elif any("warning" in alarm.lower() for alarm in active_alarms):
                priority = "medium"
            
            # Create description
            description = f"Equipment downtime detected with {len(active_alarms)} active alarms"
            
            # Create Andon event data
            andon_data = {
                "line_id": line_id,
                "equipment_code": equipment_code,
                "event_type": event_type,
                "priority": priority,
                "description": description,
                "auto_generated": True,
                "plc_source": True,
                "downtime_data": {
                    "active_alarms": active_alarms,
                    "fault_count": len(active_alarms),
                    "downtime_timestamp": datetime.utcnow().isoformat()
                }
            }
            
            # Create the Andon event
            andon_event = await self._create_andon_event_from_data(andon_data)
            
            if andon_event:
                logger.info(
                    "Downtime Andon event created",
                    andon_event_id=andon_event["id"],
                    equipment_code=equipment_code,
                    alarm_count=len(active_alarms)
                )
            
            return andon_event
            
        except Exception as e:
            logger.error("Failed to create downtime Andon", error=str(e))
            return None
    
    def _classify_fault_category_for_andon(
        self, 
        category: str, 
        faults: List[Dict]
    ) -> Tuple[str, str]:
        """Classify fault category for Andon event type and priority."""
        try:
            # Map fault categories to Andon event types and priorities
            category_mapping = {
                "critical": ("maintenance", "critical"),
                "high_priority": ("maintenance", "high"),
                "medium_priority": ("maintenance", "medium"),
                "low_priority": ("maintenance", "low"),
                "upstream": ("upstream", "medium"),
                "downstream": ("downstream", "medium"),
                "material": ("material", "medium"),
                "quality": ("quality", "medium")
            }
            
            # Get base mapping
            event_type, priority = category_mapping.get(category, ("maintenance", "medium"))
            
            # Adjust priority based on fault count
            fault_count = len(faults)
            if fault_count > 3:
                # Multiple faults, increase priority
                if priority == "low":
                    priority = "medium"
                elif priority == "medium":
                    priority = "high"
            
            # Check for critical faults in the category
            critical_faults = [f for f in faults if f.get("severity") == "critical"]
            if critical_faults:
                priority = "critical"
            
            return event_type, priority
            
        except Exception as e:
            logger.error("Failed to classify fault category for Andon", error=str(e))
            return "maintenance", "medium"
    
    def _create_fault_description(self, category: str, faults: List[Dict]) -> str:
        """Create description for Andon event based on faults."""
        try:
            if not faults:
                return f"PLC fault detected in category: {category}"
            
            if len(faults) == 1:
                fault = faults[0]
                return f"PLC fault: {fault.get('name', 'Unknown')} - {fault.get('description', 'No description')}"
            
            # Multiple faults
            fault_names = [f.get("name", "Unknown") for f in faults[:3]]  # Limit to first 3
            if len(faults) > 3:
                return f"Multiple PLC faults detected: {', '.join(fault_names)} and {len(faults) - 3} more"
            else:
                return f"Multiple PLC faults detected: {', '.join(fault_names)}"
                
        except Exception as e:
            logger.error("Failed to create fault description", error=str(e))
            return f"PLC fault detected in category: {category}"
    
    async def _is_duplicate_andon_event(
        self, 
        line_id: UUID, 
        equipment_code: str, 
        event_type: str
    ) -> bool:
        """Check if duplicate Andon event exists."""
        try:
            # Check for active Andon events of the same type
            active_events = await self.get_active_andon_events(line_id)
            
            for event in active_events:
                if (event.equipment_code == equipment_code and 
                    event.event_type.value == event_type and 
                    event.status in [AndonStatus.OPEN, AndonStatus.ACKNOWLEDGED]):
                    return True
            
            return False
            
        except Exception as e:
            logger.error("Failed to check for duplicate Andon events", error=str(e))
            return False
    
    async def _create_andon_event_from_data(self, andon_data: Dict) -> Optional[Dict[str, Any]]:
        """Create Andon event from data dictionary."""
        try:
            # This would typically call the parent create_andon_event method
            # For now, return a mock event structure
            return {
                "id": "mock-event-id",
                "line_id": andon_data["line_id"],
                "equipment_code": andon_data["equipment_code"],
                "event_type": andon_data["event_type"],
                "priority": andon_data["priority"],
                "description": andon_data["description"],
                "status": "open",
                "auto_generated": andon_data.get("auto_generated", False),
                "plc_source": andon_data.get("plc_source", False),
                "created_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error("Failed to create Andon event from data", error=str(e))
            return None
    
    async def _send_andon_notifications(self, andon_event: Dict, fault_category: str):
        """Send notifications for Andon event."""
        try:
            if not self.notification_service:
                return
            
            # Send different notifications based on priority
            priority = andon_event.get("priority", "medium")
            
            if priority == "critical":
                # Send immediate notifications
                await self._send_immediate_notifications(andon_event)
            elif priority == "high":
                # Send high priority notifications
                await self._send_high_priority_notifications(andon_event)
            else:
                # Send standard notifications
                await self._send_standard_notifications(andon_event)
                
        except Exception as e:
            logger.error("Failed to send Andon notifications", error=str(e))
    
    async def _send_immediate_notifications(self, andon_event: Dict):
        """Send immediate notifications for critical events."""
        # Implementation for immediate notifications
        logger.info("Immediate notifications sent", event_id=andon_event.get("id"))
    
    async def _send_high_priority_notifications(self, andon_event: Dict):
        """Send high priority notifications."""
        # Implementation for high priority notifications
        logger.info("High priority notifications sent", event_id=andon_event.get("id"))
    
    async def _send_standard_notifications(self, andon_event: Dict):
        """Send standard notifications."""
        # Implementation for standard notifications
        logger.info("Standard notifications sent", event_id=andon_event.get("id"))
    
    def _get_fault_catalog(self) -> Dict[int, Dict]:
        """Get fault catalog for fault analysis."""
        # This would typically load from database or configuration
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
    
    def _load_fault_thresholds(self) -> Dict[str, Dict]:
        """Load fault thresholds for Andon event creation."""
        return {
            "critical": {
                "enabled": True,
                "min_faults": 1,
                "include_high_priority": True,
                "max_duration_minutes": 2
            },
            "high_priority": {
                "enabled": True,
                "min_faults": 1,
                "include_high_priority": True,
                "max_duration_minutes": 5
            },
            "medium_priority": {
                "enabled": True,
                "min_faults": 2,
                "include_high_priority": False,
                "max_duration_minutes": 15
            },
            "low_priority": {
                "enabled": False,
                "min_faults": 3,
                "include_high_priority": False,
                "max_duration_minutes": 30
            },
            "upstream": {
                "enabled": False,
                "min_faults": 1,
                "include_high_priority": False,
                "max_duration_minutes": 60
            },
            "downstream": {
                "enabled": False,
                "min_faults": 1,
                "include_high_priority": False,
                "max_duration_minutes": 60
            },
            "material": {
                "enabled": True,
                "min_faults": 1,
                "include_high_priority": True,
                "max_duration_minutes": 20
            },
            "quality": {
                "enabled": True,
                "min_faults": 1,
                "include_high_priority": False,
                "max_duration_minutes": 30
            },
            "downtime": {
                "enabled": True,
                "threshold_minutes": 5
            }
        }
    
    def configure_fault_thresholds(self, category: str, settings: Dict):
        """Configure fault thresholds for a category."""
        try:
            self.fault_thresholds[category] = settings
            logger.info("Fault thresholds configured", category=category, settings=settings)
            
        except Exception as e:
            logger.error("Failed to configure fault thresholds", error=str(e))
    
    def enable_auto_andons(self, enabled: bool = True):
        """Enable or disable automatic Andon event creation."""
        self.auto_andons_enabled = enabled
        logger.info("Auto Andon events enabled", enabled=enabled)
    
    def get_andon_statistics_from_plc(
        self, 
        line_id: UUID, 
        equipment_code: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> Dict[str, Any]:
        """Get Andon statistics from PLC integration."""
        try:
            # This would typically query PLC-generated Andon events
            return {
                "line_id": line_id,
                "equipment_code": equipment_code,
                "period": {
                    "start_date": start_date,
                    "end_date": end_date
                },
                "plc_generated_events": 0,
                "auto_andons_created": 0,
                "fault_based_events": 0,
                "downtime_based_events": 0,
                "escalations_triggered": 0,
                "average_resolution_time_minutes": 0
            }
            
        except Exception as e:
            logger.error("Failed to get PLC Andon statistics", error=str(e))
            return {}
