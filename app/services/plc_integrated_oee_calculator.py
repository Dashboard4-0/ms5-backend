"""
MS5.0 Floor Dashboard - PLC Integrated OEE Calculator

This module extends the existing OEECalculator to integrate with PLC data streams,
providing real-time OEE calculations based on actual equipment metrics from
the PLC telemetry system.
"""

from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Any, Tuple
from uuid import UUID
import structlog

from app.services.oee_calculator import OEECalculator
from app.services.downtime_tracker import DowntimeTracker
from app.database import execute_query, execute_scalar, execute_update
from app.utils.exceptions import BusinessLogicError, NotFoundError

logger = structlog.get_logger()


class PLCIntegratedOEECalculator(OEECalculator):
    """OEE calculator integrated with PLC data streams."""
    
    def __init__(self):
        super().__init__()
        self.downtime_tracker = DowntimeTracker()
        self.plc_metrics_cache = {}
        self.cache_ttl = 60  # 1 minute cache for real-time data
    
    async def calculate_real_time_oee(
        self, 
        line_id: UUID, 
        equipment_code: str, 
        current_metrics: Dict,
        timestamp: datetime = None
    ) -> Dict[str, Any]:
        """Calculate real-time OEE using current PLC metrics."""
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        try:
            # Get current production context
            production_context = await self._get_production_context(line_id, equipment_code)
            
            # Calculate OEE components from PLC data
            availability = await self._calculate_availability_from_plc(current_metrics, production_context)
            performance = await self._calculate_performance_from_plc(current_metrics, production_context)
            quality = await self._calculate_quality_from_production(current_metrics, production_context)
            
            # Calculate overall OEE
            oee = availability * performance * quality
            
            # Get downtime statistics for the current period
            downtime_stats = await self._get_current_downtime_stats(line_id, equipment_code, timestamp)
            
            # Cache the result
            cache_key = f"{equipment_code}_{timestamp.strftime('%Y%m%d%H%M%S')}"
            self.plc_metrics_cache[cache_key] = {
                "oee": oee,
                "availability": availability,
                "performance": performance,
                "quality": quality,
                "timestamp": timestamp,
                "downtime_stats": downtime_stats
            }
            
            return {
                "oee": round(oee, 4),
                "availability": round(availability, 4),
                "performance": round(performance, 4),
                "quality": round(quality, 4),
                "timestamp": timestamp,
                "equipment_code": equipment_code,
                "line_id": line_id,
                "production_context": production_context,
                "downtime_stats": downtime_stats,
                "is_currently_down": current_metrics.get("is_currently_down", False),
                "current_downtime_duration_seconds": current_metrics.get("current_downtime_duration", 0),
                "calculation_method": "plc_integrated"
            }
            
        except Exception as e:
            logger.error(
                "Failed to calculate real-time OEE from PLC data",
                error=str(e),
                line_id=line_id,
                equipment_code=equipment_code
            )
            raise BusinessLogicError("Failed to calculate real-time OEE from PLC data")
    
    async def calculate_plc_based_oee(
        self,
        line_id: UUID,
        equipment_code: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """Calculate OEE for a period using PLC historical data."""
        try:
            # Get PLC metrics for the period
            plc_metrics = await self._get_plc_metrics_period(equipment_code, start_time, end_time)
            
            if not plc_metrics:
                return self._get_empty_oee_result(line_id, equipment_code, start_time, end_time)
            
            # Calculate period-based OEE components
            availability = await self._calculate_period_availability(plc_metrics)
            performance = await self._calculate_period_performance(plc_metrics)
            quality = await self._calculate_period_quality(plc_metrics)
            
            # Calculate overall OEE
            oee = availability * performance * quality
            
            # Get downtime events for the period
            downtime_events = await self.downtime_tracker.get_downtime_events(
                line_id=line_id,
                equipment_code=equipment_code,
                start_date=start_time.date(),
                end_date=end_time.date(),
                limit=1000
            )
            
            # Calculate downtime impact
            downtime_impact = await self._calculate_downtime_impact(downtime_events, start_time, end_time)
            
            return {
                "oee": round(oee, 4),
                "availability": round(availability, 4),
                "performance": round(performance, 4),
                "quality": round(quality, 4),
                "timestamp": end_time,
                "equipment_code": equipment_code,
                "line_id": line_id,
                "period": {
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration_hours": (end_time - start_time).total_seconds() / 3600
                },
                "plc_metrics_summary": plc_metrics,
                "downtime_events": len(downtime_events),
                "downtime_impact": downtime_impact,
                "calculation_method": "plc_period_based"
            }
            
        except Exception as e:
            logger.error(
                "Failed to calculate PLC-based OEE for period",
                error=str(e),
                line_id=line_id,
                equipment_code=equipment_code
            )
            raise BusinessLogicError("Failed to calculate PLC-based OEE for period")
    
    async def get_oee_trends_from_plc(
        self,
        line_id: UUID,
        equipment_code: str,
        hours: int = 24
    ) -> Dict[str, Any]:
        """Get OEE trends from PLC data over specified hours."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=hours)
            
            # Get hourly OEE calculations
            hourly_oee = []
            for i in range(hours):
                hour_start = start_time + timedelta(hours=i)
                hour_end = hour_start + timedelta(hours=1)
                
                try:
                    hour_oee = await self.calculate_plc_based_oee(
                        line_id, equipment_code, hour_start, hour_end
                    )
                    
                    hourly_oee.append({
                        "hour": hour_start.hour,
                        "timestamp": hour_start,
                        "oee": hour_oee.get("oee", 0),
                        "availability": hour_oee.get("availability", 0),
                        "performance": hour_oee.get("performance", 0),
                        "quality": hour_oee.get("quality", 0)
                    })
                except Exception as e:
                    logger.warning(
                        "Failed to calculate hourly OEE",
                        error=str(e),
                        hour=hour_start.hour,
                        equipment_code=equipment_code
                    )
                    hourly_oee.append({
                        "hour": hour_start.hour,
                        "timestamp": hour_start,
                        "oee": 0,
                        "availability": 0,
                        "performance": 0,
                        "quality": 0
                    })
            
            # Calculate trends
            if hourly_oee:
                oee_values = [h["oee"] for h in hourly_oee if h["oee"] > 0]
                availability_values = [h["availability"] for h in hourly_oee if h["availability"] > 0]
                performance_values = [h["performance"] for h in hourly_oee if h["performance"] > 0]
                quality_values = [h["quality"] for h in hourly_oee if h["quality"] > 0]
                
                trends = {
                    "oee": {
                        "current": hourly_oee[-1]["oee"] if hourly_oee else 0,
                        "average": sum(oee_values) / len(oee_values) if oee_values else 0,
                        "min": min(oee_values) if oee_values else 0,
                        "max": max(oee_values) if oee_values else 0,
                        "trend": "up" if len(oee_values) > 1 and oee_values[-1] > oee_values[0] else "down"
                    },
                    "availability": {
                        "current": hourly_oee[-1]["availability"] if hourly_oee else 0,
                        "average": sum(availability_values) / len(availability_values) if availability_values else 0
                    },
                    "performance": {
                        "current": hourly_oee[-1]["performance"] if hourly_oee else 0,
                        "average": sum(performance_values) / len(performance_values) if performance_values else 0
                    },
                    "quality": {
                        "current": hourly_oee[-1]["quality"] if hourly_oee else 0,
                        "average": sum(quality_values) / len(quality_values) if quality_values else 0
                    }
                }
            else:
                trends = {
                    "oee": {"current": 0, "average": 0, "min": 0, "max": 0, "trend": "stable"},
                    "availability": {"current": 0, "average": 0},
                    "performance": {"current": 0, "average": 0},
                    "quality": {"current": 0, "average": 0}
                }
            
            return {
                "line_id": line_id,
                "equipment_code": equipment_code,
                "period_hours": hours,
                "start_time": start_time,
                "end_time": end_time,
                "hourly_oee": hourly_oee,
                "trends": trends,
                "data_points": len(hourly_oee),
                "calculation_method": "plc_trend_analysis"
            }
            
        except Exception as e:
            logger.error(
                "Failed to get OEE trends from PLC data",
                error=str(e),
                line_id=line_id,
                equipment_code=equipment_code
            )
            raise BusinessLogicError("Failed to get OEE trends from PLC data")
    
    async def _get_production_context(self, line_id: UUID, equipment_code: str) -> Dict[str, Any]:
        """Get current production context for OEE calculation."""
        try:
            context_query = """
            SELECT 
                c.production_line_id,
                c.current_job_id,
                c.production_schedule_id,
                c.target_quantity,
                c.actual_quantity,
                c.target_speed,
                c.current_product_type_id,
                c.shift_id,
                c.production_efficiency,
                c.quality_rate,
                c.changeover_status,
                c.current_operator,
                c.current_shift,
                ec.oee_targets,
                ec.target_speed as equipment_target_speed
            FROM factory_telemetry.context c
            LEFT JOIN factory_telemetry.equipment_config ec ON c.equipment_code = ec.equipment_code
            WHERE c.equipment_code = :equipment_code
            """
            
            result = await execute_query(context_query, {"equipment_code": equipment_code})
            
            if result:
                return result[0]
            
            return {}
            
        except Exception as e:
            logger.error("Failed to get production context", error=str(e), equipment_code=equipment_code)
            return {}
    
    async def _calculate_availability_from_plc(
        self, 
        current_metrics: Dict, 
        production_context: Dict
    ) -> float:
        """Calculate availability from PLC running status."""
        try:
            # Get running status from PLC
            is_running = current_metrics.get("running_status", False)
            has_faults = current_metrics.get("internal_fault", False) or current_metrics.get("upstream_fault", False) or current_metrics.get("downstream_fault", False)
            planned_stop = current_metrics.get("planned_stop", False)
            
            # Calculate availability based on PLC status
            if planned_stop:
                # Equipment is in planned stop, availability is 1.0 (planned downtime)
                return 1.0
            elif has_faults:
                # Equipment has faults, availability is 0.0 (unplanned downtime)
                return 0.0
            elif is_running:
                # Equipment is running, availability is 1.0
                return 1.0
            else:
                # Equipment is stopped but no faults, check if it's planned
                return 0.0 if not planned_stop else 1.0
                
        except Exception as e:
            logger.error("Failed to calculate availability from PLC", error=str(e))
            return 0.0
    
    async def _calculate_performance_from_plc(
        self, 
        current_metrics: Dict, 
        production_context: Dict
    ) -> float:
        """Calculate performance from PLC speed metrics."""
        try:
            # Get speed metrics from PLC
            actual_speed = current_metrics.get("speed_real", 0.0)
            
            # Get target speed from production context or equipment config
            target_speed = production_context.get("target_speed", 0.0)
            if target_speed <= 0:
                target_speed = production_context.get("equipment_target_speed", 0.0)
            
            if target_speed <= 0:
                # No target speed defined, cannot calculate performance
                return 0.0
            
            # Calculate performance as ratio of actual to target speed
            performance = min(1.0, actual_speed / target_speed)
            
            return round(performance, 4)
            
        except Exception as e:
            logger.error("Failed to calculate performance from PLC", error=str(e))
            return 0.0
    
    async def _calculate_quality_from_production(
        self, 
        current_metrics: Dict, 
        production_context: Dict
    ) -> float:
        """Calculate quality from production data."""
        try:
            # Get quality metrics from production context
            quality_rate = production_context.get("quality_rate", 0.0)
            
            if quality_rate > 0:
                # Use production context quality rate
                return round(quality_rate / 100.0, 4)  # Convert percentage to decimal
            
            # Fallback: check for quality indicators in PLC metrics
            has_quality_issues = current_metrics.get("quality_issue", False)
            if has_quality_issues:
                return 0.8  # Assume 80% quality if issues detected
            
            # Default to 95% quality if no data available
            return 0.95
            
        except Exception as e:
            logger.error("Failed to calculate quality from production", error=str(e))
            return 0.95  # Default quality rate
    
    async def _get_current_downtime_stats(
        self, 
        line_id: UUID, 
        equipment_code: str, 
        timestamp: datetime
    ) -> Dict[str, Any]:
        """Get current downtime statistics."""
        try:
            # Get downtime events for today
            today = timestamp.date()
            downtime_events = await self.downtime_tracker.get_downtime_events(
                line_id=line_id,
                equipment_code=equipment_code,
                start_date=today,
                end_date=today,
                limit=100
            )
            
            # Calculate statistics
            total_downtime_minutes = sum(
                event.duration_seconds / 60 for event in downtime_events 
                if event.duration_seconds
            )
            
            active_events = [event for event in downtime_events if event.status == "open"]
            
            return {
                "total_events_today": len(downtime_events),
                "active_events": len(active_events),
                "total_downtime_minutes": round(total_downtime_minutes, 2),
                "current_downtime_reason": active_events[0].reason_description if active_events else None,
                "current_downtime_category": active_events[0].category if active_events else None
            }
            
        except Exception as e:
            logger.error("Failed to get current downtime stats", error=str(e))
            return {
                "total_events_today": 0,
                "active_events": 0,
                "total_downtime_minutes": 0,
                "current_downtime_reason": None,
                "current_downtime_category": None
            }
    
    async def _get_plc_metrics_period(
        self, 
        equipment_code: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> Dict[str, Any]:
        """Get PLC metrics for a specific period."""
        try:
            # Get metrics from latest table for the period
            metrics_query = """
            SELECT 
                ml.timestamp,
                ml.value,
                md.metric_key
            FROM factory_telemetry.metric_latest ml
            JOIN factory_telemetry.metric_def md ON ml.metric_def_id = md.id
            WHERE md.equipment_code = :equipment_code
            AND ml.timestamp >= :start_time
            AND ml.timestamp <= :end_time
            AND md.metric_key IN ('speed_real', 'running_status', 'internal_fault', 'upstream_fault', 'downstream_fault', 'planned_stop')
            ORDER BY ml.timestamp DESC
            """
            
            result = await execute_query(metrics_query, {
                "equipment_code": equipment_code,
                "start_time": start_time,
                "end_time": end_time
            })
            
            # Process metrics into structured format
            metrics_summary = {
                "total_records": len(result),
                "avg_speed": 0.0,
                "running_percentage": 0.0,
                "fault_percentage": 0.0,
                "planned_stop_percentage": 0.0,
                "data_points": {}
            }
            
            if result:
                # Group metrics by type
                speed_values = []
                running_count = 0
                fault_count = 0
                planned_stop_count = 0
                total_count = 0
                
                for row in result:
                    metric_key = row["metric_key"]
                    value = row["value"]
                    
                    if metric_key == "speed_real":
                        try:
                            speed_values.append(float(value))
                        except (ValueError, TypeError):
                            pass
                    elif metric_key == "running_status":
                        total_count += 1
                        if value:
                            running_count += 1
                    elif metric_key in ["internal_fault", "upstream_fault", "downstream_fault"]:
                        if value:
                            fault_count += 1
                    elif metric_key == "planned_stop":
                        if value:
                            planned_stop_count += 1
                
                # Calculate averages
                if speed_values:
                    metrics_summary["avg_speed"] = round(sum(speed_values) / len(speed_values), 2)
                
                if total_count > 0:
                    metrics_summary["running_percentage"] = round((running_count / total_count) * 100, 2)
                    metrics_summary["fault_percentage"] = round((fault_count / total_count) * 100, 2)
                    metrics_summary["planned_stop_percentage"] = round((planned_stop_count / total_count) * 100, 2)
            
            return metrics_summary
            
        except Exception as e:
            logger.error("Failed to get PLC metrics for period", error=str(e))
            return {}
    
    async def _calculate_period_availability(self, plc_metrics: Dict) -> float:
        """Calculate availability for a period from PLC metrics."""
        try:
            running_percentage = plc_metrics.get("running_percentage", 0)
            fault_percentage = plc_metrics.get("fault_percentage", 0)
            planned_stop_percentage = plc_metrics.get("planned_stop_percentage", 0)
            
            # Availability = (Running time + Planned stop time) / Total time
            # Faults reduce availability
            availability = max(0.0, (running_percentage - fault_percentage) / 100.0)
            
            return round(availability, 4)
            
        except Exception as e:
            logger.error("Failed to calculate period availability", error=str(e))
            return 0.0
    
    async def _calculate_period_performance(self, plc_metrics: Dict) -> float:
        """Calculate performance for a period from PLC metrics."""
        try:
            avg_speed = plc_metrics.get("avg_speed", 0.0)
            
            # This would need target speed from equipment configuration
            # For now, assume a default target speed
            target_speed = 10.0  # This should come from equipment config
            
            if target_speed <= 0:
                return 0.0
            
            performance = min(1.0, avg_speed / target_speed)
            return round(performance, 4)
            
        except Exception as e:
            logger.error("Failed to calculate period performance", error=str(e))
            return 0.0
    
    async def _calculate_period_quality(self, plc_metrics: Dict) -> float:
        """Calculate quality for a period from PLC metrics."""
        try:
            # Quality calculation would typically require quality data
            # For now, return a default quality rate
            return 0.95  # 95% quality rate
            
        except Exception as e:
            logger.error("Failed to calculate period quality", error=str(e))
            return 0.95
    
    async def _calculate_downtime_impact(
        self, 
        downtime_events: List, 
        start_time: datetime, 
        end_time: datetime
    ) -> Dict[str, Any]:
        """Calculate downtime impact on OEE."""
        try:
            total_downtime_seconds = sum(
                event.duration_seconds for event in downtime_events 
                if event.duration_seconds
            )
            
            total_period_seconds = (end_time - start_time).total_seconds()
            
            downtime_percentage = (total_downtime_seconds / total_period_seconds) * 100 if total_period_seconds > 0 else 0
            
            # Categorize downtime
            unplanned_downtime = sum(
                event.duration_seconds for event in downtime_events 
                if event.category == "unplanned" and event.duration_seconds
            )
            
            planned_downtime = sum(
                event.duration_seconds for event in downtime_events 
                if event.category == "planned" and event.duration_seconds
            )
            
            return {
                "total_downtime_seconds": total_downtime_seconds,
                "total_downtime_minutes": round(total_downtime_seconds / 60, 2),
                "downtime_percentage": round(downtime_percentage, 2),
                "unplanned_downtime_seconds": unplanned_downtime,
                "unplanned_downtime_minutes": round(unplanned_downtime / 60, 2),
                "planned_downtime_seconds": planned_downtime,
                "planned_downtime_minutes": round(planned_downtime / 60, 2),
                "total_events": len(downtime_events)
            }
            
        except Exception as e:
            logger.error("Failed to calculate downtime impact", error=str(e))
            return {
                "total_downtime_seconds": 0,
                "total_downtime_minutes": 0,
                "downtime_percentage": 0,
                "unplanned_downtime_seconds": 0,
                "unplanned_downtime_minutes": 0,
                "planned_downtime_seconds": 0,
                "planned_downtime_minutes": 0,
                "total_events": 0
            }
    
    def _get_empty_oee_result(
        self, 
        line_id: UUID, 
        equipment_code: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> Dict[str, Any]:
        """Get empty OEE result when no data is available."""
        return {
            "oee": 0.0,
            "availability": 0.0,
            "performance": 0.0,
            "quality": 0.0,
            "timestamp": end_time,
            "equipment_code": equipment_code,
            "line_id": line_id,
            "period": {
                "start_time": start_time,
                "end_time": end_time,
                "duration_hours": (end_time - start_time).total_seconds() / 3600
            },
            "plc_metrics_summary": {},
            "downtime_events": 0,
            "downtime_impact": {
                "total_downtime_seconds": 0,
                "total_downtime_minutes": 0,
                "downtime_percentage": 0,
                "unplanned_downtime_seconds": 0,
                "unplanned_downtime_minutes": 0,
                "planned_downtime_seconds": 0,
                "planned_downtime_minutes": 0,
                "total_events": 0
            },
            "calculation_method": "plc_period_based_no_data"
        }
    
    def get_cached_oee_data(self, equipment_code: str, timestamp: datetime = None) -> Optional[Dict]:
        """Get cached OEE data for equipment."""
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        cache_key = f"{equipment_code}_{timestamp.strftime('%Y%m%d%H%M%S')}"
        return self.plc_metrics_cache.get(cache_key)
    
    def clear_cache(self, equipment_code: str = None):
        """Clear OEE calculation cache."""
        if equipment_code:
            keys_to_remove = [key for key in self.plc_metrics_cache.keys() if key.startswith(equipment_code)]
            for key in keys_to_remove:
                del self.plc_metrics_cache[key]
        else:
            self.plc_metrics_cache.clear()
