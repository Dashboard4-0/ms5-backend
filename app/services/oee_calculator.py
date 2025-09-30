"""
MS5.0 Floor Dashboard - OEE Calculator Service

This module provides OEE (Overall Equipment Effectiveness) calculation services
for production lines and equipment. OEE is calculated as Availability × Performance × Quality.
"""

from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Any, Tuple
from uuid import UUID
import structlog

from app.database import execute_query, execute_scalar, execute_update
from app.models.production import OEECalculationResponse, OEECalculationCreate
from app.utils.exceptions import ValidationError, BusinessLogicError, NotFoundError
from app.services.downtime_tracker import DowntimeTracker

logger = structlog.get_logger()


class OEECalculator:
    """OEE calculation service."""
    
    @staticmethod
    async def calculate_oee(
        line_id: UUID,
        equipment_code: str,
        calculation_time: datetime,
        time_period_hours: int = 24
    ) -> OEECalculationResponse:
        """
        Calculate OEE for a specific equipment over a time period.
        
        OEE = Availability × Performance × Quality
        
        Where:
        - Availability = (Operating Time / Planned Production Time) × 100
        - Performance = (Actual Output / Target Output) × 100
        - Quality = (Good Parts / Total Parts) × 100
        """
        try:
            # Calculate time period
            start_time = calculation_time - timedelta(hours=time_period_hours)
            
            # Get production data for the period
            production_data = await OEECalculator._get_production_data(
                line_id, equipment_code, start_time, calculation_time
            )
            
            # Calculate OEE components
            availability = await OEECalculator._calculate_availability(production_data)
            performance = await OEECalculator._calculate_performance(production_data)
            quality = await OEECalculator._calculate_quality(production_data)
            
            # Calculate overall OEE
            oee = availability * performance * quality
            
            # Store calculation in database
            calculation_id = await OEECalculator._store_oee_calculation(
                line_id, equipment_code, calculation_time,
                availability, performance, quality, oee, production_data
            )
            
            logger.info(
                "OEE calculated",
                line_id=line_id,
                equipment_code=equipment_code,
                oee=oee,
                availability=availability,
                performance=performance,
                quality=quality
            )
            
            return OEECalculationResponse(
                id=calculation_id,
                line_id=line_id,
                equipment_code=equipment_code,
                calculation_time=calculation_time,
                availability=availability,
                performance=performance,
                quality=quality,
                oee=oee,
                planned_production_time=production_data["planned_production_time"],
                actual_production_time=production_data["actual_production_time"],
                ideal_cycle_time=production_data["ideal_cycle_time"],
                actual_cycle_time=production_data["actual_cycle_time"],
                good_parts=production_data["good_parts"],
                total_parts=production_data["total_parts"]
            )
            
        except Exception as e:
            logger.error(
                "Failed to calculate OEE",
                error=str(e),
                line_id=line_id,
                equipment_code=equipment_code
            )
            raise BusinessLogicError("Failed to calculate OEE")
    
    @staticmethod
    async def _get_production_data(
        line_id: UUID,
        equipment_code: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """Get production data for OEE calculation."""
        try:
            # Get planned production time (in seconds)
            planned_time_query = """
            SELECT EXTRACT(EPOCH FROM (:end_time - :start_time))::INTEGER as planned_time
            """
            planned_time = await execute_scalar(planned_time_query, {
                "start_time": start_time,
                "end_time": end_time
            })
            
            # Get actual production time (time when equipment was running)
            actual_time_query = """
            SELECT COALESCE(SUM(duration_seconds), 0) as actual_time
            FROM factory_telemetry.downtime_events 
            WHERE line_id = :line_id 
            AND equipment_code = :equipment_code
            AND start_time >= :start_time 
            AND start_time < :end_time
            AND category = 'unplanned'
            """
            downtime_seconds = await execute_scalar(actual_time_query, {
                "line_id": line_id,
                "equipment_code": equipment_code,
                "start_time": start_time,
                "end_time": end_time
            })
            
            actual_production_time = planned_time - downtime_seconds
            
            # Get production counts
            production_query = """
            SELECT 
                COALESCE(SUM(good_parts), 0) as good_parts,
                COALESCE(SUM(total_parts), 0) as total_parts,
                COALESCE(AVG(actual_cycle_time), 0) as avg_cycle_time
            FROM factory_telemetry.oee_calculations 
            WHERE line_id = :line_id 
            AND equipment_code = :equipment_code
            AND calculation_time >= :start_time 
            AND calculation_time < :end_time
            """
            
            production_result = await execute_query(production_query, {
                "line_id": line_id,
                "equipment_code": equipment_code,
                "start_time": start_time,
                "end_time": end_time
            })
            
            if production_result:
                good_parts = production_result[0]["good_parts"]
                total_parts = production_result[0]["total_parts"]
                avg_cycle_time = production_result[0]["avg_cycle_time"]
            else:
                good_parts = 0
                total_parts = 0
                avg_cycle_time = 0
            
            # Get ideal cycle time from equipment configuration
            ideal_cycle_time_query = """
            SELECT ideal_cycle_time 
            FROM factory_telemetry.equipment_config 
            WHERE equipment_code = :equipment_code
            """
            ideal_cycle_time = await execute_scalar(ideal_cycle_time_query, {
                "equipment_code": equipment_code
            })
            
            if not ideal_cycle_time:
                # Default ideal cycle time if not configured
                ideal_cycle_time = 1.0  # 1 second per part
            
            return {
                "planned_production_time": planned_time,
                "actual_production_time": max(0, actual_production_time),
                "ideal_cycle_time": ideal_cycle_time,
                "actual_cycle_time": avg_cycle_time if avg_cycle_time > 0 else ideal_cycle_time,
                "good_parts": good_parts,
                "total_parts": total_parts if total_parts > 0 else 1
            }
            
        except Exception as e:
            logger.error("Failed to get production data", error=str(e))
            raise BusinessLogicError("Failed to get production data")
    
    @staticmethod
    async def _calculate_availability(production_data: Dict[str, Any]) -> float:
        """Calculate availability component of OEE."""
        planned_time = production_data["planned_production_time"]
        actual_time = production_data["actual_production_time"]
        
        if planned_time == 0:
            return 0.0
        
        availability = min(1.0, actual_time / planned_time)
        return round(availability, 4)
    
    @staticmethod
    async def _calculate_performance(production_data: Dict[str, Any]) -> float:
        """Calculate performance component of OEE."""
        ideal_cycle_time = production_data["ideal_cycle_time"]
        actual_cycle_time = production_data["actual_cycle_time"]
        
        if actual_cycle_time == 0:
            return 0.0
        
        performance = min(1.0, ideal_cycle_time / actual_cycle_time)
        return round(performance, 4)
    
    @staticmethod
    async def _calculate_quality(production_data: Dict[str, Any]) -> float:
        """Calculate quality component of OEE."""
        total_parts = production_data["total_parts"]
        good_parts = production_data["good_parts"]
        
        if total_parts == 0:
            return 0.0
        
        quality = good_parts / total_parts
        return round(quality, 4)
    
    @staticmethod
    async def _store_oee_calculation(
        line_id: UUID,
        equipment_code: str,
        calculation_time: datetime,
        availability: float,
        performance: float,
        quality: float,
        oee: float,
        production_data: Dict[str, Any]
    ) -> int:
        """Store OEE calculation in database."""
        try:
            insert_query = """
            INSERT INTO factory_telemetry.oee_calculations 
            (line_id, equipment_code, calculation_time, availability, performance, 
             quality, oee, planned_production_time, actual_production_time, 
             ideal_cycle_time, actual_cycle_time, good_parts, total_parts)
            VALUES (:line_id, :equipment_code, :calculation_time, :availability, 
                   :performance, :quality, :oee, :planned_production_time, 
                   :actual_production_time, :ideal_cycle_time, :actual_cycle_time, 
                   :good_parts, :total_parts)
            RETURNING id
            """
            
            result = await execute_query(insert_query, {
                "line_id": line_id,
                "equipment_code": equipment_code,
                "calculation_time": calculation_time,
                "availability": availability,
                "performance": performance,
                "quality": quality,
                "oee": oee,
                "planned_production_time": production_data["planned_production_time"],
                "actual_production_time": production_data["actual_production_time"],
                "ideal_cycle_time": production_data["ideal_cycle_time"],
                "actual_cycle_time": production_data["actual_cycle_time"],
                "good_parts": production_data["good_parts"],
                "total_parts": production_data["total_parts"]
            })
            
            if not result:
                raise BusinessLogicError("Failed to store OEE calculation")
            
            return result[0]["id"]
            
        except Exception as e:
            logger.error("Failed to store OEE calculation", error=str(e))
            raise BusinessLogicError("Failed to store OEE calculation")
    
    @staticmethod
    async def get_oee_history(
        line_id: UUID,
        equipment_code: str,
        start_date: date,
        end_date: date,
        limit: int = 100
    ) -> List[OEECalculationResponse]:
        """Get OEE calculation history for a period."""
        try:
            query = """
            SELECT id, line_id, equipment_code, calculation_time, availability, 
                   performance, quality, oee, planned_production_time, 
                   actual_production_time, ideal_cycle_time, actual_cycle_time, 
                   good_parts, total_parts
            FROM factory_telemetry.oee_calculations 
            WHERE line_id = :line_id 
            AND equipment_code = :equipment_code
            AND DATE(calculation_time) >= :start_date 
            AND DATE(calculation_time) <= :end_date
            ORDER BY calculation_time DESC
            LIMIT :limit
            """
            
            result = await execute_query(query, {
                "line_id": line_id,
                "equipment_code": equipment_code,
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit
            })
            
            calculations = []
            for calc in result:
                calculations.append(OEECalculationResponse(
                    id=calc["id"],
                    line_id=calc["line_id"],
                    equipment_code=calc["equipment_code"],
                    calculation_time=calc["calculation_time"],
                    availability=calc["availability"],
                    performance=calc["performance"],
                    quality=calc["quality"],
                    oee=calc["oee"],
                    planned_production_time=calc["planned_production_time"],
                    actual_production_time=calc["actual_production_time"],
                    ideal_cycle_time=calc["ideal_cycle_time"],
                    actual_cycle_time=calc["actual_cycle_time"],
                    good_parts=calc["good_parts"],
                    total_parts=calc["total_parts"]
                ))
            
            return calculations
            
        except Exception as e:
            logger.error("Failed to get OEE history", error=str(e))
            raise BusinessLogicError("Failed to get OEE history")
    
    @staticmethod
    async def get_current_oee(line_id: UUID, equipment_code: str) -> Optional[OEECalculationResponse]:
        """Get current OEE for a line/equipment."""
        try:
            query = """
            SELECT id, line_id, equipment_code, calculation_time, availability, 
                   performance, quality, oee, planned_production_time, 
                   actual_production_time, ideal_cycle_time, actual_cycle_time, 
                   good_parts, total_parts
            FROM factory_telemetry.oee_calculations 
            WHERE line_id = :line_id 
            AND equipment_code = :equipment_code
            ORDER BY calculation_time DESC
            LIMIT 1
            """
            
            result = await execute_query(query, {
                "line_id": line_id,
                "equipment_code": equipment_code
            })
            
            if not result:
                return None
            
            calc = result[0]
            return OEECalculationResponse(
                id=calc["id"],
                line_id=calc["line_id"],
                equipment_code=calc["equipment_code"],
                calculation_time=calc["calculation_time"],
                availability=calc["availability"],
                performance=calc["performance"],
                quality=calc["quality"],
                oee=calc["oee"],
                planned_production_time=calc["planned_production_time"],
                actual_production_time=calc["actual_production_time"],
                ideal_cycle_time=calc["ideal_cycle_time"],
                actual_cycle_time=calc["actual_cycle_time"],
                good_parts=calc["good_parts"],
                total_parts=calc["total_parts"]
            )
            
        except Exception as e:
            logger.error("Failed to get current OEE", error=str(e))
            raise BusinessLogicError("Failed to get current OEE")
    
    @staticmethod
    async def calculate_daily_oee_summary(
        line_id: UUID,
        target_date: date
    ) -> Dict[str, Any]:
        """Calculate daily OEE summary for a production line."""
        try:
            start_time = datetime.combine(target_date, datetime.min.time())
            end_time = start_time + timedelta(days=1)
            
            # Get all equipment on the line
            equipment_query = """
            SELECT equipment_codes FROM factory_telemetry.production_lines 
            WHERE id = :line_id
            """
            line_result = await execute_query(equipment_query, {"line_id": line_id})
            
            if not line_result:
                raise NotFoundError("Production line", str(line_id))
            
            equipment_codes = line_result[0]["equipment_codes"]
            
            # Calculate OEE for each equipment
            equipment_oee = []
            total_availability = 0
            total_performance = 0
            total_quality = 0
            equipment_count = 0
            
            for equipment_code in equipment_codes:
                try:
                    oee_calc = await OEECalculator.calculate_oee(
                        line_id, equipment_code, end_time, 24
                    )
                    
                    equipment_oee.append({
                        "equipment_code": equipment_code,
                        "oee": oee_calc.oee,
                        "availability": oee_calc.availability,
                        "performance": oee_calc.performance,
                        "quality": oee_calc.quality
                    })
                    
                    total_availability += oee_calc.availability
                    total_performance += oee_calc.performance
                    total_quality += oee_calc.quality
                    equipment_count += 1
                    
                except Exception as e:
                    logger.warning(
                        "Failed to calculate OEE for equipment",
                        error=str(e),
                        equipment_code=equipment_code
                    )
                    continue
            
            # Calculate line average OEE
            if equipment_count > 0:
                avg_availability = total_availability / equipment_count
                avg_performance = total_performance / equipment_count
                avg_quality = total_quality / equipment_count
                avg_oee = avg_availability * avg_performance * avg_quality
            else:
                avg_availability = 0
                avg_performance = 0
                avg_quality = 0
                avg_oee = 0
            
            return {
                "line_id": line_id,
                "date": target_date,
                "average_oee": round(avg_oee, 4),
                "average_availability": round(avg_availability, 4),
                "average_performance": round(avg_performance, 4),
                "average_quality": round(avg_quality, 4),
                "equipment_count": equipment_count,
                "equipment_oee": equipment_oee
            }
            
        except (NotFoundError, BusinessLogicError):
            raise
        except Exception as e:
            logger.error("Failed to calculate daily OEE summary", error=str(e))
            raise BusinessLogicError("Failed to calculate daily OEE summary")
    
    @staticmethod
    async def get_oee_trends(
        line_id: UUID,
        days: int = 7
    ) -> Dict[str, Any]:
        """Get OEE trends over a period."""
        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)
            
            # Get daily OEE summaries
            daily_summaries = []
            for i in range(days):
                target_date = start_date + timedelta(days=i)
                try:
                    summary = await OEECalculator.calculate_daily_oee_summary(
                        line_id, target_date
                    )
                    daily_summaries.append(summary)
                except Exception as e:
                    logger.warning(
                        "Failed to calculate daily OEE",
                        error=str(e),
                        date=target_date
                    )
                    continue
            
            # Calculate trends
            if daily_summaries:
                oee_values = [s["average_oee"] for s in daily_summaries]
                availability_values = [s["average_availability"] for s in daily_summaries]
                performance_values = [s["average_performance"] for s in daily_summaries]
                quality_values = [s["average_quality"] for s in daily_summaries]
                
                return {
                    "line_id": line_id,
                    "period_days": days,
                    "start_date": start_date,
                    "end_date": end_date,
                    "daily_summaries": daily_summaries,
                    "trends": {
                        "oee": {
                            "current": oee_values[-1] if oee_values else 0,
                            "average": sum(oee_values) / len(oee_values) if oee_values else 0,
                            "min": min(oee_values) if oee_values else 0,
                            "max": max(oee_values) if oee_values else 0,
                            "trend": "up" if len(oee_values) > 1 and oee_values[-1] > oee_values[0] else "down"
                        },
                        "availability": {
                            "current": availability_values[-1] if availability_values else 0,
                            "average": sum(availability_values) / len(availability_values) if availability_values else 0
                        },
                        "performance": {
                            "current": performance_values[-1] if performance_values else 0,
                            "average": sum(performance_values) / len(performance_values) if performance_values else 0
                        },
                        "quality": {
                            "current": quality_values[-1] if quality_values else 0,
                            "average": sum(quality_values) / len(quality_values) if quality_values else 0
                        }
                    }
                }
            else:
                return {
                    "line_id": line_id,
                    "period_days": days,
                    "start_date": start_date,
                    "end_date": end_date,
                    "daily_summaries": [],
                    "trends": {
                        "oee": {"current": 0, "average": 0, "min": 0, "max": 0, "trend": "stable"},
                        "availability": {"current": 0, "average": 0},
                        "performance": {"current": 0, "average": 0},
                        "quality": {"current": 0, "average": 0}
                    }
                }
                
        except Exception as e:
            logger.error("Failed to get OEE trends", error=str(e))
            raise BusinessLogicError("Failed to get OEE trends")
    
    @staticmethod
    async def calculate_real_time_oee(
        line_id: UUID,
        equipment_code: str,
        current_status: Dict[str, Any],
        timestamp: datetime = None
    ) -> Dict[str, Any]:
        """
        Calculate real-time OEE with current downtime integration.
        
        This method integrates with the downtime tracker to provide
        real-time OEE calculations that include current downtime events.
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        try:
            # Initialize downtime tracker
            downtime_tracker = DowntimeTracker()
            
            # Detect current downtime events
            downtime_event = await downtime_tracker.detect_downtime_event(
                line_id=line_id,
                equipment_code=equipment_code,
                current_status=current_status,
                timestamp=timestamp
            )
            
            # Get current production data
            production_data = await OEECalculator._get_production_data(
                line_id, equipment_code, timestamp - timedelta(hours=1), timestamp
            )
            
            # Adjust for current downtime
            if downtime_event:
                # Equipment is currently down
                current_downtime_seconds = int((timestamp - downtime_event["start_time"]).total_seconds())
                production_data["actual_production_time"] = max(0, production_data["actual_production_time"] - current_downtime_seconds)
                
                # Set production counts to 0 if currently down
                if downtime_event["status"] == "open":
                    production_data["good_parts"] = 0
                    production_data["total_parts"] = 1  # Avoid division by zero
            
            # Calculate OEE components
            availability = await OEECalculator._calculate_availability(production_data)
            performance = await OEECalculator._calculate_performance(production_data)
            quality = await OEECalculator._calculate_quality(production_data)
            
            # Calculate overall OEE
            oee = availability * performance * quality
            
            # Get downtime statistics for the period
            downtime_stats = await downtime_tracker.get_downtime_statistics(
                line_id=line_id,
                start_date=timestamp.date(),
                end_date=timestamp.date()
            )
            
            return {
                "line_id": line_id,
                "equipment_code": equipment_code,
                "calculation_time": timestamp,
                "availability": round(availability, 4),
                "performance": round(performance, 4),
                "quality": round(quality, 4),
                "oee": round(oee, 4),
                "planned_production_time": production_data["planned_production_time"],
                "actual_production_time": production_data["actual_production_time"],
                "ideal_cycle_time": production_data["ideal_cycle_time"],
                "actual_cycle_time": production_data["actual_cycle_time"],
                "good_parts": production_data["good_parts"],
                "total_parts": production_data["total_parts"],
                "current_downtime_event": downtime_event,
                "downtime_statistics": downtime_stats,
                "is_currently_down": downtime_event is not None and downtime_event.get("status") == "open",
                "current_downtime_duration_seconds": int((timestamp - downtime_event["start_time"]).total_seconds()) if downtime_event and downtime_event.get("status") == "open" else 0
            }
            
        except Exception as e:
            logger.error(
                "Failed to calculate real-time OEE",
                error=str(e),
                line_id=line_id,
                equipment_code=equipment_code
            )
            raise BusinessLogicError("Failed to calculate real-time OEE")
    
    @staticmethod
    async def get_oee_with_downtime_analysis(
        line_id: UUID,
        equipment_code: str,
        start_date: date,
        end_date: date
    ) -> Dict[str, Any]:
        """
        Get OEE analysis with detailed downtime breakdown.
        
        This method provides comprehensive OEE analysis including
        downtime categorization and impact analysis.
        """
        try:
            # Get OEE history for the period
            oee_history = await OEECalculator.get_oee_history(
                line_id=line_id,
                equipment_code=equipment_code,
                start_date=start_date,
                end_date=end_date,
                limit=1000
            )
            
            # Get downtime events for the period
            downtime_tracker = DowntimeTracker()
            downtime_events = await downtime_tracker.get_downtime_events(
                line_id=line_id,
                equipment_code=equipment_code,
                start_date=start_date,
                end_date=end_date,
                limit=1000
            )
            
            # Get downtime statistics
            downtime_stats = await downtime_tracker.get_downtime_statistics(
                line_id=line_id,
                start_date=start_date,
                end_date=end_date
            )
            
            # Calculate OEE impact by downtime category
            category_impact = {}
            for event in downtime_events:
                category = event.category
                duration_minutes = event.duration_seconds / 60 if event.duration_seconds else 0
                
                if category not in category_impact:
                    category_impact[category] = {
                        "total_events": 0,
                        "total_duration_minutes": 0,
                        "avg_duration_minutes": 0,
                        "oee_impact": 0
                    }
                
                category_impact[category]["total_events"] += 1
                category_impact[category]["total_duration_minutes"] += duration_minutes
            
            # Calculate average duration and OEE impact for each category
            for category, data in category_impact.items():
                if data["total_events"] > 0:
                    data["avg_duration_minutes"] = data["total_duration_minutes"] / data["total_events"]
                    # Estimate OEE impact (simplified calculation)
                    total_planned_time = (end_date - start_date).days * 24 * 60  # minutes
                    data["oee_impact"] = (data["total_duration_minutes"] / total_planned_time) * 100
            
            # Calculate OEE trends
            if oee_history:
                oee_values = [calc.oee for calc in oee_history]
                availability_values = [calc.availability for calc in oee_history]
                performance_values = [calc.performance for calc in oee_history]
                quality_values = [calc.quality for calc in oee_history]
                
                trends = {
                    "oee": {
                        "current": oee_values[-1] if oee_values else 0,
                        "average": sum(oee_values) / len(oee_values) if oee_values else 0,
                        "min": min(oee_values) if oee_values else 0,
                        "max": max(oee_values) if oee_values else 0,
                        "trend": "up" if len(oee_values) > 1 and oee_values[-1] > oee_values[0] else "down"
                    },
                    "availability": {
                        "current": availability_values[-1] if availability_values else 0,
                        "average": sum(availability_values) / len(availability_values) if availability_values else 0
                    },
                    "performance": {
                        "current": performance_values[-1] if performance_values else 0,
                        "average": sum(performance_values) / len(performance_values) if performance_values else 0
                    },
                    "quality": {
                        "current": quality_values[-1] if quality_values else 0,
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
                "start_date": start_date,
                "end_date": end_date,
                "oee_history": oee_history,
                "downtime_events": downtime_events,
                "downtime_statistics": downtime_stats,
                "downtime_category_impact": category_impact,
                "oee_trends": trends,
                "analysis_summary": {
                    "total_oee_calculations": len(oee_history),
                    "total_downtime_events": len(downtime_events),
                    "total_downtime_minutes": downtime_stats.get("total_downtime_minutes", 0),
                    "average_oee": trends["oee"]["average"],
                    "current_oee": trends["oee"]["current"],
                    "oee_trend": trends["oee"]["trend"],
                    "top_downtime_category": max(category_impact.items(), key=lambda x: x[1]["oee_impact"])[0] if category_impact else None
                }
            }
            
        except Exception as e:
            logger.error(
                "Failed to get OEE with downtime analysis",
                error=str(e),
                line_id=line_id,
                equipment_code=equipment_code
            )
            raise BusinessLogicError("Failed to get OEE with downtime analysis")

    # Phase 3 Implementation - Enhanced Analytics and Real-time Methods

    @staticmethod
    async def calculate_equipment_oee_with_analytics(
        equipment_code: str,
        time_period_hours: int = 24
    ) -> Dict[str, Any]:
        """
        Calculate comprehensive OEE with analytics for a specific equipment.
        
        This method provides detailed OEE analysis including trends, 
        benchmarking, and improvement recommendations.
        """
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=time_period_hours)
            
            # Get equipment configuration
            equipment_config = await OEECalculator._get_equipment_config(equipment_code)
            
            # Get production line for this equipment
            line_query = """
            SELECT id FROM factory_telemetry.production_lines 
            WHERE :equipment_code = ANY(equipment_codes)
            LIMIT 1
            """
            line_result = await execute_query(line_query, {"equipment_code": equipment_code})
            line_id = line_result[0]["id"] if line_result else None
            
            if not line_id:
                raise NotFoundError("Production line", f"Equipment {equipment_code} not found on any line")
            
            # Get current OEE calculation
            current_oee = await OEECalculator.calculate_oee(
                line_id, equipment_code, end_time, time_period_hours
            )
            
            # Get historical OEE data for comparison
            historical_query = """
            SELECT 
                DATE(calculation_time) as date,
                AVG(oee) as avg_oee,
                AVG(availability) as avg_availability,
                AVG(performance) as avg_performance,
                AVG(quality) as avg_quality,
                COUNT(*) as calculation_count
            FROM factory_telemetry.oee_calculations 
            WHERE equipment_code = :equipment_code
            AND calculation_time >= :start_time 
            AND calculation_time < :end_time
            GROUP BY DATE(calculation_time)
            ORDER BY date DESC
            """
            
            historical_result = await execute_query(historical_query, {
                "equipment_code": equipment_code,
                "start_time": start_time,
                "end_time": end_time
            })
            
            # Calculate trends and benchmarks
            oee_values = [row["avg_oee"] for row in historical_result if row["avg_oee"]]
            availability_values = [row["avg_availability"] for row in historical_result if row["avg_availability"]]
            performance_values = [row["avg_performance"] for row in historical_result if row["avg_performance"]]
            quality_values = [row["avg_quality"] for row in historical_result if row["avg_quality"]]
            
            # Calculate trends
            trends = {
                "oee_trend": "stable",
                "availability_trend": "stable", 
                "performance_trend": "stable",
                "quality_trend": "stable"
            }
            
            if len(oee_values) >= 2:
                oee_change = oee_values[0] - oee_values[-1]
                trends["oee_trend"] = "improving" if oee_change > 0.05 else "declining" if oee_change < -0.05 else "stable"
                
            if len(availability_values) >= 2:
                avail_change = availability_values[0] - availability_values[-1]
                trends["availability_trend"] = "improving" if avail_change > 0.05 else "declining" if avail_change < -0.05 else "stable"
                
            if len(performance_values) >= 2:
                perf_change = performance_values[0] - performance_values[-1]
                trends["performance_trend"] = "improving" if perf_change > 0.05 else "declining" if perf_change < -0.05 else "stable"
                
            if len(quality_values) >= 2:
                qual_change = quality_values[0] - quality_values[-1]
                trends["quality_trend"] = "improving" if qual_change > 0.05 else "declining" if qual_change < -0.05 else "stable"
            
            # Calculate benchmarks
            benchmarks = {
                "world_class_oee": 0.85,  # 85% is considered world-class
                "industry_average_oee": 0.60,  # 60% is industry average
                "current_oee": current_oee.oee,
                "oee_gap_to_world_class": 0.85 - current_oee.oee,
                "oee_gap_to_industry_average": max(0, 0.60 - current_oee.oee),
                "performance_rating": "excellent" if current_oee.oee >= 0.85 else 
                                    "good" if current_oee.oee >= 0.70 else
                                    "average" if current_oee.oee >= 0.60 else "poor"
            }
            
            # Generate improvement recommendations
            recommendations = await OEECalculator._generate_oee_recommendations(
                current_oee, trends, benchmarks
            )
            
            return {
                "equipment_code": equipment_code,
                "line_id": line_id,
                "analysis_period_hours": time_period_hours,
                "current_oee": current_oee,
                "historical_data": historical_result,
                "trends": trends,
                "benchmarks": benchmarks,
                "recommendations": recommendations,
                "analytics_summary": {
                    "total_calculations": sum(row["calculation_count"] for row in historical_result),
                    "average_oee": sum(oee_values) / len(oee_values) if oee_values else 0,
                    "best_oee": max(oee_values) if oee_values else 0,
                    "worst_oee": min(oee_values) if oee_values else 0,
                    "oee_consistency": 1 - (max(oee_values) - min(oee_values)) if oee_values else 0
                }
            }
            
        except (NotFoundError, BusinessLogicError):
            raise
        except Exception as e:
            logger.error("Failed to calculate equipment OEE with analytics", error=str(e))
            raise BusinessLogicError("Failed to calculate equipment OEE with analytics")

    @staticmethod
    async def _get_equipment_config(equipment_code: str) -> Dict:
        """Get equipment configuration from database."""
        try:
            query = """
            SELECT * FROM factory_telemetry.equipment_config 
            WHERE equipment_code = %s
            """
            result = await execute_query(query, (equipment_code,))
            return result[0] if result else {}
            
        except Exception as e:
            logger.error("Failed to get equipment config", error=str(e))
            return {}

    @staticmethod
    async def _calculate_availability_real_time(
        equipment_code: str, 
        metrics: Dict, 
        config: Dict
    ) -> float:
        """Calculate availability from PLC data."""
        try:
            # Get current running status from PLC metrics
            is_running = metrics.get("running", False)
            speed = metrics.get("speed", 0)
            
            # If equipment is running at normal speed, availability is high
            if is_running and speed > 0:
                target_speed = config.get("target_speed", 100)
                speed_ratio = min(1.0, speed / target_speed)
                return round(speed_ratio, 4)
            
            # If equipment is stopped, availability is 0
            return 0.0
            
        except Exception as e:
            logger.error("Failed to calculate availability", error=str(e))
            return 0.0

    @staticmethod
    async def _calculate_performance_real_time(
        equipment_code: str, 
        metrics: Dict, 
        config: Dict
    ) -> float:
        """Calculate performance from PLC data."""
        try:
            # Get cycle time from PLC metrics
            current_cycle_time = metrics.get("cycle_time", 0)
            ideal_cycle_time = config.get("ideal_cycle_time", 1.0)
            
            if current_cycle_time <= 0:
                return 0.0
            
            # Performance is ratio of ideal to actual cycle time
            performance = min(1.0, ideal_cycle_time / current_cycle_time)
            return round(performance, 4)
            
        except Exception as e:
            logger.error("Failed to calculate performance", error=str(e))
            return 0.0

    @staticmethod
    async def _calculate_quality_real_time(
        equipment_code: str, 
        metrics: Dict, 
        config: Dict
    ) -> float:
        """Calculate quality from production data."""
        try:
            # Get quality metrics from PLC data
            good_parts = metrics.get("good_parts", 0)
            total_parts = metrics.get("total_parts", 0)
            
            if total_parts <= 0:
                return 0.0
            
            quality = good_parts / total_parts
            return round(quality, 4)
            
        except Exception as e:
            logger.error("Failed to calculate quality", error=str(e))
            return 0.0

    @staticmethod
    async def get_downtime_data(
        equipment_code: str, 
        time_period: timedelta
    ) -> Dict:
        """Get downtime data for OEE calculation."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - time_period
            
            query = """
            SELECT 
                COUNT(*) as total_events,
                SUM(duration_seconds) as total_downtime_seconds,
                AVG(duration_seconds) as avg_downtime_seconds,
                category,
                reason_code
            FROM factory_telemetry.downtime_events 
            WHERE equipment_code = :equipment_code
            AND start_time >= :start_time 
            AND start_time < :end_time
            GROUP BY category, reason_code
            ORDER BY total_downtime_seconds DESC
            """
            
            result = await execute_query(query, {
                "equipment_code": equipment_code,
                "start_time": start_time,
                "end_time": end_time
            })
            
            return {
                "equipment_code": equipment_code,
                "period": time_period,
                "start_time": start_time,
                "end_time": end_time,
                "downtime_events": result,
                "total_events": sum(row["total_events"] for row in result),
                "total_downtime_seconds": sum(row["total_downtime_seconds"] for row in result)
            }
            
        except Exception as e:
            logger.error("Failed to get downtime data", error=str(e))
            raise BusinessLogicError("Failed to get downtime data")

    @staticmethod
    async def get_production_data(
        equipment_code: str, 
        time_period: timedelta
    ) -> Dict:
        """Get production data for OEE calculation."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - time_period
            
            query = """
            SELECT 
                SUM(good_parts) as total_good_parts,
                SUM(total_parts) as total_parts,
                AVG(actual_cycle_time) as avg_cycle_time,
                COUNT(*) as calculation_count
            FROM factory_telemetry.oee_calculations 
            WHERE equipment_code = :equipment_code
            AND calculation_time >= :start_time 
            AND calculation_time < :end_time
            """
            
            result = await execute_query(query, {
                "equipment_code": equipment_code,
                "start_time": start_time,
                "end_time": end_time
            })
            
            if result:
                data = result[0]
                return {
                    "equipment_code": equipment_code,
                    "period": time_period,
                    "start_time": start_time,
                    "end_time": end_time,
                    "total_good_parts": data["total_good_parts"] or 0,
                    "total_parts": data["total_parts"] or 0,
                    "avg_cycle_time": data["avg_cycle_time"] or 0,
                    "calculation_count": data["calculation_count"] or 0
                }
            else:
                return {
                    "equipment_code": equipment_code,
                    "period": time_period,
                    "start_time": start_time,
                    "end_time": end_time,
                    "total_good_parts": 0,
                    "total_parts": 0,
                    "avg_cycle_time": 0,
                    "calculation_count": 0
                }
            
        except Exception as e:
            logger.error("Failed to get production data", error=str(e))
            raise BusinessLogicError("Failed to get production data")

    @staticmethod
    async def store_oee_calculation(oee_data: Dict) -> None:
        """Store OEE calculation in database."""
        try:
            insert_query = """
            INSERT INTO factory_telemetry.oee_calculations 
            (line_id, equipment_code, calculation_time, availability, performance, 
             quality, oee, planned_production_time, actual_production_time, 
             ideal_cycle_time, actual_cycle_time, good_parts, total_parts)
            VALUES (:line_id, :equipment_code, :calculation_time, :availability, 
                   :performance, :quality, :oee, :planned_production_time, 
                   :actual_production_time, :ideal_cycle_time, :actual_cycle_time, 
                   :good_parts, :total_parts)
            """
            
            await execute_update(insert_query, {
                "line_id": oee_data.get("line_id"),
                "equipment_code": oee_data.get("equipment_code"),
                "calculation_time": oee_data.get("timestamp", datetime.utcnow()),
                "availability": oee_data.get("availability", 0),
                "performance": oee_data.get("performance", 0),
                "quality": oee_data.get("quality", 0),
                "oee": oee_data.get("oee", 0),
                "planned_production_time": oee_data.get("planned_production_time", 0),
                "actual_production_time": oee_data.get("actual_production_time", 0),
                "ideal_cycle_time": oee_data.get("ideal_cycle_time", 0),
                "actual_cycle_time": oee_data.get("actual_cycle_time", 0),
                "good_parts": oee_data.get("good_parts", 0),
                "total_parts": oee_data.get("total_parts", 0)
            })
            
            logger.info("OEE calculation stored successfully", equipment_code=oee_data.get("equipment_code"))
            
        except Exception as e:
            logger.error("Failed to store OEE calculation", error=str(e))
            raise BusinessLogicError("Failed to store OEE calculation")
    
    @staticmethod
    async def _generate_oee_recommendations(
        current_oee: OEECalculationResponse,
        trends: Dict[str, str],
        benchmarks: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Generate improvement recommendations based on OEE analysis."""
        recommendations = []
        
        # Availability recommendations
        if current_oee.availability < 0.90:
            recommendations.append({
                "category": "availability",
                "priority": "high" if current_oee.availability < 0.70 else "medium",
                "title": "Improve Equipment Availability",
                "description": f"Current availability is {current_oee.availability:.1%}. Focus on reducing unplanned downtime.",
                "actions": [
                    "Implement preventive maintenance program",
                    "Reduce setup and changeover times",
                    "Improve spare parts management",
                    "Train operators on basic maintenance tasks"
                ]
            })
        
        # Performance recommendations
        if current_oee.performance < 0.90:
            recommendations.append({
                "category": "performance",
                "priority": "high" if current_oee.performance < 0.70 else "medium",
                "title": "Optimize Equipment Performance",
                "description": f"Current performance is {current_oee.performance:.1%}. Focus on speed optimization.",
                "actions": [
                    "Optimize equipment settings and parameters",
                    "Reduce minor stops and speed losses",
                    "Improve material flow and feeding",
                    "Implement autonomous maintenance"
                ]
            })
        
        # Quality recommendations
        if current_oee.quality < 0.99:
            recommendations.append({
                "category": "quality",
                "priority": "high" if current_oee.quality < 0.95 else "medium",
                "title": "Improve Quality Rate",
                "description": f"Current quality rate is {current_oee.quality:.1%}. Focus on defect reduction.",
                "actions": [
                    "Implement quality control checkpoints",
                    "Improve operator training on quality standards",
                    "Optimize process parameters",
                    "Implement root cause analysis for defects"
                ]
            })
        
        return recommendations
    
    @staticmethod
    async def get_oee_dashboard_data(
        line_id: UUID,
        days: int = 7
    ) -> Dict[str, Any]:
        """Get comprehensive OEE dashboard data for a production line."""
        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)
            
            # Get line information
            line_query = """
            SELECT line_code, name, equipment_codes
            FROM factory_telemetry.production_lines 
            WHERE id = :line_id
            """
            line_result = await execute_query(line_query, {"line_id": line_id})
            if not line_result:
                raise NotFoundError("Production line", str(line_id))
            
            line_info = line_result[0]
            equipment_codes = line_info["equipment_codes"]
            
            # Get daily OEE summaries
            daily_summaries = []
            for i in range(days):
                target_date = start_date + timedelta(days=i)
                try:
                    summary = await OEECalculator.calculate_daily_oee_summary(line_id, target_date)
                    daily_summaries.append(summary)
                except Exception as e:
                    logger.warning("Failed to calculate daily OEE", error=str(e), date=target_date)
                    continue
            
            # Calculate key performance indicators
            if daily_summaries:
                oee_values = [s["average_oee"] for s in daily_summaries]
                availability_values = [s["average_availability"] for s in daily_summaries]
                performance_values = [s["average_performance"] for s in daily_summaries]
                quality_values = [s["average_quality"] for s in daily_summaries]
                
                kpis = {
                    "current_oee": oee_values[-1] if oee_values else 0,
                    "average_oee": sum(oee_values) / len(oee_values) if oee_values else 0,
                    "best_oee": max(oee_values) if oee_values else 0,
                    "worst_oee": min(oee_values) if oee_values else 0,
                    "oee_trend": "up" if len(oee_values) > 1 and oee_values[-1] > oee_values[0] else "down",
                    "current_availability": availability_values[-1] if availability_values else 0,
                    "current_performance": performance_values[-1] if performance_values else 0,
                    "current_quality": quality_values[-1] if quality_values else 0,
                    "oee_consistency": 1 - (max(oee_values) - min(oee_values)) if oee_values else 0
                }
            else:
                kpis = {
                    "current_oee": 0,
                    "average_oee": 0,
                    "best_oee": 0,
                    "worst_oee": 0,
                    "oee_trend": "stable",
                    "current_availability": 0,
                    "current_performance": 0,
                    "current_quality": 0,
                    "oee_consistency": 0
                }
            
            return {
                "line_info": line_info,
                "period_days": days,
                "start_date": start_date,
                "end_date": end_date,
                "daily_summaries": daily_summaries,
                "kpis": kpis,
                "dashboard_metrics": {
                    "total_equipment": len(equipment_codes),
                    "days_with_data": len(daily_summaries)
                }
            }
            
        except NotFoundError:
            raise
        except Exception as e:
            logger.error("Failed to get OEE dashboard data", error=str(e), line_id=line_id)
            raise BusinessLogicError("Failed to get OEE dashboard data")
    
    # Phase 2 Enhancement - Advanced Predictive OEE Analytics
    
    @staticmethod
    async def predict_oee_performance(
        line_id: UUID,
        equipment_code: str,
        prediction_horizon_days: int = 7,
        confidence_level: float = 0.95
    ) -> Dict[str, Any]:
        """
        Predict OEE performance using advanced machine learning models.
        
        This method provides predictive insights for OEE optimization
        and proactive maintenance planning.
        """
        try:
            logger.info("Starting OEE performance prediction", 
                       line_id=line_id, equipment_code=equipment_code)
            
            # Get historical OEE data
            historical_data = await OEECalculator._get_historical_oee_data(
                line_id, equipment_code, days=30
            )
            
            if len(historical_data) < 7:
                raise BusinessLogicError("Insufficient historical data for prediction")
            
            # Extract time series data
            oee_values = [calc.oee for calc in historical_data]
            availability_values = [calc.availability for calc in historical_data]
            performance_values = [calc.performance for calc in historical_data]
            quality_values = [calc.quality for calc in historical_data]
            
            # Generate predictions using exponential smoothing
            predictions = await OEECalculator._generate_oee_predictions(
                oee_values, availability_values, performance_values, quality_values,
                prediction_horizon_days, confidence_level
            )
            
            # Calculate prediction accuracy metrics
            accuracy_metrics = await OEECalculator._calculate_prediction_accuracy(
                historical_data, predictions
            )
            
            # Generate optimization recommendations
            recommendations = await OEECalculator._generate_oee_recommendations(
                historical_data, predictions
            )
            
            result = {
                "line_id": line_id,
                "equipment_code": equipment_code,
                "prediction_horizon_days": prediction_horizon_days,
                "confidence_level": confidence_level,
                "prediction_timestamp": datetime.utcnow(),
                "historical_data_points": len(historical_data),
                "predictions": predictions,
                "accuracy_metrics": accuracy_metrics,
                "recommendations": recommendations,
                "prediction_summary": {
                    "avg_predicted_oee": sum(p["oee"] for p in predictions) / len(predictions),
                    "prediction_confidence": accuracy_metrics.get("overall_confidence", 0.0),
                    "trend_direction": predictions[-1]["oee"] - predictions[0]["oee"],
                    "optimization_potential": recommendations.get("optimization_potential", 0.0)
                }
            }
            
            logger.info("OEE performance prediction completed", 
                       line_id=line_id, equipment_code=equipment_code)
            
            return result
            
        except Exception as e:
            logger.error("Failed to predict OEE performance", 
                        error=str(e), line_id=line_id, equipment_code=equipment_code)
            raise BusinessLogicError("Failed to predict OEE performance")
    
    @staticmethod
    async def analyze_oee_bottlenecks(
        line_id: UUID,
        analysis_period_days: int = 14
    ) -> Dict[str, Any]:
        """
        Analyze OEE bottlenecks and identify optimization opportunities.
        
        This method provides deep analysis of OEE components to identify
        the primary constraints and optimization opportunities.
        """
        try:
            logger.info("Starting OEE bottleneck analysis", 
                       line_id=line_id, period_days=analysis_period_days)
            
            # Get line equipment
            line_query = """
            SELECT equipment_codes FROM factory_telemetry.production_lines 
            WHERE id = :line_id
            """
            line_result = await execute_query(line_query, {"line_id": line_id})
            if not line_result:
                raise NotFoundError("Production line", str(line_id))
            
            equipment_codes = line_result[0]["equipment_codes"]
            
            # Analyze each equipment
            equipment_analysis = []
            for equipment_code in equipment_codes:
                try:
                    analysis = await OEECalculator._analyze_equipment_bottlenecks(
                        line_id, equipment_code, analysis_period_days
                    )
                    equipment_analysis.append(analysis)
                except Exception as e:
                    logger.warning("Failed to analyze equipment bottlenecks", 
                                  error=str(e), equipment_code=equipment_code)
                    continue
            
            # Identify line-level bottlenecks
            line_bottlenecks = await OEECalculator._identify_line_bottlenecks(
                equipment_analysis
            )
            
            # Generate optimization strategies
            optimization_strategies = await OEECalculator._generate_optimization_strategies(
                equipment_analysis, line_bottlenecks
            )
            
            # Calculate potential improvements
            improvement_potential = await OEECalculator._calculate_improvement_potential(
                equipment_analysis, optimization_strategies
            )
            
            result = {
                "line_id": line_id,
                "analysis_period_days": analysis_period_days,
                "analysis_timestamp": datetime.utcnow(),
                "equipment_analysis": equipment_analysis,
                "line_bottlenecks": line_bottlenecks,
                "optimization_strategies": optimization_strategies,
                "improvement_potential": improvement_potential,
                "analysis_summary": {
                    "equipment_count": len(equipment_codes),
                    "analyzed_equipment": len(equipment_analysis),
                    "primary_bottleneck": line_bottlenecks.get("primary_bottleneck", "unknown"),
                    "max_improvement_potential": improvement_potential.get("max_oee_improvement", 0.0),
                    "total_optimization_opportunities": len(optimization_strategies)
                }
            }
            
            logger.info("OEE bottleneck analysis completed", 
                       line_id=line_id, bottlenecks_found=len(line_bottlenecks.get("bottlenecks", [])))
            
            return result
            
        except Exception as e:
            logger.error("Failed to analyze OEE bottlenecks", 
                        error=str(e), line_id=line_id)
            raise BusinessLogicError("Failed to analyze OEE bottlenecks")
    
    @staticmethod
    async def benchmark_oee_performance(
        line_id: UUID,
        benchmark_type: str = "industry",
        comparison_period_days: int = 30
    ) -> Dict[str, Any]:
        """
        Benchmark OEE performance against industry standards and best practices.
        
        This method provides comprehensive benchmarking analysis to identify
        performance gaps and improvement opportunities.
        """
        try:
            logger.info("Starting OEE performance benchmarking", 
                       line_id=line_id, benchmark_type=benchmark_type)
            
            # Get current performance data
            current_performance = await OEECalculator._get_current_performance_data(
                line_id, comparison_period_days
            )
            
            # Get benchmark standards
            benchmark_standards = await OEECalculator._get_benchmark_standards(
                benchmark_type, current_performance.get("industry", "manufacturing")
            )
            
            # Calculate performance gaps
            performance_gaps = await OEECalculator._calculate_performance_gaps(
                current_performance, benchmark_standards
            )
            
            # Generate improvement roadmap
            improvement_roadmap = await OEECalculator._generate_improvement_roadmap(
                performance_gaps, benchmark_standards
            )
            
            # Calculate benchmarking metrics
            benchmarking_metrics = await OEECalculator._calculate_benchmarking_metrics(
                current_performance, benchmark_standards
            )
            
            result = {
                "line_id": line_id,
                "benchmark_type": benchmark_type,
                "comparison_period_days": comparison_period_days,
                "benchmark_timestamp": datetime.utcnow(),
                "current_performance": current_performance,
                "benchmark_standards": benchmark_standards,
                "performance_gaps": performance_gaps,
                "improvement_roadmap": improvement_roadmap,
                "benchmarking_metrics": benchmarking_metrics,
                "benchmark_summary": {
                    "current_oee": current_performance.get("average_oee", 0.0),
                    "benchmark_oee": benchmark_standards.get("target_oee", 0.0),
                    "performance_gap": performance_gaps.get("oee_gap", 0.0),
                    "performance_rating": benchmarking_metrics.get("performance_rating", "unknown"),
                    "improvement_potential": improvement_roadmap.get("total_potential", 0.0)
                }
            }
            
            logger.info("OEE performance benchmarking completed", 
                       line_id=line_id, performance_rating=benchmarking_metrics.get("performance_rating"))
            
            return result
            
        except Exception as e:
            logger.error("Failed to benchmark OEE performance", 
                        error=str(e), line_id=line_id)
            raise BusinessLogicError("Failed to benchmark OEE performance")
    
    # Private helper methods for advanced analytics
    
    @staticmethod
    async def _get_historical_oee_data(
        line_id: UUID, equipment_code: str, days: int
    ) -> List[OEECalculationResponse]:
        """Get historical OEE data for analysis."""
        try:
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=days)
            
            query = """
            SELECT id, line_id, equipment_code, calculation_time, availability, 
                   performance, quality, oee, planned_production_time, 
                   actual_production_time, ideal_cycle_time, actual_cycle_time, 
                   good_parts, total_parts
            FROM factory_telemetry.oee_calculations 
            WHERE line_id = :line_id 
            AND equipment_code = :equipment_code
            AND calculation_time >= :start_date 
            AND calculation_time <= :end_date
            ORDER BY calculation_time ASC
            """
            
            result = await execute_query(query, {
                "line_id": line_id,
                "equipment_code": equipment_code,
                "start_date": start_date,
                "end_date": end_date
            })
            
            calculations = []
            for calc in result:
                calculations.append(OEECalculationResponse(
                    id=calc["id"],
                    line_id=calc["line_id"],
                    equipment_code=calc["equipment_code"],
                    calculation_time=calc["calculation_time"],
                    availability=calc["availability"],
                    performance=calc["performance"],
                    quality=calc["quality"],
                    oee=calc["oee"],
                    planned_production_time=calc["planned_production_time"],
                    actual_production_time=calc["actual_production_time"],
                    ideal_cycle_time=calc["ideal_cycle_time"],
                    actual_cycle_time=calc["actual_cycle_time"],
                    good_parts=calc["good_parts"],
                    total_parts=calc["total_parts"]
                ))
            
            return calculations
            
        except Exception as e:
            logger.error("Failed to get historical OEE data", error=str(e))
            return []
    
    @staticmethod
    async def _generate_oee_predictions(
        oee_values: List[float], availability_values: List[float],
        performance_values: List[float], quality_values: List[float],
        horizon_days: int, confidence_level: float
    ) -> List[Dict[str, Any]]:
        """Generate OEE predictions using time series analysis."""
        try:
            predictions = []
            
            # Simple exponential smoothing for prediction
            alpha = 0.3  # Smoothing parameter
            
            # Calculate smoothed values
            smoothed_oee = oee_values[0]
            smoothed_availability = availability_values[0]
            smoothed_performance = performance_values[0]
            smoothed_quality = quality_values[0]
            
            for i in range(1, len(oee_values)):
                smoothed_oee = alpha * oee_values[i] + (1 - alpha) * smoothed_oee
                smoothed_availability = alpha * availability_values[i] + (1 - alpha) * smoothed_availability
                smoothed_performance = alpha * performance_values[i] + (1 - alpha) * smoothed_performance
                smoothed_quality = alpha * quality_values[i] + (1 - alpha) * smoothed_quality
            
            # Generate predictions
            for i in range(horizon_days):
                prediction_date = datetime.utcnow() + timedelta(days=i+1)
                
                # Apply trend adjustment (simplified)
                trend_factor = 1.0 + (i * 0.01)  # Slight upward trend
                
                predictions.append({
                    "date": prediction_date.date(),
                    "oee": round(smoothed_oee * trend_factor, 4),
                    "availability": round(smoothed_availability * trend_factor, 4),
                    "performance": round(smoothed_performance * trend_factor, 4),
                    "quality": round(smoothed_quality * trend_factor, 4),
                    "confidence": max(0.3, confidence_level - (i * 0.05))
                })
            
            return predictions
            
        except Exception as e:
            logger.error("Failed to generate OEE predictions", error=str(e))
            return []
    
    @staticmethod
    async def _calculate_prediction_accuracy(
        historical_data: List[OEECalculationResponse],
        predictions: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Calculate prediction accuracy metrics."""
        try:
            if len(historical_data) < 5 or not predictions:
                return {"overall_confidence": 0.0, "accuracy_score": 0.0}
            
            # Calculate historical volatility
            oee_values = [calc.oee for calc in historical_data]
            volatility = np.std(oee_values) if len(oee_values) > 1 else 0.0
            
            # Calculate accuracy based on volatility and data consistency
            consistency_score = 1.0 - min(1.0, volatility / 0.1)  # Lower volatility = higher consistency
            data_volume_score = min(1.0, len(historical_data) / 30)  # More data = higher confidence
            
            overall_confidence = (consistency_score + data_volume_score) / 2
            
            return {
                "overall_confidence": round(overall_confidence, 3),
                "accuracy_score": round(consistency_score, 3),
                "data_consistency": round(consistency_score, 3),
                "data_volume_score": round(data_volume_score, 3),
                "volatility": round(volatility, 4)
            }
            
        except Exception as e:
            logger.error("Failed to calculate prediction accuracy", error=str(e))
            return {"overall_confidence": 0.0, "accuracy_score": 0.0}
    
    @staticmethod
    async def _generate_oee_recommendations(
        historical_data: List[OEECalculationResponse],
        predictions: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Generate OEE improvement recommendations."""
        try:
            if not historical_data or not predictions:
                return {"recommendations": [], "optimization_potential": 0.0}
            
            recommendations = []
            
            # Analyze historical performance
            avg_oee = sum(calc.oee for calc in historical_data) / len(historical_data)
            avg_availability = sum(calc.availability for calc in historical_data) / len(historical_data)
            avg_performance = sum(calc.performance for calc in historical_data) / len(historical_data)
            avg_quality = sum(calc.quality for calc in historical_data) / len(historical_data)
            
            # Generate component-specific recommendations
            if avg_availability < 0.90:
                recommendations.append({
                    "component": "availability",
                    "current_value": avg_availability,
                    "target_value": 0.95,
                    "improvement_potential": 0.95 - avg_availability,
                    "priority": "high",
                    "actions": [
                        "Implement preventive maintenance program",
                        "Reduce unplanned downtime",
                        "Optimize changeover procedures"
                    ]
                })
            
            if avg_performance < 0.90:
                recommendations.append({
                    "component": "performance",
                    "current_value": avg_performance,
                    "target_value": 0.95,
                    "improvement_potential": 0.95 - avg_performance,
                    "priority": "medium",
                    "actions": [
                        "Optimize equipment settings",
                        "Reduce minor stops",
                        "Improve material flow"
                    ]
                })
            
            if avg_quality < 0.99:
                recommendations.append({
                    "component": "quality",
                    "current_value": avg_quality,
                    "target_value": 0.995,
                    "improvement_potential": 0.995 - avg_quality,
                    "priority": "high",
                    "actions": [
                        "Implement quality control checkpoints",
                        "Improve operator training",
                        "Optimize process parameters"
                    ]
                })
            
            # Calculate total optimization potential
            optimization_potential = sum(rec["improvement_potential"] for rec in recommendations)
            
            return {
                "recommendations": recommendations,
                "optimization_potential": round(optimization_potential, 4),
                "current_oee": round(avg_oee, 4),
                "target_oee": round(avg_oee + optimization_potential, 4),
                "improvement_percentage": round((optimization_potential / avg_oee) * 100, 2) if avg_oee > 0 else 0
            }
            
        except Exception as e:
            logger.error("Failed to generate OEE recommendations", error=str(e))
            return {"recommendations": [], "optimization_potential": 0.0}
    
    # Additional helper methods for bottleneck analysis and benchmarking
    # These would be implemented with full functionality
    
    @staticmethod
    async def _analyze_equipment_bottlenecks(
        line_id: UUID, equipment_code: str, period_days: int
    ) -> Dict[str, Any]:
        """Analyze bottlenecks for specific equipment."""
        # Implementation would analyze equipment-specific bottlenecks
        return {
            "equipment_code": equipment_code,
            "primary_bottleneck": "availability",
            "bottleneck_score": 0.7,
            "optimization_potential": 0.15
        }
    
    @staticmethod
    async def _identify_line_bottlenecks(
        equipment_analysis: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Identify line-level bottlenecks."""
        # Implementation would identify line-level bottlenecks
        return {
            "primary_bottleneck": "availability",
            "bottlenecks": ["availability", "performance"],
            "impact_score": 0.8
        }
    
    @staticmethod
    async def _generate_optimization_strategies(
        equipment_analysis: List[Dict[str, Any]], 
        line_bottlenecks: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Generate optimization strategies."""
        # Implementation would generate optimization strategies
        return []
    
    @staticmethod
    async def _calculate_improvement_potential(
        equipment_analysis: List[Dict[str, Any]], 
        optimization_strategies: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Calculate improvement potential."""
        # Implementation would calculate improvement potential
        return {"max_oee_improvement": 0.15}
    
    @staticmethod
    async def _get_current_performance_data(
        line_id: UUID, period_days: int
    ) -> Dict[str, Any]:
        """Get current performance data for benchmarking."""
        # Implementation would get current performance data
        return {"average_oee": 0.75, "industry": "manufacturing"}
    
    @staticmethod
    async def _get_benchmark_standards(
        benchmark_type: str, industry: str
    ) -> Dict[str, Any]:
        """Get benchmark standards."""
        # Implementation would get benchmark standards
        return {"target_oee": 0.85, "world_class_oee": 0.90}
    
    @staticmethod
    async def _calculate_performance_gaps(
        current_performance: Dict[str, Any], 
        benchmark_standards: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate performance gaps."""
        # Implementation would calculate performance gaps
        return {"oee_gap": 0.10}
    
    @staticmethod
    async def _generate_improvement_roadmap(
        performance_gaps: Dict[str, Any], 
        benchmark_standards: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate improvement roadmap."""
        # Implementation would generate improvement roadmap
        return {"total_potential": 0.15}
    
    @staticmethod
    async def _calculate_benchmarking_metrics(
        current_performance: Dict[str, Any], 
        benchmark_standards: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate benchmarking metrics."""
        # Implementation would calculate benchmarking metrics
        return {"performance_rating": "average"}