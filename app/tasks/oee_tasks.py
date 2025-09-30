"""
MS5.0 Floor Dashboard - OEE Tasks

This module contains Celery tasks for OEE (Overall Equipment Effectiveness)
calculation and analytics, including real-time OEE monitoring, historical
analysis, and performance optimization recommendations.

Tasks:
- OEE metrics calculation
- Line-specific OEE calculations
- OEE analytics and reporting
- Performance trend analysis
- OEE optimization recommendations
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from celery import current_task
from app.celery import celery_app
from app.config import settings
from app.services.cache_service import CacheService
from app.services.database_service import DatabaseService
from app.models.production import ProductionLine
from app.models.oee import OEEMetrics, OEECalculation
import structlog

# Configure structured logging
logger = structlog.get_logger(__name__)

# Initialize services
cache_service = CacheService()
db_service = DatabaseService()


@celery_app.task(bind=True, name="app.tasks.oee_tasks.calculate_oee_metrics")
def calculate_oee_metrics(self, production_line_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Calculate OEE metrics for specific line or all active lines.
    
    OEE is calculated as: Availability × Performance × Quality
    
    Args:
        production_line_id: Optional specific line ID to calculate
        
    Returns:
        Dict containing OEE calculation results
    """
    try:
        logger.info(
            "Starting OEE metrics calculation",
            task_id=self.request.id,
            production_line_id=production_line_id
        )
        
        if production_line_id:
            # Calculate for specific line
            lines = [asyncio.run(_get_production_line(production_line_id))]
        else:
            # Calculate for all active lines
            lines = asyncio.run(_get_active_production_lines())
        
        calculated_lines = 0
        total_oee_score = 0
        
        for line in lines:
            try:
                # Calculate OEE components
                oee_data = asyncio.run(_calculate_oee_components(line))
                
                # Calculate overall OEE score
                oee_score = (
                    oee_data['availability'] * 
                    oee_data['performance'] * 
                    oee_data['quality']
                ) / 10000  # Convert to percentage
                
                # Store OEE metrics
                asyncio.run(_store_oee_metrics(line.id, oee_data, oee_score))
                
                # Update cache
                cache_key = f"oee_metrics:{line.id}"
                cache_service.set(cache_key, {
                    **oee_data,
                    'oee_score': oee_score,
                    'timestamp': datetime.utcnow().isoformat()
                }, ttl=300)  # 5 minutes
                
                calculated_lines += 1
                total_oee_score += oee_score
                
                logger.info(
                    "OEE metrics calculated",
                    line_id=line.id,
                    oee_score=oee_score,
                    availability=oee_data['availability'],
                    performance=oee_data['performance'],
                    quality=oee_data['quality']
                )
                
            except Exception as e:
                logger.error(
                    "Failed to calculate OEE metrics",
                    line_id=line.id,
                    error=str(e),
                    exc_info=True
                )
        
        average_oee = total_oee_score / calculated_lines if calculated_lines > 0 else 0
        
        result = {
            "status": "success",
            "calculated_lines": calculated_lines,
            "average_oee_score": average_oee,
            "timestamp": datetime.utcnow().isoformat(),
            "task_id": self.request.id
        }
        
        logger.info("OEE metrics calculation completed", **result)
        return result
        
    except Exception as exc:
        logger.error(
            "OEE metrics calculation failed",
            production_line_id=production_line_id,
            error=str(exc),
            exc_info=True
        )
        raise self.retry(exc=exc, countdown=60, max_retries=3)


@celery_app.task(bind=True, name="app.tasks.oee_tasks.calculate_line_oee")
def calculate_line_oee(self, production_line_id: str, time_period: str = "hourly") -> Dict[str, Any]:
    """
    Calculate detailed OEE for a specific production line.
    
    Args:
        production_line_id: Production line ID
        time_period: Time period for calculation (hourly, daily, weekly)
        
    Returns:
        Dict containing detailed OEE results
    """
    try:
        logger.info(
            "Starting line OEE calculation",
            task_id=self.request.id,
            production_line_id=production_line_id,
            time_period=time_period
        )
        
        # Get production line
        line = asyncio.run(_get_production_line(production_line_id))
        if not line:
            raise ValueError(f"Production line not found: {production_line_id}")
        
        # Calculate time range
        end_time = datetime.utcnow()
        if time_period == "hourly":
            start_time = end_time - timedelta(hours=1)
        elif time_period == "daily":
            start_time = end_time - timedelta(days=1)
        elif time_period == "weekly":
            start_time = end_time - timedelta(weeks=1)
        else:
            raise ValueError(f"Invalid time period: {time_period}")
        
        # Calculate detailed OEE components
        oee_components = asyncio.run(_calculate_detailed_oee_components(
            line, start_time, end_time
        ))
        
        # Calculate trend analysis
        trend_data = asyncio.run(_calculate_oee_trends(line.id, time_period))
        
        # Generate optimization recommendations
        recommendations = asyncio.run(_generate_oee_recommendations(
            line.id, oee_components
        ))
        
        # Store detailed OEE calculation
        asyncio.run(_store_detailed_oee_calculation(
            line.id, time_period, oee_components, trend_data, recommendations
        ))
        
        # Update cache with detailed results
        cache_key = f"line_oee:{line.id}:{time_period}"
        cache_service.set(cache_key, {
            "oee_components": oee_components,
            "trend_data": trend_data,
            "recommendations": recommendations,
            "timestamp": datetime.utcnow().isoformat()
        }, ttl=600)  # 10 minutes
        
        result = {
            "status": "success",
            "production_line_id": production_line_id,
            "time_period": time_period,
            "oee_score": oee_components['oee_score'],
            "availability": oee_components['availability'],
            "performance": oee_components['performance'],
            "quality": oee_components['quality'],
            "trend_direction": trend_data.get('trend_direction'),
            "recommendations_count": len(recommendations),
            "timestamp": datetime.utcnow().isoformat(),
            "task_id": self.request.id
        }
        
        logger.info("Line OEE calculation completed", **result)
        return result
        
    except Exception as exc:
        logger.error(
            "Line OEE calculation failed",
            production_line_id=production_line_id,
            time_period=time_period,
            error=str(exc),
            exc_info=True
        )
        raise self.retry(exc=exc, countdown=30, max_retries=3)


@celery_app.task(bind=True, name="app.tasks.oee_tasks.calculate_oee_analytics")
def calculate_oee_analytics(self, time_period: str = "daily") -> Dict[str, Any]:
    """
    Calculate comprehensive OEE analytics across all production lines.
    
    Args:
        time_period: Time period for analytics (daily, weekly, monthly)
        
    Returns:
        Dict containing OEE analytics results
    """
    try:
        logger.info(
            "Starting OEE analytics calculation",
            task_id=self.request.id,
            time_period=time_period
        )
        
        # Calculate time range
        end_time = datetime.utcnow()
        if time_period == "daily":
            start_time = end_time - timedelta(days=1)
        elif time_period == "weekly":
            start_time = end_time - timedelta(weeks=1)
        elif time_period == "monthly":
            start_time = end_time - timedelta(days=30)
        else:
            raise ValueError(f"Invalid time period: {time_period}")
        
        # Get all active production lines
        lines = asyncio.run(_get_active_production_lines())
        
        analytics_data = {
            "overall_oee": 0,
            "line_performance": [],
            "top_performers": [],
            "improvement_opportunities": [],
            "downtime_analysis": {},
            "quality_trends": {},
            "performance_benchmarks": {}
        }
        
        total_oee = 0
        line_count = 0
        
        for line in lines:
            try:
                # Calculate line analytics
                line_analytics = asyncio.run(_calculate_line_analytics(
                    line, start_time, end_time
                ))
                
                analytics_data["line_performance"].append(line_analytics)
                total_oee += line_analytics["oee_score"]
                line_count += 1
                
            except Exception as e:
                logger.error(
                    "Failed to calculate line analytics",
                    line_id=line.id,
                    error=str(e),
                    exc_info=True
                )
        
        # Calculate overall analytics
        analytics_data["overall_oee"] = total_oee / line_count if line_count > 0 else 0
        
        # Identify top performers and improvement opportunities
        analytics_data["top_performers"] = _identify_top_performers(
            analytics_data["line_performance"]
        )
        analytics_data["improvement_opportunities"] = _identify_improvement_opportunities(
            analytics_data["line_performance"]
        )
        
        # Calculate downtime and quality trends
        analytics_data["downtime_analysis"] = asyncio.run(
            _calculate_downtime_analysis(start_time, end_time)
        )
        analytics_data["quality_trends"] = asyncio.run(
            _calculate_quality_trends(start_time, end_time)
        )
        
        # Calculate performance benchmarks
        analytics_data["performance_benchmarks"] = _calculate_performance_benchmarks(
            analytics_data["line_performance"]
        )
        
        # Store analytics results
        asyncio.run(_store_oee_analytics(time_period, analytics_data))
        
        # Update cache
        cache_key = f"oee_analytics:{time_period}"
        cache_service.set(cache_key, analytics_data, ttl=1800)  # 30 minutes
        
        result = {
            "status": "success",
            "time_period": time_period,
            "overall_oee": analytics_data["overall_oee"],
            "lines_analyzed": line_count,
            "top_performers_count": len(analytics_data["top_performers"]),
            "improvement_opportunities_count": len(analytics_data["improvement_opportunities"]),
            "timestamp": datetime.utcnow().isoformat(),
            "task_id": self.request.id
        }
        
        logger.info("OEE analytics calculation completed", **result)
        return result
        
    except Exception as exc:
        logger.error(
            "OEE analytics calculation failed",
            time_period=time_period,
            error=str(exc),
            exc_info=True
        )
        raise self.retry(exc=exc, countdown=60, max_retries=3)


@celery_app.task(bind=True, name="app.tasks.oee_tasks.update_oee_trends")
def update_oee_trends(self, production_line_id: str) -> Dict[str, Any]:
    """
    Update OEE trends and performance indicators for a production line.
    
    Args:
        production_line_id: Production line ID to update trends for
        
    Returns:
        Dict containing trend update results
    """
    try:
        logger.info(
            "Starting OEE trends update",
            task_id=self.request.id,
            production_line_id=production_line_id
        )
        
        # Calculate trends for different time periods
        trend_periods = ["hourly", "daily", "weekly"]
        updated_trends = 0
        
        for period in trend_periods:
            try:
                # Calculate trend for this period
                trend_data = asyncio.run(_calculate_trend_for_period(
                    production_line_id, period
                ))
                
                # Store trend data
                asyncio.run(_store_oee_trend(production_line_id, period, trend_data))
                
                # Update cache
                cache_key = f"oee_trend:{production_line_id}:{period}"
                cache_service.set(cache_key, trend_data, ttl=1800)  # 30 minutes
                
                updated_trends += 1
                
                logger.info(
                    "OEE trend updated",
                    line_id=production_line_id,
                    period=period,
                    trend_direction=trend_data.get('trend_direction')
                )
                
            except Exception as e:
                logger.error(
                    "Failed to update OEE trend",
                    line_id=production_line_id,
                    period=period,
                    error=str(e),
                    exc_info=True
                )
        
        result = {
            "status": "success",
            "production_line_id": production_line_id,
            "updated_trends": updated_trends,
            "timestamp": datetime.utcnow().isoformat(),
            "task_id": self.request.id
        }
        
        logger.info("OEE trends update completed", **result)
        return result
        
    except Exception as exc:
        logger.error(
            "OEE trends update failed",
            production_line_id=production_line_id,
            error=str(exc),
            exc_info=True
        )
        raise self.retry(exc=exc, countdown=30, max_retries=3)


@celery_app.task(bind=True, name="app.tasks.oee_tasks.generate_oee_report")
def generate_oee_report(self, report_type: str, time_period: str, production_line_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Generate comprehensive OEE reports.
    
    Args:
        report_type: Type of report (summary, detailed, comparison)
        time_period: Time period for report (daily, weekly, monthly)
        production_line_id: Optional specific line ID for line-specific reports
        
    Returns:
        Dict containing report generation results
    """
    try:
        logger.info(
            "Starting OEE report generation",
            task_id=self.request.id,
            report_type=report_type,
            time_period=time_period,
            production_line_id=production_line_id
        )
        
        # Generate report based on type
        if report_type == "summary":
            report_data = asyncio.run(_generate_summary_report(time_period, production_line_id))
        elif report_type == "detailed":
            report_data = asyncio.run(_generate_detailed_report(time_period, production_line_id))
        elif report_type == "comparison":
            report_data = asyncio.run(_generate_comparison_report(time_period))
        else:
            raise ValueError(f"Invalid report type: {report_type}")
        
        # Store report
        report_id = asyncio.run(_store_oee_report(
            report_type, time_period, production_line_id, report_data
        ))
        
        # Trigger notification for report completion
        celery_app.send_task(
            "app.tasks.notification_tasks.send_report_notification",
            args=[report_id, report_type, time_period],
            queue="notifications"
        )
        
        result = {
            "status": "success",
            "report_id": report_id,
            "report_type": report_type,
            "time_period": time_period,
            "production_line_id": production_line_id,
            "timestamp": datetime.utcnow().isoformat(),
            "task_id": self.request.id
        }
        
        logger.info("OEE report generation completed", **result)
        return result
        
    except Exception as exc:
        logger.error(
            "OEE report generation failed",
            report_type=report_type,
            time_period=time_period,
            production_line_id=production_line_id,
            error=str(exc),
            exc_info=True
        )
        raise self.retry(exc=exc, countdown=60, max_retries=3)


# Helper functions for OEE calculations and analytics

async def _get_active_production_lines() -> List[ProductionLine]:
    """Get all active production lines."""
    try:
        # This would query the database for active production lines
        # For now, return empty list as placeholder
        return []
    except Exception as e:
        logger.error("Failed to get active production lines", error=str(e))
        raise


async def _get_production_line(line_id: str) -> ProductionLine:
    """Get specific production line by ID."""
    try:
        # This would query the database for specific production line
        # For now, return None as placeholder
        return None
    except Exception as e:
        logger.error("Failed to get production line", line_id=line_id, error=str(e))
        raise


async def _calculate_oee_components(line: ProductionLine) -> Dict[str, Any]:
    """Calculate OEE components (Availability, Performance, Quality)."""
    try:
        # This would calculate actual OEE components from production data
        # For now, return placeholder values
        return {
            "availability": 85.0,  # Percentage
            "performance": 92.0,   # Percentage
            "quality": 98.5,       # Percentage
            "oee_score": 77.1      # Calculated OEE score
        }
    except Exception as e:
        logger.error("Failed to calculate OEE components", line_id=line.id, error=str(e))
        raise


async def _store_oee_metrics(line_id: str, oee_data: Dict[str, Any], oee_score: float) -> None:
    """Store OEE metrics in database."""
    try:
        # This would store OEE metrics in the database
        pass
    except Exception as e:
        logger.error("Failed to store OEE metrics", line_id=line_id, error=str(e))
        raise


async def _calculate_detailed_oee_components(line: ProductionLine, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
    """Calculate detailed OEE components for a specific time period."""
    try:
        # This would calculate detailed OEE components with breakdowns
        # For now, return placeholder values
        return {
            "availability": {
                "total": 85.0,
                "planned_downtime": 5.0,
                "unplanned_downtime": 10.0,
                "breakdowns": 8.0,
                "changeovers": 2.0
            },
            "performance": {
                "total": 92.0,
                "speed_losses": 5.0,
                "minor_stoppages": 3.0
            },
            "quality": {
                "total": 98.5,
                "defects": 1.5,
                "rework": 0.8,
                "scrap": 0.7
            },
            "oee_score": 77.1
        }
    except Exception as e:
        logger.error("Failed to calculate detailed OEE components", line_id=line.id, error=str(e))
        raise


async def _calculate_oee_trends(line_id: str, time_period: str) -> Dict[str, Any]:
    """Calculate OEE trends for a production line."""
    try:
        # This would calculate OEE trends over time
        # For now, return placeholder values
        return {
            "trend_direction": "improving",
            "trend_percentage": 5.2,
            "volatility": "low",
            "consistency_score": 85.0
        }
    except Exception as e:
        logger.error("Failed to calculate OEE trends", line_id=line_id, error=str(e))
        raise


async def _generate_oee_recommendations(line_id: str, oee_components: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Generate OEE optimization recommendations."""
    try:
        recommendations = []
        
        # Analyze availability
        if oee_components.get('availability', {}).get('total', 0) < 90:
            recommendations.append({
                "category": "availability",
                "priority": "high",
                "recommendation": "Reduce unplanned downtime through preventive maintenance",
                "potential_improvement": "5-10%"
            })
        
        # Analyze performance
        if oee_components.get('performance', {}).get('total', 0) < 95:
            recommendations.append({
                "category": "performance",
                "priority": "medium",
                "recommendation": "Optimize machine speeds and reduce minor stoppages",
                "potential_improvement": "3-7%"
            })
        
        # Analyze quality
        if oee_components.get('quality', {}).get('total', 0) < 99:
            recommendations.append({
                "category": "quality",
                "priority": "medium",
                "recommendation": "Implement quality control improvements",
                "potential_improvement": "1-3%"
            })
        
        return recommendations
        
    except Exception as e:
        logger.error("Failed to generate OEE recommendations", line_id=line_id, error=str(e))
        return []


async def _store_detailed_oee_calculation(line_id: str, time_period: str, oee_components: Dict[str, Any], 
                                        trend_data: Dict[str, Any], recommendations: List[Dict[str, Any]]) -> None:
    """Store detailed OEE calculation results."""
    try:
        # This would store detailed OEE calculation in the database
        pass
    except Exception as e:
        logger.error("Failed to store detailed OEE calculation", line_id=line_id, error=str(e))
        raise


async def _calculate_line_analytics(line: ProductionLine, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
    """Calculate comprehensive analytics for a production line."""
    try:
        # This would calculate comprehensive line analytics
        # For now, return placeholder values
        return {
            "line_id": line.id,
            "line_name": line.name,
            "oee_score": 77.1,
            "availability": 85.0,
            "performance": 92.0,
            "quality": 98.5,
            "production_volume": 1250,
            "downtime_hours": 12.5,
            "defect_rate": 1.5
        }
    except Exception as e:
        logger.error("Failed to calculate line analytics", line_id=line.id, error=str(e))
        raise


def _identify_top_performers(line_performance: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Identify top performing production lines."""
    try:
        # Sort by OEE score and return top 3
        sorted_lines = sorted(line_performance, key=lambda x: x['oee_score'], reverse=True)
        return sorted_lines[:3]
    except Exception as e:
        logger.error("Failed to identify top performers", error=str(e))
        return []


def _identify_improvement_opportunities(line_performance: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Identify production lines with improvement opportunities."""
    try:
        # Find lines with OEE below 70%
        improvement_opportunities = [
            line for line in line_performance 
            if line['oee_score'] < 70
        ]
        
        # Sort by potential improvement (lowest OEE first)
        return sorted(improvement_opportunities, key=lambda x: x['oee_score'])
        
    except Exception as e:
        logger.error("Failed to identify improvement opportunities", error=str(e))
        return []


async def _calculate_downtime_analysis(start_time: datetime, end_time: datetime) -> Dict[str, Any]:
    """Calculate downtime analysis across all lines."""
    try:
        # This would calculate comprehensive downtime analysis
        # For now, return placeholder values
        return {
            "total_downtime_hours": 45.2,
            "planned_downtime_percentage": 60,
            "unplanned_downtime_percentage": 40,
            "top_downtime_causes": [
                {"cause": "Equipment failure", "hours": 15.5},
                {"cause": "Changeover", "hours": 12.3},
                {"cause": "Material shortage", "hours": 8.7}
            ]
        }
    except Exception as e:
        logger.error("Failed to calculate downtime analysis", error=str(e))
        raise


async def _calculate_quality_trends(start_time: datetime, end_time: datetime) -> Dict[str, Any]:
    """Calculate quality trends across all lines."""
    try:
        # This would calculate comprehensive quality trends
        # For now, return placeholder values
        return {
            "overall_quality_rate": 98.2,
            "defect_rate": 1.8,
            "rework_rate": 0.9,
            "scrap_rate": 0.9,
            "quality_trend": "stable"
        }
    except Exception as e:
        logger.error("Failed to calculate quality trends", error=str(e))
        raise


def _calculate_performance_benchmarks(line_performance: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate performance benchmarks."""
    try:
        if not line_performance:
            return {}
        
        oee_scores = [line['oee_score'] for line in line_performance]
        
        return {
            "average_oee": sum(oee_scores) / len(oee_scores),
            "best_oee": max(oee_scores),
            "worst_oee": min(oee_scores),
            "oee_standard_deviation": _calculate_standard_deviation(oee_scores)
        }
    except Exception as e:
        logger.error("Failed to calculate performance benchmarks", error=str(e))
        return {}


def _calculate_standard_deviation(values: List[float]) -> float:
    """Calculate standard deviation of a list of values."""
    try:
        if len(values) < 2:
            return 0.0
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
        return variance ** 0.5
    except Exception:
        return 0.0


async def _store_oee_analytics(time_period: str, analytics_data: Dict[str, Any]) -> None:
    """Store OEE analytics results."""
    try:
        # This would store OEE analytics in the database
        pass
    except Exception as e:
        logger.error("Failed to store OEE analytics", error=str(e))
        raise


async def _calculate_trend_for_period(line_id: str, period: str) -> Dict[str, Any]:
    """Calculate trend for a specific period."""
    try:
        # This would calculate trend for the specific period
        # For now, return placeholder values
        return {
            "trend_direction": "improving",
            "trend_percentage": 3.2,
            "volatility": "medium",
            "consistency_score": 78.5
        }
    except Exception as e:
        logger.error("Failed to calculate trend for period", line_id=line_id, period=period, error=str(e))
        raise


async def _store_oee_trend(line_id: str, period: str, trend_data: Dict[str, Any]) -> None:
    """Store OEE trend data."""
    try:
        # This would store OEE trend data in the database
        pass
    except Exception as e:
        logger.error("Failed to store OEE trend", line_id=line_id, period=period, error=str(e))
        raise


async def _generate_summary_report(time_period: str, production_line_id: Optional[str]) -> Dict[str, Any]:
    """Generate OEE summary report."""
    try:
        # This would generate a summary OEE report
        # For now, return placeholder values
        return {
            "report_type": "summary",
            "time_period": time_period,
            "overall_oee": 77.1,
            "lines_analyzed": 5,
            "summary": "OEE performance is within acceptable range with room for improvement"
        }
    except Exception as e:
        logger.error("Failed to generate summary report", error=str(e))
        raise


async def _generate_detailed_report(time_period: str, production_line_id: Optional[str]) -> Dict[str, Any]:
    """Generate detailed OEE report."""
    try:
        # This would generate a detailed OEE report
        # For now, return placeholder values
        return {
            "report_type": "detailed",
            "time_period": time_period,
            "production_line_id": production_line_id,
            "detailed_analysis": "Comprehensive OEE analysis with recommendations"
        }
    except Exception as e:
        logger.error("Failed to generate detailed report", error=str(e))
        raise


async def _generate_comparison_report(time_period: str) -> Dict[str, Any]:
    """Generate OEE comparison report."""
    try:
        # This would generate an OEE comparison report
        # For now, return placeholder values
        return {
            "report_type": "comparison",
            "time_period": time_period,
            "comparison_data": "Line-by-line OEE comparison with benchmarking"
        }
    except Exception as e:
        logger.error("Failed to generate comparison report", error=str(e))
        raise


async def _store_oee_report(report_type: str, time_period: str, production_line_id: Optional[str], report_data: Dict[str, Any]) -> str:
    """Store OEE report and return report ID."""
    try:
        # This would store the OEE report and return a report ID
        # For now, return a placeholder report ID
        return f"oee_report_{report_type}_{time_period}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    except Exception as e:
        logger.error("Failed to store OEE report", error=str(e))
        raise