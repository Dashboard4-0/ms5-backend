"""
MS5.0 Floor Dashboard - Report Generator Service

This module provides comprehensive report generation capabilities including
PDF generation, data aggregation, and report template management.
"""

import os
import json
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple
from uuid import UUID
import structlog

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.platypus import Image as RLImage
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader

from app.database import execute_query, execute_scalar
from app.utils.exceptions import (
    NotFoundError, ValidationError, BusinessLogicError, ConflictError
)

logger = structlog.get_logger()


class ReportGenerator:
    """Service for generating production reports and PDFs."""
    
    def __init__(self):
        self.reports_directory = "reports"
        self.templates_directory = "report_templates"
        self.ensure_directories()
        self.styles = self.setup_styles()
    
    def ensure_directories(self) -> None:
        """Ensure report directories exist."""
        os.makedirs(self.reports_directory, exist_ok=True)
        os.makedirs(self.templates_directory, exist_ok=True)
    
    def setup_styles(self) -> Dict[str, Any]:
        """Setup report styles."""
        styles = getSampleStyleSheet()
        
        # Custom styles
        styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            spaceAfter=30,
            alignment=TA_CENTER,
            textColor=colors.darkblue
        ))
        
        styles.add(ParagraphStyle(
            name='CustomHeading',
            parent=styles['Heading2'],
            fontSize=16,
            spaceAfter=12,
            textColor=colors.darkblue
        ))
        
        styles.add(ParagraphStyle(
            name='CustomSubHeading',
            parent=styles['Heading3'],
            fontSize=14,
            spaceAfter=8,
            textColor=colors.darkgreen
        ))
        
        styles.add(ParagraphStyle(
            name='CustomBody',
            parent=styles['Normal'],
            fontSize=10,
            spaceAfter=6
        ))
        
        styles.add(ParagraphStyle(
            name='CustomFooter',
            parent=styles['Normal'],
            fontSize=8,
            alignment=TA_CENTER,
            textColor=colors.grey
        ))
        
        return styles
    
    async def generate_production_report(
        self,
        line_id: UUID,
        report_date: date,
        shift: Optional[str] = None,
        report_type: str = "daily"
    ) -> Dict[str, Any]:
        """Generate comprehensive production report."""
        try:
            # Get production data
            production_data = await self.get_production_data(line_id, report_date, shift)
            
            # Generate PDF
            pdf_filename = await self.create_production_pdf(
                line_id, report_date, shift, production_data, report_type
            )
            
            # Store report metadata
            report_id = await self.store_report_metadata(
                line_id, report_date, shift, report_type, pdf_filename, production_data
            )
            
            logger.info(
                "Production report generated",
                report_id=report_id,
                line_id=line_id,
                report_date=report_date,
                shift=shift,
                report_type=report_type
            )
            
            return {
                "report_id": report_id,
                "filename": pdf_filename,
                "file_path": f"{self.reports_directory}/{pdf_filename}",
                "generated_at": datetime.utcnow(),
                "data": production_data
            }
            
        except Exception as e:
            logger.error("Failed to generate production report", error=str(e), line_id=line_id)
            raise BusinessLogicError("Failed to generate production report")
    
    async def generate_oee_report(
        self,
        line_id: UUID,
        start_date: date,
        end_date: date,
        report_type: str = "oee_analysis"
    ) -> Dict[str, Any]:
        """Generate OEE analysis report."""
        try:
            # Get OEE data
            oee_data = await self.get_oee_data(line_id, start_date, end_date)
            
            # Generate PDF
            pdf_filename = await self.create_oee_pdf(
                line_id, start_date, end_date, oee_data, report_type
            )
            
            # Store report metadata
            report_id = await self.store_report_metadata(
                line_id, start_date, None, report_type, pdf_filename, oee_data
            )
            
            logger.info(
                "OEE report generated",
                report_id=report_id,
                line_id=line_id,
                start_date=start_date,
                end_date=end_date
            )
            
            return {
                "report_id": report_id,
                "filename": pdf_filename,
                "file_path": f"{self.reports_directory}/{pdf_filename}",
                "generated_at": datetime.utcnow(),
                "data": oee_data
            }
            
        except Exception as e:
            logger.error("Failed to generate OEE report", error=str(e), line_id=line_id)
            raise BusinessLogicError("Failed to generate OEE report")
    
    async def generate_downtime_report(
        self,
        line_id: UUID,
        start_date: date,
        end_date: date,
        report_type: str = "downtime_analysis"
    ) -> Dict[str, Any]:
        """Generate downtime analysis report."""
        try:
            # Get downtime data
            downtime_data = await self.get_downtime_data(line_id, start_date, end_date)
            
            # Generate PDF
            pdf_filename = await self.create_downtime_pdf(
                line_id, start_date, end_date, downtime_data, report_type
            )
            
            # Store report metadata
            report_id = await self.store_report_metadata(
                line_id, start_date, None, report_type, pdf_filename, downtime_data
            )
            
            logger.info(
                "Downtime report generated",
                report_id=report_id,
                line_id=line_id,
                start_date=start_date,
                end_date=end_date
            )
            
            return {
                "report_id": report_id,
                "filename": pdf_filename,
                "file_path": f"{self.reports_directory}/{pdf_filename}",
                "generated_at": datetime.utcnow(),
                "data": downtime_data
            }
            
        except Exception as e:
            logger.error("Failed to generate downtime report", error=str(e), line_id=line_id)
            raise BusinessLogicError("Failed to generate downtime report")
    
    async def generate_custom_report(
        self,
        template_id: UUID,
        parameters: Dict[str, Any],
        user_id: UUID
    ) -> Dict[str, Any]:
        """Generate custom report from template."""
        try:
            # Get template
            template = await self.get_report_template(template_id)
            
            # Get data based on template
            report_data = await self.get_custom_report_data(template, parameters)
            
            # Generate PDF
            pdf_filename = await self.create_custom_pdf(
                template, parameters, report_data, user_id
            )
            
            # Store report metadata
            report_id = await self.store_report_metadata(
                None, date.today(), None, "custom", pdf_filename, report_data, user_id
            )
            
            logger.info(
                "Custom report generated",
                report_id=report_id,
                template_id=template_id,
                user_id=user_id
            )
            
            return {
                "report_id": report_id,
                "filename": pdf_filename,
                "file_path": f"{self.reports_directory}/{pdf_filename}",
                "generated_at": datetime.utcnow(),
                "data": report_data
            }
            
        except Exception as e:
            logger.error("Failed to generate custom report", error=str(e), template_id=template_id)
            raise BusinessLogicError("Failed to generate custom report")
    
    async def create_production_pdf(
        self,
        line_id: UUID,
        report_date: date,
        shift: Optional[str],
        data: Dict[str, Any],
        report_type: str
    ) -> str:
        """Create production report PDF."""
        filename = f"production_{line_id}_{report_date}_{report_type}.pdf"
        filepath = os.path.join(self.reports_directory, filename)
        
        doc = SimpleDocTemplate(filepath, pagesize=A4)
        story = []
        
        # Add header
        story.extend(self.create_report_header(line_id, report_date, shift, "Production Report"))
        
        # Add summary section
        story.extend(self.create_summary_section(data))
        
        # Add OEE section
        story.extend(self.create_oee_section(data.get("oee", {})))
        
        # Add downtime section
        story.extend(self.create_downtime_section(data.get("downtime", {})))
        
        # Add production details
        story.extend(self.create_production_details(data.get("production", {})))
        
        # Add quality section
        story.extend(self.create_quality_section(data.get("quality", {})))
        
        # Add equipment status
        story.extend(self.create_equipment_status_section(data.get("equipment", {})))
        
        # Add footer
        story.extend(self.create_report_footer())
        
        # Build PDF
        doc.build(story)
        
        return filename
    
    async def create_oee_pdf(
        self,
        line_id: UUID,
        start_date: date,
        end_date: date,
        data: Dict[str, Any],
        report_type: str
    ) -> str:
        """Create OEE analysis PDF."""
        filename = f"oee_{line_id}_{start_date}_{end_date}.pdf"
        filepath = os.path.join(self.reports_directory, filename)
        
        doc = SimpleDocTemplate(filepath, pagesize=A4)
        story = []
        
        # Add header
        story.extend(self.create_report_header(line_id, start_date, None, "OEE Analysis Report"))
        
        # Add OEE overview
        story.extend(self.create_oee_overview_section(data))
        
        # Add OEE trends
        story.extend(self.create_oee_trends_section(data))
        
        # Add performance analysis
        story.extend(self.create_performance_analysis_section(data))
        
        # Add recommendations
        story.extend(self.create_recommendations_section(data))
        
        # Add footer
        story.extend(self.create_report_footer())
        
        # Build PDF
        doc.build(story)
        
        return filename
    
    async def create_downtime_pdf(
        self,
        line_id: UUID,
        start_date: date,
        end_date: date,
        data: Dict[str, Any],
        report_type: str
    ) -> str:
        """Create downtime analysis PDF."""
        filename = f"downtime_{line_id}_{start_date}_{end_date}.pdf"
        filepath = os.path.join(self.reports_directory, filename)
        
        doc = SimpleDocTemplate(filepath, pagesize=A4)
        story = []
        
        # Add header
        story.extend(self.create_report_header(line_id, start_date, None, "Downtime Analysis Report"))
        
        # Add downtime overview
        story.extend(self.create_downtime_overview_section(data))
        
        # Add downtime breakdown
        story.extend(self.create_downtime_breakdown_section(data))
        
        # Add top reasons
        story.extend(self.create_top_reasons_section(data))
        
        # Add equipment analysis
        story.extend(self.create_equipment_downtime_section(data))
        
        # Add recommendations
        story.extend(self.create_downtime_recommendations_section(data))
        
        # Add footer
        story.extend(self.create_report_footer())
        
        # Build PDF
        doc.build(story)
        
        return filename
    
    async def create_custom_pdf(
        self,
        template: Dict[str, Any],
        parameters: Dict[str, Any],
        data: Dict[str, Any],
        user_id: UUID
    ) -> str:
        """Create custom report PDF from template."""
        filename = f"custom_{template['id']}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.pdf"
        filepath = os.path.join(self.reports_directory, filename)
        
        doc = SimpleDocTemplate(filepath, pagesize=A4)
        story = []
        
        # Add header
        story.extend(self.create_custom_report_header(template, parameters))
        
        # Add sections based on template
        for section in template.get("sections", []):
            story.extend(self.create_custom_section(section, data))
        
        # Add footer
        story.extend(self.create_report_footer())
        
        # Build PDF
        doc.build(story)
        
        return filename
    
    def create_report_header(
        self,
        line_id: UUID,
        report_date: date,
        shift: Optional[str],
        title: str
    ) -> List[Any]:
        """Create report header section."""
        elements = []
        
        # Title
        elements.append(Paragraph(title, self.styles['CustomTitle']))
        
        # Report info
        info_data = [
            ['Report Date:', report_date.strftime('%Y-%m-%d')],
            ['Generated At:', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')],
        ]
        
        if line_id:
            info_data.append(['Production Line:', str(line_id)])
        
        if shift:
            info_data.append(['Shift:', shift])
        
        info_table = Table(info_data, colWidths=[2*inch, 3*inch])
        info_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('ALIGN', (1, 0), (1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        
        elements.append(info_table)
        elements.append(Spacer(1, 20))
        
        return elements
    
    def create_summary_section(self, data: Dict[str, Any]) -> List[Any]:
        """Create summary section."""
        elements = []
        
        elements.append(Paragraph("Executive Summary", self.styles['CustomHeading']))
        
        # Summary metrics
        summary_data = [
            ['Metric', 'Value', 'Target', 'Status'],
            ['Total Production', f"{data.get('total_production', 0):,}", 
             f"{data.get('target_production', 0):,}", 
             self.get_status_indicator(data.get('total_production', 0), data.get('target_production', 0))],
            ['OEE', f"{data.get('oee', {}).get('oee', 0):.1%}", 
             f"{data.get('oee', {}).get('target_oee', 0):.1%}", 
             self.get_status_indicator(data.get('oee', {}).get('oee', 0), data.get('oee', {}).get('target_oee', 0))],
            ['Downtime Hours', f"{data.get('downtime', {}).get('total_hours', 0):.1f}", 
             f"{data.get('downtime', {}).get('target_hours', 0):.1f}", 
             self.get_status_indicator(data.get('downtime', {}).get('total_hours', 0), data.get('downtime', {}).get('target_hours', 0), reverse=True)],
            ['Quality Rate', f"{data.get('quality', {}).get('rate', 0):.1%}", 
             f"{data.get('quality', {}).get('target_rate', 0):.1%}", 
             self.get_status_indicator(data.get('quality', {}).get('rate', 0), data.get('quality', {}).get('target_rate', 0))],
        ]
        
        summary_table = Table(summary_data, colWidths=[2*inch, 1.5*inch, 1.5*inch, 1*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        elements.append(summary_table)
        elements.append(Spacer(1, 20))
        
        return elements
    
    def create_oee_section(self, oee_data: Dict[str, Any]) -> List[Any]:
        """Create OEE section."""
        elements = []
        
        elements.append(Paragraph("Overall Equipment Effectiveness (OEE)", self.styles['CustomHeading']))
        
        # OEE table
        oee_table_data = [
            ['Metric', 'Value', 'Target', 'Status'],
            ['Availability', f"{oee_data.get('availability', 0):.1%}", 
             f"{oee_data.get('target_availability', 0):.1%}", 
             self.get_status_indicator(oee_data.get('availability', 0), oee_data.get('target_availability', 0))],
            ['Performance', f"{oee_data.get('performance', 0):.1%}", 
             f"{oee_data.get('target_performance', 0):.1%}", 
             self.get_status_indicator(oee_data.get('performance', 0), oee_data.get('target_performance', 0))],
            ['Quality', f"{oee_data.get('quality', 0):.1%}", 
             f"{oee_data.get('target_quality', 0):.1%}", 
             self.get_status_indicator(oee_data.get('quality', 0), oee_data.get('target_quality', 0))],
            ['OEE', f"{oee_data.get('oee', 0):.1%}", 
             f"{oee_data.get('target_oee', 0):.1%}", 
             self.get_status_indicator(oee_data.get('oee', 0), oee_data.get('target_oee', 0))],
        ]
        
        oee_table = Table(oee_table_data, colWidths=[2*inch, 1.5*inch, 1.5*inch, 1*inch])
        oee_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        elements.append(oee_table)
        elements.append(Spacer(1, 20))
        
        return elements
    
    def create_downtime_section(self, downtime_data: Dict[str, Any]) -> List[Any]:
        """Create downtime section."""
        elements = []
        
        elements.append(Paragraph("Downtime Analysis", self.styles['CustomHeading']))
        
        # Downtime summary
        downtime_summary = [
            ['Category', 'Duration (Hours)', 'Percentage', 'Events'],
            ['Planned', f"{downtime_data.get('planned_hours', 0):.1f}", 
             f"{downtime_data.get('planned_percentage', 0):.1%}", 
             f"{downtime_data.get('planned_events', 0)}"],
            ['Unplanned', f"{downtime_data.get('unplanned_hours', 0):.1f}", 
             f"{downtime_data.get('unplanned_percentage', 0):.1%}", 
             f"{downtime_data.get('unplanned_events', 0)}"],
            ['Maintenance', f"{downtime_data.get('maintenance_hours', 0):.1f}", 
             f"{downtime_data.get('maintenance_percentage', 0):.1%}", 
             f"{downtime_data.get('maintenance_events', 0)}"],
            ['Changeover', f"{downtime_data.get('changeover_hours', 0):.1f}", 
             f"{downtime_data.get('changeover_percentage', 0):.1%}", 
             f"{downtime_data.get('changeover_events', 0)}"],
            ['Total', f"{downtime_data.get('total_hours', 0):.1f}", 
             f"{downtime_data.get('total_percentage', 0):.1%}", 
             f"{downtime_data.get('total_events', 0)}"],
        ]
        
        downtime_table = Table(downtime_summary, colWidths=[2*inch, 1.5*inch, 1.5*inch, 1*inch])
        downtime_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        elements.append(downtime_table)
        elements.append(Spacer(1, 20))
        
        return elements
    
    def create_production_details(self, production_data: Dict[str, Any]) -> List[Any]:
        """Create production details section."""
        elements = []
        
        elements.append(Paragraph("Production Details", self.styles['CustomHeading']))
        
        # Production details table
        details_data = [
            ['Time Period', 'Production', 'Target', 'Efficiency'],
            ['Hour 1', f"{production_data.get('hour_1', 0):,}", 
             f"{production_data.get('target_hour_1', 0):,}", 
             f"{production_data.get('efficiency_hour_1', 0):.1%}"],
            ['Hour 2', f"{production_data.get('hour_2', 0):,}", 
             f"{production_data.get('target_hour_2', 0):,}", 
             f"{production_data.get('efficiency_hour_2', 0):.1%}"],
            # Add more hours as needed
        ]
        
        details_table = Table(details_data, colWidths=[1.5*inch, 1.5*inch, 1.5*inch, 1.5*inch])
        details_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        elements.append(details_table)
        elements.append(Spacer(1, 20))
        
        return elements
    
    def create_quality_section(self, quality_data: Dict[str, Any]) -> List[Any]:
        """Create quality section."""
        elements = []
        
        elements.append(Paragraph("Quality Analysis", self.styles['CustomHeading']))
        
        # Quality metrics
        quality_metrics = [
            ['Metric', 'Value', 'Target', 'Status'],
            ['Quality Rate', f"{quality_data.get('rate', 0):.1%}", 
             f"{quality_data.get('target_rate', 0):.1%}", 
             self.get_status_indicator(quality_data.get('rate', 0), quality_data.get('target_rate', 0))],
            ['Defects', f"{quality_data.get('defects', 0):,}", 
             f"{quality_data.get('target_defects', 0):,}", 
             self.get_status_indicator(quality_data.get('defects', 0), quality_data.get('target_defects', 0), reverse=True)],
            ['Rework', f"{quality_data.get('rework', 0):,}", 
             f"{quality_data.get('target_rework', 0):,}", 
             self.get_status_indicator(quality_data.get('rework', 0), quality_data.get('target_rework', 0), reverse=True)],
        ]
        
        quality_table = Table(quality_metrics, colWidths=[2*inch, 1.5*inch, 1.5*inch, 1*inch])
        quality_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        elements.append(quality_table)
        elements.append(Spacer(1, 20))
        
        return elements
    
    def create_equipment_status_section(self, equipment_data: Dict[str, Any]) -> List[Any]:
        """Create equipment status section."""
        elements = []
        
        elements.append(Paragraph("Equipment Status", self.styles['CustomHeading']))
        
        # Equipment status table
        equipment_list = equipment_data.get('equipment', [])
        if equipment_list:
            status_data = [['Equipment', 'Status', 'Uptime', 'Last Maintenance']]
            
            for equipment in equipment_list:
                status_data.append([
                    equipment.get('code', 'Unknown'),
                    equipment.get('status', 'Unknown'),
                    f"{equipment.get('uptime', 0):.1%}",
                    equipment.get('last_maintenance', 'Never')
                ])
            
            status_table = Table(status_data, colWidths=[2*inch, 1.5*inch, 1.5*inch, 1.5*inch])
            status_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            
            elements.append(status_table)
        else:
            elements.append(Paragraph("No equipment data available", self.styles['CustomBody']))
        
        elements.append(Spacer(1, 20))
        
        return elements
    
    def create_report_footer(self) -> List[Any]:
        """Create report footer."""
        elements = []
        
        elements.append(Spacer(1, 20))
        elements.append(Paragraph(
            f"Generated by MS5.0 Floor Dashboard on {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
            self.styles['CustomFooter']
        ))
        
        return elements
    
    def get_status_indicator(self, actual: float, target: float, reverse: bool = False) -> str:
        """Get status indicator for metrics."""
        if target == 0:
            return "N/A"
        
        if reverse:
            # For metrics where lower is better (like defects, downtime)
            if actual <= target:
                return "✓ Good"
            elif actual <= target * 1.1:
                return "⚠ Warning"
            else:
                return "✗ Poor"
        else:
            # For metrics where higher is better (like OEE, quality)
            if actual >= target:
                return "✓ Good"
            elif actual >= target * 0.9:
                return "⚠ Warning"
            else:
                return "✗ Poor"
    
    # Data retrieval methods
    async def get_production_data(self, line_id: UUID, report_date: date, shift: Optional[str]) -> Dict[str, Any]:
        """Get production data for report."""
        # Implementation would query database for production data
        # This is a placeholder implementation
        return {
            "total_production": 1000,
            "target_production": 1200,
            "oee": {
                "availability": 0.85,
                "performance": 0.90,
                "quality": 0.95,
                "oee": 0.73,
                "target_availability": 0.90,
                "target_performance": 0.95,
                "target_quality": 0.98,
                "target_oee": 0.85
            },
            "downtime": {
                "total_hours": 2.5,
                "target_hours": 1.0,
                "planned_hours": 1.0,
                "unplanned_hours": 1.5,
                "maintenance_hours": 0.5,
                "changeover_hours": 0.5
            },
            "quality": {
                "rate": 0.95,
                "target_rate": 0.98,
                "defects": 50,
                "target_defects": 20,
                "rework": 10,
                "target_rework": 5
            },
            "equipment": {
                "equipment": [
                    {"code": "EQ001", "status": "Running", "uptime": 0.95, "last_maintenance": "2025-01-15"},
                    {"code": "EQ002", "status": "Maintenance", "uptime": 0.90, "last_maintenance": "2025-01-20"}
                ]
            }
        }
    
    async def get_oee_data(self, line_id: UUID, start_date: date, end_date: date) -> Dict[str, Any]:
        """Get OEE data for report."""
        # Implementation would query database for OEE data
        return {}
    
    async def get_downtime_data(self, line_id: UUID, start_date: date, end_date: date) -> Dict[str, Any]:
        """Get downtime data for report."""
        # Implementation would query database for downtime data
        return {}
    
    async def get_custom_report_data(self, template: Dict[str, Any], parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Get custom report data based on template."""
        # Implementation would query database based on template configuration
        return {}
    
    async def get_report_template(self, template_id: UUID) -> Dict[str, Any]:
        """Get report template by ID."""
        # Implementation would query database for template
        return {}
    
    async def store_report_metadata(
        self,
        line_id: Optional[UUID],
        report_date: date,
        shift: Optional[str],
        report_type: str,
        filename: str,
        data: Dict[str, Any],
        user_id: Optional[UUID] = None
    ) -> UUID:
        """Store report metadata in database."""
        # Implementation would store report metadata in database
        # This is a placeholder implementation
        return UUID("12345678-1234-5678-9012-123456789012")
    
    # Additional helper methods for custom report sections
    def create_custom_report_header(self, template: Dict[str, Any], parameters: Dict[str, Any]) -> List[Any]:
        """Create custom report header."""
        elements = []
        
        title = template.get('title', 'Custom Report')
        elements.append(Paragraph(title, self.styles['CustomTitle']))
        
        # Add parameters
        if parameters:
            param_data = [['Parameter', 'Value']]
            for key, value in parameters.items():
                param_data.append([key, str(value)])
            
            param_table = Table(param_data, colWidths=[2*inch, 3*inch])
            param_table.setStyle(TableStyle([
                ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
                ('ALIGN', (1, 0), (1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ]))
            
            elements.append(param_table)
        
        elements.append(Spacer(1, 20))
        
        return elements
    
    def create_custom_section(self, section: Dict[str, Any], data: Dict[str, Any]) -> List[Any]:
        """Create custom report section."""
        elements = []
        
        section_type = section.get('type', 'text')
        section_title = section.get('title', 'Section')
        
        elements.append(Paragraph(section_title, self.styles['CustomHeading']))
        
        if section_type == 'table':
            # Create table section
            table_data = section.get('data', [])
            if table_data:
                table = Table(table_data, colWidths=section.get('col_widths', [2*inch] * len(table_data[0])))
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black)
                ]))
                elements.append(table)
        
        elif section_type == 'text':
            # Create text section
            text_content = section.get('content', 'No content available')
            elements.append(Paragraph(text_content, self.styles['CustomBody']))
        
        elements.append(Spacer(1, 20))
        
        return elements
    
    # Additional helper methods for OEE and downtime reports
    def create_oee_overview_section(self, data: Dict[str, Any]) -> List[Any]:
        """Create OEE overview section."""
        elements = []
        
        elements.append(Paragraph("OEE Overview", self.styles['CustomHeading']))
        
        # OEE overview content
        overview_text = f"""
        This report covers the OEE analysis for the specified period. 
        The data shows the overall equipment effectiveness metrics including 
        availability, performance, and quality factors.
        """
        
        elements.append(Paragraph(overview_text, self.styles['CustomBody']))
        elements.append(Spacer(1, 20))
        
        return elements
    
    def create_oee_trends_section(self, data: Dict[str, Any]) -> List[Any]:
        """Create OEE trends section."""
        elements = []
        
        elements.append(Paragraph("OEE Trends", self.styles['CustomHeading']))
        
        # OEE trends content
        trends_text = f"""
        The OEE trends show the performance over time. This section would 
        typically include charts and graphs showing the progression of 
        availability, performance, and quality metrics.
        """
        
        elements.append(Paragraph(trends_text, self.styles['CustomBody']))
        elements.append(Spacer(1, 20))
        
        return elements
    
    def create_performance_analysis_section(self, data: Dict[str, Any]) -> List[Any]:
        """Create performance analysis section."""
        elements = []
        
        elements.append(Paragraph("Performance Analysis", self.styles['CustomHeading']))
        
        # Performance analysis content
        analysis_text = f"""
        The performance analysis provides insights into the factors affecting 
        OEE. This includes breakdowns of availability losses, performance 
        losses, and quality losses.
        """
        
        elements.append(Paragraph(analysis_text, self.styles['CustomBody']))
        elements.append(Spacer(1, 20))
        
        return elements
    
    def create_recommendations_section(self, data: Dict[str, Any]) -> List[Any]:
        """Create recommendations section."""
        elements = []
        
        elements.append(Paragraph("Recommendations", self.styles['CustomHeading']))
        
        # Recommendations content
        recommendations_text = f"""
        Based on the analysis, the following recommendations are made:
        
        1. Focus on improving availability through better maintenance scheduling
        2. Address performance issues by optimizing production processes
        3. Implement quality control measures to reduce defects
        4. Monitor equipment health more closely to prevent unplanned downtime
        """
        
        elements.append(Paragraph(recommendations_text, self.styles['CustomBody']))
        elements.append(Spacer(1, 20))
        
        return elements
    
    def create_downtime_overview_section(self, data: Dict[str, Any]) -> List[Any]:
        """Create downtime overview section."""
        elements = []
        
        elements.append(Paragraph("Downtime Overview", self.styles['CustomHeading']))
        
        # Downtime overview content
        overview_text = f"""
        This report provides a comprehensive analysis of downtime events 
        for the specified period. The data includes breakdowns by category, 
        equipment, and root causes.
        """
        
        elements.append(Paragraph(overview_text, self.styles['CustomBody']))
        elements.append(Spacer(1, 20))
        
        return elements
    
    def create_downtime_breakdown_section(self, data: Dict[str, Any]) -> List[Any]:
        """Create downtime breakdown section."""
        elements = []
        
        elements.append(Paragraph("Downtime Breakdown", self.styles['CustomHeading']))
        
        # Downtime breakdown content
        breakdown_text = f"""
        The downtime breakdown shows the distribution of downtime events 
        across different categories and equipment. This helps identify 
        the most significant sources of production losses.
        """
        
        elements.append(Paragraph(breakdown_text, self.styles['CustomBody']))
        elements.append(Spacer(1, 20))
        
        return elements
    
    def create_top_reasons_section(self, data: Dict[str, Any]) -> List[Any]:
        """Create top reasons section."""
        elements = []
        
        elements.append(Paragraph("Top Downtime Reasons", self.styles['CustomHeading']))
        
        # Top reasons content
        reasons_text = f"""
        The top downtime reasons are identified based on frequency and 
        duration. This information is crucial for prioritizing improvement 
        efforts and resource allocation.
        """
        
        elements.append(Paragraph(reasons_text, self.styles['CustomBody']))
        elements.append(Spacer(1, 20))
        
        return elements
    
    def create_equipment_downtime_section(self, data: Dict[str, Any]) -> List[Any]:
        """Create equipment downtime section."""
        elements = []
        
        elements.append(Paragraph("Equipment Downtime Analysis", self.styles['CustomHeading']))
        
        # Equipment downtime content
        equipment_text = f"""
        The equipment downtime analysis shows which equipment contributes 
        most to downtime. This helps in identifying equipment that requires 
        attention or replacement.
        """
        
        elements.append(Paragraph(equipment_text, self.styles['CustomBody']))
        elements.append(Spacer(1, 20))
        
        return elements
    
    def create_downtime_recommendations_section(self, data: Dict[str, Any]) -> List[Any]:
        """Create downtime recommendations section."""
        elements = []
        
        elements.append(Paragraph("Downtime Reduction Recommendations", self.styles['CustomHeading']))
        
        # Downtime recommendations content
        recommendations_text = f"""
        Based on the downtime analysis, the following recommendations are made:
        
        1. Implement predictive maintenance for critical equipment
        2. Improve changeover procedures to reduce setup time
        3. Address root causes of frequent downtime events
        4. Train operators on proper equipment operation and maintenance
        5. Implement real-time monitoring and alerting systems
        """
        
        elements.append(Paragraph(recommendations_text, self.styles['CustomBody']))
        elements.append(Spacer(1, 20))
        
        return elements
    
    # Phase 2 Enhancement - Advanced Report Analytics and Intelligence
    
    async def generate_intelligent_production_insights(
        self,
        line_id: UUID,
        analysis_period_days: int = 30,
        insight_categories: List[str] = None
    ) -> Dict[str, Any]:
        """
        Generate intelligent production insights using advanced analytics.
        
        This method provides comprehensive analysis and actionable insights
        for production optimization using machine learning and pattern recognition.
        """
        try:
            if insight_categories is None:
                insight_categories = ["performance", "efficiency", "quality", "maintenance", "optimization"]
            
            logger.info("Generating intelligent production insights", 
                       line_id=line_id, categories=insight_categories)
            
            # Get comprehensive production data
            production_data = await self._get_comprehensive_production_data(
                line_id, analysis_period_days
            )
            
            insights = {}
            
            # Generate insights for each category
            for category in insight_categories:
                category_insights = await self._generate_category_production_insights(
                    category, production_data
                )
                insights[category] = category_insights
            
            # Generate cross-category insights
            cross_category_insights = await self._generate_cross_category_production_insights(
                insights, production_data
            )
            
            # Rank insights by impact and priority
            ranked_insights = await self._rank_production_insights_by_impact(
                insights, cross_category_insights
            )
            
            # Generate actionable recommendations
            actionable_recommendations = await self._generate_actionable_production_recommendations(
                ranked_insights, production_data
            )
            
            # Generate executive summary
            executive_summary = await self._generate_executive_summary(
                insights, actionable_recommendations
            )
            
            result = {
                "line_id": line_id,
                "analysis_period_days": analysis_period_days,
                "analysis_timestamp": datetime.utcnow(),
                "insight_categories": insight_categories,
                "category_insights": insights,
                "cross_category_insights": cross_category_insights,
                "ranked_insights": ranked_insights,
                "actionable_recommendations": actionable_recommendations,
                "executive_summary": executive_summary,
                "insights_summary": {
                    "total_insights": sum(len(cat_insights) for cat_insights in insights.values()),
                    "high_priority_insights": len([i for i in ranked_insights if i.get("priority") == "high"]),
                    "optimization_potential": actionable_recommendations.get("total_optimization_potential", 0),
                    "expected_improvement": actionable_recommendations.get("expected_improvement_percentage", 0)
                }
            }
            
            logger.info("Intelligent production insights generated", 
                       line_id=line_id, insights_count=result["insights_summary"]["total_insights"])
            
            return result
            
        except Exception as e:
            logger.error("Failed to generate intelligent production insights", 
                        error=str(e), line_id=line_id)
            raise BusinessLogicError("Failed to generate intelligent production insights")
    
    # Private helper methods for advanced report analytics
    
    async def _get_comprehensive_production_data(
        self, line_id: UUID, period_days: int
    ) -> Dict[str, Any]:
        """Get comprehensive production data for analysis."""
        try:
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=period_days)
            
            # Get production schedules data
            schedules_query = """
            SELECT 
                COUNT(*) as total_schedules,
                SUM(target_quantity) as total_target,
                SUM(CASE WHEN status = 'completed' THEN target_quantity ELSE 0 END) as completed_quantity,
                AVG(EXTRACT(EPOCH FROM (scheduled_end - scheduled_start))/3600) as avg_duration_hours
            FROM factory_telemetry.production_schedules
            WHERE line_id = :line_id
            AND created_at >= :start_date
            AND created_at <= :end_date
            """
            
            schedules_result = await execute_query(schedules_query, {
                "line_id": line_id,
                "start_date": start_date,
                "end_date": end_date
            })
            
            # Get OEE data
            oee_query = """
            SELECT 
                AVG(oee) as avg_oee,
                AVG(availability) as avg_availability,
                AVG(performance) as avg_performance,
                AVG(quality) as avg_quality
            FROM factory_telemetry.oee_calculations
            WHERE line_id = :line_id
            AND calculation_time >= :start_date
            AND calculation_time <= :end_date
            """
            
            oee_result = await execute_query(oee_query, {
                "line_id": line_id,
                "start_date": start_date,
                "end_date": end_date
            })
            
            # Get Andon events data
            andon_query = """
            SELECT 
                COUNT(*) as total_events,
                COUNT(CASE WHEN status = 'resolved' THEN 1 END) as resolved_events,
                AVG(EXTRACT(EPOCH FROM (resolved_at - reported_at))/60) as avg_resolution_minutes
            FROM factory_telemetry.andon_events
            WHERE line_id = :line_id
            AND reported_at >= :start_date
            AND reported_at <= :end_date
            """
            
            andon_result = await execute_query(andon_query, {
                "line_id": line_id,
                "start_date": start_date,
                "end_date": end_date
            })
            
            return {
                "period": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "days": period_days
                },
                "schedules": schedules_result[0] if schedules_result else {},
                "oee": oee_result[0] if oee_result else {},
                "andon": andon_result[0] if andon_result else {}
            }
            
        except Exception as e:
            logger.error("Failed to get comprehensive production data", error=str(e))
            return {}
    
    async def _generate_category_production_insights(
        self, category: str, production_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Generate category-specific production insights."""
        try:
            insights = []
            
            if category == "performance":
                # Performance insights
                schedules = production_data.get("schedules", {})
                total_schedules = schedules.get("total_schedules", 0)
                completed_quantity = schedules.get("completed_quantity", 0)
                total_target = schedules.get("total_target", 0)
                
                if total_target > 0:
                    efficiency = completed_quantity / total_target
                    if efficiency < 0.8:
                        insights.append({
                            "type": "performance",
                            "title": "Low Production Efficiency",
                            "description": f"Production efficiency is {efficiency:.1%}, below optimal levels.",
                            "impact_score": 0.8,
                            "confidence": 0.9,
                            "priority": "high",
                            "recommended_actions": [
                                "Review production processes",
                                "Optimize equipment settings",
                                "Improve material flow"
                            ],
                            "expected_improvement": 15.0
                        })
            
            elif category == "quality":
                # Quality insights
                oee = production_data.get("oee", {})
                avg_quality = oee.get("avg_quality", 0)
                
                if avg_quality < 0.99:
                    insights.append({
                        "type": "quality",
                        "title": "Quality Rate Below Target",
                        "description": f"Quality rate is {avg_quality:.1%}, below 99% target.",
                        "impact_score": 0.7,
                        "confidence": 0.8,
                        "priority": "high",
                        "recommended_actions": [
                            "Implement quality control checkpoints",
                            "Improve operator training",
                            "Review process parameters"
                        ],
                        "expected_improvement": 5.0
                    })
            
            return insights
            
        except Exception as e:
            logger.error("Failed to generate category production insights", error=str(e))
            return []
    
    async def _generate_cross_category_production_insights(
        self, insights: Dict[str, Any], production_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Generate cross-category production insights."""
        try:
            cross_insights = []
            
            # Analyze correlations between different categories
            performance_insights = insights.get("performance", [])
            quality_insights = insights.get("quality", [])
            
            if performance_insights and quality_insights:
                cross_insights.append({
                    "type": "cross_category",
                    "title": "Performance-Quality Correlation",
                    "description": "Both performance and quality metrics show improvement opportunities.",
                    "impact_score": 0.9,
                    "confidence": 0.8,
                    "priority": "high",
                    "recommended_actions": [
                        "Implement integrated improvement program",
                        "Focus on root cause analysis",
                        "Develop comprehensive training program"
                    ],
                    "expected_improvement": 20.0
                })
            
            return cross_insights
            
        except Exception as e:
            logger.error("Failed to generate cross-category production insights", error=str(e))
            return []
    
    async def _rank_production_insights_by_impact(
        self, insights: Dict[str, Any], cross_category_insights: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Rank production insights by impact score."""
        try:
            all_insights = []
            
            # Collect all insights
            for category_insights in insights.values():
                all_insights.extend(category_insights)
            
            all_insights.extend(cross_category_insights)
            
            # Sort by impact score (descending)
            ranked_insights = sorted(all_insights, key=lambda x: x.get("impact_score", 0), reverse=True)
            
            return ranked_insights
            
        except Exception as e:
            logger.error("Failed to rank production insights by impact", error=str(e))
            return []
    
    async def _generate_actionable_production_recommendations(
        self, ranked_insights: List[Dict[str, Any]], production_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate actionable production recommendations."""
        try:
            recommendations = []
            total_optimization_potential = 0
            
            for insight in ranked_insights:
                if insight.get("priority") == "high":
                    recommendations.append({
                        "insight_id": insight.get("title", "unknown"),
                        "priority": insight.get("priority", "medium"),
                        "recommended_actions": insight.get("recommended_actions", []),
                        "expected_improvement": insight.get("expected_improvement", 0),
                        "implementation_effort": "medium",
                        "timeline": "2-4 weeks"
                    })
                    total_optimization_potential += insight.get("expected_improvement", 0)
            
            return {
                "recommendations": recommendations,
                "total_optimization_potential": total_optimization_potential,
                "expected_improvement_percentage": min(100, total_optimization_potential),
                "implementation_roadmap": [
                    {"phase": 1, "description": "High priority improvements", "timeline": "2-4 weeks"},
                    {"phase": 2, "description": "Medium priority improvements", "timeline": "4-8 weeks"},
                    {"phase": 3, "description": "Long-term optimizations", "timeline": "8-12 weeks"}
                ]
            }
            
        except Exception as e:
            logger.error("Failed to generate actionable production recommendations", error=str(e))
            return {"recommendations": [], "total_optimization_potential": 0}
    
    async def _generate_executive_summary(
        self, insights: Dict[str, Any], recommendations: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate executive summary."""
        try:
            total_insights = sum(len(cat_insights) for cat_insights in insights.values())
            high_priority_count = len([r for r in recommendations.get("recommendations", []) if r.get("priority") == "high"])
            
            return {
                "total_insights_generated": total_insights,
                "high_priority_recommendations": high_priority_count,
                "optimization_potential": recommendations.get("total_optimization_potential", 0),
                "key_findings": [
                    "Production efficiency below target",
                    "Quality rate needs improvement",
                    "Opportunities for process optimization"
                ],
                "recommended_next_steps": [
                    "Implement high-priority improvements",
                    "Develop comprehensive training program",
                    "Establish continuous improvement process"
                ]
            }
            
        except Exception as e:
            logger.error("Failed to generate executive summary", error=str(e))
            return {}