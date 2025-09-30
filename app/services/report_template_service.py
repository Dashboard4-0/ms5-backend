"""
MS5.0 Floor Dashboard - Report Template Service

This module provides comprehensive report template management including
template creation, validation, and configuration management.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from uuid import UUID
import json
import structlog

from app.database import execute_query, execute_scalar, execute_update
from app.utils.exceptions import (
    NotFoundError, ValidationError, BusinessLogicError, ConflictError
)

logger = structlog.get_logger()


class ReportTemplateService:
    """Service for report template management."""
    
    @staticmethod
    async def create_template(
        name: str,
        description: str,
        template_type: str,
        parameters: List[Dict[str, Any]],
        sections: List[Dict[str, Any]],
        created_by: UUID
    ) -> Dict[str, Any]:
        """Create a new report template."""
        try:
            # Validate template data
            await ReportTemplateService._validate_template_data(
                name, description, template_type, parameters, sections
            )
            
            # Create template record
            create_query = """
            INSERT INTO factory_telemetry.report_templates 
            (name, description, template_type, parameters, sections, created_by, created_at)
            VALUES (:name, :description, :template_type, :parameters, :sections, :created_by, :created_at)
            RETURNING id, name, description, template_type, parameters, sections, created_at, created_by
            """
            
            result = await execute_query(create_query, {
                "name": name,
                "description": description,
                "template_type": template_type,
                "parameters": json.dumps(parameters),
                "sections": json.dumps(sections),
                "created_by": created_by,
                "created_at": datetime.utcnow()
            })
            
            if not result:
                raise BusinessLogicError("Failed to create template")
            
            template = result[0]
            
            # Parse JSON fields
            template["parameters"] = json.loads(template["parameters"])
            template["sections"] = json.loads(template["sections"])
            
            logger.info(
                "Report template created",
                template_id=template["id"],
                name=name,
                template_type=template_type,
                created_by=created_by
            )
            
            return template
            
        except Exception as e:
            logger.error("Failed to create template", error=str(e), name=name)
            raise BusinessLogicError("Failed to create template")
    
    @staticmethod
    async def get_template(template_id: UUID) -> Dict[str, Any]:
        """Get template by ID."""
        try:
            query = """
            SELECT id, name, description, template_type, parameters, sections, 
                   created_at, created_by, updated_at, updated_by
            FROM factory_telemetry.report_templates
            WHERE id = :template_id
            """
            
            result = await execute_query(query, {"template_id": template_id})
            
            if not result:
                raise NotFoundError("Report template", str(template_id))
            
            template = result[0]
            
            # Parse JSON fields
            template["parameters"] = json.loads(template["parameters"])
            template["sections"] = json.loads(template["sections"])
            
            return template
            
        except Exception as e:
            logger.error("Failed to get template", error=str(e), template_id=template_id)
            raise BusinessLogicError("Failed to get template")
    
    @staticmethod
    async def update_template(
        template_id: UUID,
        name: Optional[str] = None,
        description: Optional[str] = None,
        parameters: Optional[List[Dict[str, Any]]] = None,
        sections: Optional[List[Dict[str, Any]]] = None,
        updated_by: UUID = None
    ) -> Dict[str, Any]:
        """Update an existing template."""
        try:
            # Get existing template
            existing_template = await ReportTemplateService.get_template(template_id)
            
            # Prepare update data
            update_data = {}
            update_fields = []
            
            if name is not None:
                update_data["name"] = name
                update_fields.append("name = :name")
            
            if description is not None:
                update_data["description"] = description
                update_fields.append("description = :description")
            
            if parameters is not None:
                # Validate parameters
                await ReportTemplateService._validate_parameters(parameters)
                update_data["parameters"] = json.dumps(parameters)
                update_fields.append("parameters = :parameters")
            
            if sections is not None:
                # Validate sections
                await ReportTemplateService._validate_sections(sections)
                update_data["sections"] = json.dumps(sections)
                update_fields.append("sections = :sections")
            
            if updated_by is not None:
                update_data["updated_by"] = updated_by
                update_fields.append("updated_by = :updated_by")
            
            if not update_fields:
                raise ValidationError("No fields to update")
            
            # Add updated_at
            update_data["updated_at"] = datetime.utcnow()
            update_fields.append("updated_at = :updated_at")
            
            # Add template_id
            update_data["template_id"] = template_id
            
            # Build update query
            update_query = f"""
            UPDATE factory_telemetry.report_templates 
            SET {', '.join(update_fields)}
            WHERE id = :template_id
            RETURNING id, name, description, template_type, parameters, sections, 
                      created_at, created_by, updated_at, updated_by
            """
            
            result = await execute_query(update_query, update_data)
            
            if not result:
                raise BusinessLogicError("Failed to update template")
            
            template = result[0]
            
            # Parse JSON fields
            template["parameters"] = json.loads(template["parameters"])
            template["sections"] = json.loads(template["sections"])
            
            logger.info(
                "Report template updated",
                template_id=template_id,
                updated_fields=update_fields,
                updated_by=updated_by
            )
            
            return template
            
        except Exception as e:
            logger.error("Failed to update template", error=str(e), template_id=template_id)
            raise BusinessLogicError("Failed to update template")
    
    @staticmethod
    async def delete_template(template_id: UUID) -> None:
        """Delete a template."""
        try:
            # Check if template exists
            await ReportTemplateService.get_template(template_id)
            
            # Delete template
            delete_query = """
            DELETE FROM factory_telemetry.report_templates 
            WHERE id = :template_id
            """
            
            await execute_update(delete_query, {"template_id": template_id})
            
            logger.info("Report template deleted", template_id=template_id)
            
        except Exception as e:
            logger.error("Failed to delete template", error=str(e), template_id=template_id)
            raise BusinessLogicError("Failed to delete template")
    
    @staticmethod
    async def list_templates(
        template_type: Optional[str] = None,
        created_by: Optional[UUID] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """List templates with filtering."""
        try:
            where_conditions = []
            query_params = {"limit": limit, "offset": offset}
            
            if template_type:
                where_conditions.append("template_type = :template_type")
                query_params["template_type"] = template_type
            
            if created_by:
                where_conditions.append("created_by = :created_by")
                query_params["created_by"] = created_by
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"""
            SELECT id, name, description, template_type, parameters, sections, 
                   created_at, created_by, updated_at, updated_by
            FROM factory_telemetry.report_templates
            {where_clause}
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :offset
            """
            
            result = await execute_query(query, query_params)
            
            # Parse JSON fields
            for template in result:
                template["parameters"] = json.loads(template["parameters"])
                template["sections"] = json.loads(template["sections"])
            
            return result
            
        except Exception as e:
            logger.error("Failed to list templates", error=str(e))
            raise BusinessLogicError("Failed to list templates")
    
    @staticmethod
    async def get_templates_by_type(template_type: str) -> List[Dict[str, Any]]:
        """Get templates by type."""
        try:
            query = """
            SELECT id, name, description, template_type, parameters, sections, 
                   created_at, created_by, updated_at, updated_by
            FROM factory_telemetry.report_templates
            WHERE template_type = :template_type
            ORDER BY name ASC
            """
            
            result = await execute_query(query, {"template_type": template_type})
            
            # Parse JSON fields
            for template in result:
                template["parameters"] = json.loads(template["parameters"])
                template["sections"] = json.loads(template["sections"])
            
            return result
            
        except Exception as e:
            logger.error("Failed to get templates by type", error=str(e), template_type=template_type)
            raise BusinessLogicError("Failed to get templates by type")
    
    @staticmethod
    async def validate_template_parameters(
        template_id: UUID,
        parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate parameters against template requirements."""
        try:
            # Get template
            template = await ReportTemplateService.get_template(template_id)
            
            # Validate parameters
            validation_result = await ReportTemplateService._validate_parameters_against_template(
                template["parameters"], parameters
            )
            
            return validation_result
            
        except Exception as e:
            logger.error("Failed to validate template parameters", error=str(e), template_id=template_id)
            raise BusinessLogicError("Failed to validate template parameters")
    
    @staticmethod
    async def get_template_statistics() -> Dict[str, Any]:
        """Get template statistics."""
        try:
            # Get total templates
            total_query = "SELECT COUNT(*) as total FROM factory_telemetry.report_templates"
            total_result = await execute_scalar(total_query)
            total_templates = total_result or 0
            
            # Get templates by type
            type_query = """
            SELECT template_type, COUNT(*) as count
            FROM factory_telemetry.report_templates
            GROUP BY template_type
            ORDER BY count DESC
            """
            type_result = await execute_query(type_query)
            
            # Get recent templates
            recent_query = """
            SELECT COUNT(*) as recent
            FROM factory_telemetry.report_templates
            WHERE created_at >= NOW() - INTERVAL '30 days'
            """
            recent_result = await execute_scalar(recent_query)
            recent_templates = recent_result or 0
            
            return {
                "total_templates": total_templates,
                "recent_templates": recent_templates,
                "templates_by_type": {row["template_type"]: row["count"] for row in type_result}
            }
            
        except Exception as e:
            logger.error("Failed to get template statistics", error=str(e))
            raise BusinessLogicError("Failed to get template statistics")
    
    @staticmethod
    async def _validate_template_data(
        name: str,
        description: str,
        template_type: str,
        parameters: List[Dict[str, Any]],
        sections: List[Dict[str, Any]]
    ) -> None:
        """Validate template data."""
        if not name or not name.strip():
            raise ValidationError("Template name is required")
        
        if not description or not description.strip():
            raise ValidationError("Template description is required")
        
        if not template_type or not template_type.strip():
            raise ValidationError("Template type is required")
        
        # Validate parameters
        await ReportTemplateService._validate_parameters(parameters)
        
        # Validate sections
        await ReportTemplateService._validate_sections(sections)
    
    @staticmethod
    async def _validate_parameters(parameters: List[Dict[str, Any]]) -> None:
        """Validate parameters structure."""
        if not isinstance(parameters, list):
            raise ValidationError("Parameters must be a list")
        
        for i, param in enumerate(parameters):
            if not isinstance(param, dict):
                raise ValidationError(f"Parameter {i} must be a dictionary")
            
            required_fields = ["name", "type", "required"]
            for field in required_fields:
                if field not in param:
                    raise ValidationError(f"Parameter {i} missing required field: {field}")
            
            if not isinstance(param["name"], str) or not param["name"].strip():
                raise ValidationError(f"Parameter {i} name must be a non-empty string")
            
            if param["type"] not in ["string", "number", "date", "uuid", "boolean", "array", "object"]:
                raise ValidationError(f"Parameter {i} has invalid type: {param['type']}")
            
            if not isinstance(param["required"], bool):
                raise ValidationError(f"Parameter {i} required field must be boolean")
    
    @staticmethod
    async def _validate_sections(sections: List[Dict[str, Any]]) -> None:
        """Validate sections structure."""
        if not isinstance(sections, list):
            raise ValidationError("Sections must be a list")
        
        for i, section in enumerate(sections):
            if not isinstance(section, dict):
                raise ValidationError(f"Section {i} must be a dictionary")
            
            if "type" not in section:
                raise ValidationError(f"Section {i} missing required field: type")
            
            if section["type"] not in ["header", "summary", "table", "text", "oee", "downtime", "production", "quality", "equipment"]:
                raise ValidationError(f"Section {i} has invalid type: {section['type']}")
    
    @staticmethod
    async def _validate_parameters_against_template(
        template_parameters: List[Dict[str, Any]],
        provided_parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate provided parameters against template requirements."""
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "missing_required": [],
            "invalid_types": [],
            "extra_parameters": []
        }
        
        # Check required parameters
        for param in template_parameters:
            if param["required"] and param["name"] not in provided_parameters:
                validation_result["missing_required"].append(param["name"])
                validation_result["valid"] = False
        
        # Check parameter types
        for param in template_parameters:
            if param["name"] in provided_parameters:
                value = provided_parameters[param["name"]]
                if not ReportTemplateService._validate_parameter_type(value, param["type"]):
                    validation_result["invalid_types"].append({
                        "parameter": param["name"],
                        "expected_type": param["type"],
                        "actual_type": type(value).__name__
                    })
                    validation_result["valid"] = False
        
        # Check for extra parameters
        template_param_names = {param["name"] for param in template_parameters}
        for param_name in provided_parameters:
            if param_name not in template_param_names:
                validation_result["extra_parameters"].append(param_name)
                validation_result["warnings"].append(f"Extra parameter: {param_name}")
        
        return validation_result
    
    @staticmethod
    def _validate_parameter_type(value: Any, expected_type: str) -> bool:
        """Validate parameter value type."""
        if expected_type == "string":
            return isinstance(value, str)
        elif expected_type == "number":
            return isinstance(value, (int, float))
        elif expected_type == "date":
            return isinstance(value, (str, datetime, date))
        elif expected_type == "uuid":
            return isinstance(value, (str, UUID))
        elif expected_type == "boolean":
            return isinstance(value, bool)
        elif expected_type == "array":
            return isinstance(value, list)
        elif expected_type == "object":
            return isinstance(value, dict)
        else:
            return False
