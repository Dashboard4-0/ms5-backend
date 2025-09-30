"""
MS5.0 Floor Dashboard - Checklist Service

This module contains the business logic for checklist management,
including template management and checklist completion workflows.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from uuid import UUID
import structlog

from app.database import execute_query, execute_scalar, execute_update, get_db_session
from app.models.production import (
    ChecklistTemplateCreate, ChecklistTemplateUpdate, ChecklistTemplateResponse,
    ChecklistCompletionCreate, ChecklistCompletionResponse,
    JobAssignmentResponse
)
from app.utils.exceptions import (
    NotFoundError, ValidationError, ConflictError, BusinessLogicError
)
from app.services.job_assignment_service import JobAssignmentService

logger = structlog.get_logger()


class ChecklistService:
    """Service for checklist management and completion workflows."""
    
    def __init__(self):
        self.job_assignment_service = JobAssignmentService()
    
    async def create_checklist_template(
        self, 
        template_data: ChecklistTemplateCreate
    ) -> ChecklistTemplateResponse:
        """Create a new checklist template."""
        try:
            # Validate checklist items structure
            self._validate_checklist_items(template_data.checklist_items)
            
            # Create template
            create_query = """
            INSERT INTO factory_telemetry.checklist_templates 
            (name, equipment_codes, checklist_items, enabled)
            VALUES (:name, :equipment_codes, :checklist_items, :enabled)
            RETURNING id, name, equipment_codes, checklist_items, enabled, created_at
            """
            
            result = await execute_query(create_query, {
                "name": template_data.name,
                "equipment_codes": template_data.equipment_codes,
                "checklist_items": template_data.checklist_items,
                "enabled": template_data.enabled
            })
            
            if not result:
                raise BusinessLogicError("Failed to create checklist template")
            
            template = result[0]
            
            logger.info("Checklist template created", template_id=template["id"], name=template_data.name)
            
            return ChecklistTemplateResponse(
                id=template["id"],
                name=template["name"],
                equipment_codes=template["equipment_codes"],
                checklist_items=template["checklist_items"],
                enabled=template["enabled"],
                created_at=template["created_at"]
            )
            
        except (ValidationError, BusinessLogicError):
            raise
        except Exception as e:
            logger.error("Failed to create checklist template", error=str(e), name=template_data.name)
            raise BusinessLogicError("Failed to create checklist template")
    
    async def get_checklist_template(self, template_id: UUID) -> ChecklistTemplateResponse:
        """Get a checklist template by ID."""
        try:
            query = """
            SELECT id, name, equipment_codes, checklist_items, enabled, created_at
            FROM factory_telemetry.checklist_templates 
            WHERE id = :template_id
            """
            
            result = await execute_query(query, {"template_id": template_id})
            
            if not result:
                raise NotFoundError("Checklist template", str(template_id))
            
            template = result[0]
            
            return ChecklistTemplateResponse(
                id=template["id"],
                name=template["name"],
                equipment_codes=template["equipment_codes"],
                checklist_items=template["checklist_items"],
                enabled=template["enabled"],
                created_at=template["created_at"]
            )
            
        except NotFoundError:
            raise
        except Exception as e:
            logger.error("Failed to get checklist template", error=str(e), template_id=template_id)
            raise BusinessLogicError("Failed to get checklist template")
    
    async def update_checklist_template(
        self, 
        template_id: UUID, 
        update_data: ChecklistTemplateUpdate
    ) -> ChecklistTemplateResponse:
        """Update a checklist template."""
        try:
            # Check if template exists
            existing = await self.get_checklist_template(template_id)
            
            # Build update query dynamically
            update_fields = []
            update_values = {"template_id": template_id}
            
            if update_data.name is not None:
                update_fields.append("name = :name")
                update_values["name"] = update_data.name
            
            if update_data.equipment_codes is not None:
                update_fields.append("equipment_codes = :equipment_codes")
                update_values["equipment_codes"] = update_data.equipment_codes
            
            if update_data.checklist_items is not None:
                # Validate checklist items structure
                self._validate_checklist_items(update_data.checklist_items)
                update_fields.append("checklist_items = :checklist_items")
                update_values["checklist_items"] = update_data.checklist_items
            
            if update_data.enabled is not None:
                update_fields.append("enabled = :enabled")
                update_values["enabled"] = update_data.enabled
            
            if not update_fields:
                return existing
            
            update_query = f"""
            UPDATE factory_telemetry.checklist_templates 
            SET {', '.join(update_fields)}, updated_at = NOW()
            WHERE id = :template_id
            """
            
            await execute_update(update_query, update_values)
            
            logger.info("Checklist template updated", template_id=template_id)
            
            # Return updated template
            return await self.get_checklist_template(template_id)
            
        except (NotFoundError, ValidationError):
            raise
        except Exception as e:
            logger.error("Failed to update checklist template", error=str(e), template_id=template_id)
            raise BusinessLogicError("Failed to update checklist template")
    
    async def list_checklist_templates(
        self,
        equipment_codes: Optional[List[str]] = None,
        enabled_only: bool = True,
        skip: int = 0,
        limit: int = 100
    ) -> List[ChecklistTemplateResponse]:
        """List checklist templates with filters."""
        try:
            where_conditions = []
            query_params = {"skip": skip, "limit": limit}
            
            if enabled_only:
                where_conditions.append("enabled = true")
            
            if equipment_codes:
                # Find templates that match any of the equipment codes
                equipment_condition = " OR ".join([
                    f"equipment_codes @> :equipment_{i}" 
                    for i in range(len(equipment_codes))
                ])
                where_conditions.append(f"({equipment_condition})")
                
                for i, code in enumerate(equipment_codes):
                    query_params[f"equipment_{i}"] = [code]
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"""
            SELECT id, name, equipment_codes, checklist_items, enabled, created_at
            FROM factory_telemetry.checklist_templates 
            {where_clause}
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :skip
            """
            
            result = await execute_query(query, query_params)
            
            templates = []
            for template in result:
                templates.append(ChecklistTemplateResponse(
                    id=template["id"],
                    name=template["name"],
                    equipment_codes=template["equipment_codes"],
                    checklist_items=template["checklist_items"],
                    enabled=template["enabled"],
                    created_at=template["created_at"]
                ))
            
            return templates
            
        except Exception as e:
            logger.error("Failed to list checklist templates", error=str(e))
            raise BusinessLogicError("Failed to list checklist templates")
    
    async def get_checklist_template_for_equipment(
        self, 
        equipment_codes: List[str]
    ) -> Optional[ChecklistTemplateResponse]:
        """Get the most appropriate checklist template for given equipment codes."""
        try:
            # Find templates that match the equipment codes
            equipment_condition = " OR ".join([
                f"equipment_codes @> :equipment_{i}" 
                for i in range(len(equipment_codes))
            ])
            
            query_params = {}
            for i, code in enumerate(equipment_codes):
                query_params[f"equipment_{i}"] = [code]
            
            query = f"""
            SELECT id, name, equipment_codes, checklist_items, enabled, created_at
            FROM factory_telemetry.checklist_templates 
            WHERE enabled = true AND ({equipment_condition})
            ORDER BY array_length(equipment_codes, 1) DESC, created_at DESC
            LIMIT 1
            """
            
            result = await execute_query(query, query_params)
            
            if not result:
                return None
            
            template = result[0]
            
            return ChecklistTemplateResponse(
                id=template["id"],
                name=template["name"],
                equipment_codes=template["equipment_codes"],
                checklist_items=template["checklist_items"],
                enabled=template["enabled"],
                created_at=template["created_at"]
            )
            
        except Exception as e:
            logger.error("Failed to get checklist template for equipment", error=str(e), equipment_codes=equipment_codes)
            raise BusinessLogicError("Failed to get checklist template for equipment")
    
    async def complete_checklist(
        self, 
        completion_data: ChecklistCompletionCreate,
        user_id: UUID
    ) -> ChecklistCompletionResponse:
        """Complete a pre-start checklist."""
        try:
            # Get job assignment details
            job_assignment = await self.job_assignment_service.get_job_assignment(completion_data.job_assignment_id)
            
            # Validate user authorization
            if job_assignment.user_id != user_id:
                raise ValidationError("User not authorized for this job assignment")
            
            # Get checklist template
            template = await self.get_checklist_template(completion_data.template_id)
            
            # Validate checklist responses
            self._validate_checklist_responses(template.checklist_items, completion_data.responses)
            
            # Create completion record
            create_query = """
            INSERT INTO factory_telemetry.checklist_completions 
            (job_assignment_id, template_id, completed_by, completed_at, responses, signature_data, status)
            VALUES (:job_assignment_id, :template_id, :completed_by, NOW(), :responses, :signature_data, :status)
            RETURNING id, job_assignment_id, template_id, completed_by, completed_at, 
                     responses, signature_data, status
            """
            
            result = await execute_query(create_query, {
                "job_assignment_id": completion_data.job_assignment_id,
                "template_id": completion_data.template_id,
                "completed_by": user_id,
                "responses": completion_data.responses,
                "signature_data": completion_data.signature_data,
                "status": "completed"
            })
            
            if not result:
                raise BusinessLogicError("Failed to create checklist completion")
            
            completion = result[0]
            
            # Update job assignment status to ready to start
            await self._update_job_assignment_status(completion_data.job_assignment_id, "ready_to_start")
            
            logger.info(
                "Checklist completed", 
                completion_id=completion["id"], 
                job_assignment_id=completion_data.job_assignment_id,
                user_id=user_id
            )
            
            return ChecklistCompletionResponse(
                id=completion["id"],
                job_assignment_id=completion["job_assignment_id"],
                template_id=completion["template_id"],
                completed_by=completion["completed_by"],
                completed_at=completion["completed_at"],
                responses=completion["responses"],
                signature_data=completion["signature_data"],
                status=completion["status"]
            )
            
        except (NotFoundError, ValidationError, BusinessLogicError):
            raise
        except Exception as e:
            logger.error("Failed to complete checklist", error=str(e), job_assignment_id=completion_data.job_assignment_id)
            raise BusinessLogicError("Failed to complete checklist")
    
    async def get_checklist_completion(
        self, 
        completion_id: UUID
    ) -> ChecklistCompletionResponse:
        """Get a checklist completion by ID."""
        try:
            query = """
            SELECT id, job_assignment_id, template_id, completed_by, completed_at, 
                   responses, signature_data, status
            FROM factory_telemetry.checklist_completions 
            WHERE id = :completion_id
            """
            
            result = await execute_query(query, {"completion_id": completion_id})
            
            if not result:
                raise NotFoundError("Checklist completion", str(completion_id))
            
            completion = result[0]
            
            return ChecklistCompletionResponse(
                id=completion["id"],
                job_assignment_id=completion["job_assignment_id"],
                template_id=completion["template_id"],
                completed_by=completion["completed_by"],
                completed_at=completion["completed_at"],
                responses=completion["responses"],
                signature_data=completion["signature_data"],
                status=completion["status"]
            )
            
        except NotFoundError:
            raise
        except Exception as e:
            logger.error("Failed to get checklist completion", error=str(e), completion_id=completion_id)
            raise BusinessLogicError("Failed to get checklist completion")
    
    async def list_checklist_completions(
        self,
        job_assignment_id: Optional[UUID] = None,
        user_id: Optional[UUID] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[ChecklistCompletionResponse]:
        """List checklist completions with filters."""
        try:
            where_conditions = []
            query_params = {"skip": skip, "limit": limit}
            
            if job_assignment_id:
                where_conditions.append("job_assignment_id = :job_assignment_id")
                query_params["job_assignment_id"] = job_assignment_id
            
            if user_id:
                where_conditions.append("completed_by = :user_id")
                query_params["user_id"] = user_id
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"""
            SELECT id, job_assignment_id, template_id, completed_by, completed_at, 
                   responses, signature_data, status
            FROM factory_telemetry.checklist_completions 
            {where_clause}
            ORDER BY completed_at DESC
            LIMIT :limit OFFSET :skip
            """
            
            result = await execute_query(query, query_params)
            
            completions = []
            for completion in result:
                completions.append(ChecklistCompletionResponse(
                    id=completion["id"],
                    job_assignment_id=completion["job_assignment_id"],
                    template_id=completion["template_id"],
                    completed_by=completion["completed_by"],
                    completed_at=completion["completed_at"],
                    responses=completion["responses"],
                    signature_data=completion["signature_data"],
                    status=completion["status"]
                ))
            
            return completions
            
        except Exception as e:
            logger.error("Failed to list checklist completions", error=str(e))
            raise BusinessLogicError("Failed to list checklist completions")
    
    def _validate_checklist_items(self, checklist_items: List[Dict[str, Any]]) -> None:
        """Validate checklist items structure."""
        if not checklist_items:
            raise ValidationError("Checklist items cannot be empty")
        
        for i, item in enumerate(checklist_items):
            if not isinstance(item, dict):
                raise ValidationError(f"Checklist item {i} must be a dictionary")
            
            required_fields = ["id", "text", "required", "type"]
            for field in required_fields:
                if field not in item:
                    raise ValidationError(f"Checklist item {i} missing required field: {field}")
            
            if not isinstance(item["id"], str) or not item["id"].strip():
                raise ValidationError(f"Checklist item {i} must have a valid ID")
            
            if not isinstance(item["text"], str) or not item["text"].strip():
                raise ValidationError(f"Checklist item {i} must have valid text")
            
            if not isinstance(item["required"], bool):
                raise ValidationError(f"Checklist item {i} 'required' field must be boolean")
            
            if item["type"] not in ["checkbox", "text", "number", "select", "signature"]:
                raise ValidationError(f"Checklist item {i} has invalid type: {item['type']}")
            
            # Validate select options if type is select
            if item["type"] == "select":
                if "options" not in item or not isinstance(item["options"], list):
                    raise ValidationError(f"Checklist item {i} of type 'select' must have options")
                
                if not item["options"]:
                    raise ValidationError(f"Checklist item {i} select options cannot be empty")
    
    def _validate_checklist_responses(
        self, 
        checklist_items: List[Dict[str, Any]], 
        responses: Dict[str, Any]
    ) -> None:
        """Validate checklist responses against template."""
        if not responses:
            raise ValidationError("Checklist responses cannot be empty")
        
        # Check that all required items are completed
        for item in checklist_items:
            item_id = item["id"]
            
            if item["required"] and item_id not in responses:
                raise ValidationError(f"Required checklist item '{item['text']}' not completed")
            
            if item_id in responses:
                response = responses[item_id]
                
                # Validate response based on item type
                if item["type"] == "checkbox":
                    if not isinstance(response, bool):
                        raise ValidationError(f"Checklist item '{item['text']}' must be boolean")
                
                elif item["type"] == "text":
                    if not isinstance(response, str):
                        raise ValidationError(f"Checklist item '{item['text']}' must be text")
                
                elif item["type"] == "number":
                    if not isinstance(response, (int, float)):
                        raise ValidationError(f"Checklist item '{item['text']}' must be a number")
                
                elif item["type"] == "select":
                    if response not in item["options"]:
                        raise ValidationError(f"Checklist item '{item['text']}' has invalid selection")
                
                elif item["type"] == "signature":
                    if not isinstance(response, dict) or "signature" not in response:
                        raise ValidationError(f"Checklist item '{item['text']}' must have signature data")
    
    async def _update_job_assignment_status(
        self, 
        job_assignment_id: UUID, 
        status: str
    ) -> None:
        """Update job assignment status."""
        try:
            update_query = """
            UPDATE factory_telemetry.job_assignments 
            SET status = :status, updated_at = NOW()
            WHERE id = :job_assignment_id
            """
            
            await execute_update(update_query, {
                "job_assignment_id": job_assignment_id,
                "status": status
            })
            
        except Exception as e:
            logger.error("Failed to update job assignment status", error=str(e), job_assignment_id=job_assignment_id)
            raise BusinessLogicError("Failed to update job assignment status")
