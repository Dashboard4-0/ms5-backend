"""
MS5.0 Floor Dashboard - File Upload API

This module provides file upload endpoints for images, documents, and other files
required by the MS5.0 Floor Dashboard application.
"""

import os
import uuid
from typing import List, Optional
from datetime import datetime

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, status
from fastapi.responses import JSONResponse
import structlog

from app.auth.permissions import get_current_user, require_permission
from app.config import settings
from app.utils.exceptions import ValidationError, NotFoundError

logger = structlog.get_logger()

router = APIRouter()


class FileUploadService:
    """Service for handling file uploads."""
    
    def __init__(self):
        self.upload_dir = settings.UPLOAD_DIRECTORY
        self.max_file_size = settings.MAX_FILE_SIZE
        self.allowed_file_types = settings.ALLOWED_FILE_TYPES
        self._ensure_upload_directory()
    
    def _ensure_upload_directory(self):
        """Ensure upload directory exists."""
        if not os.path.exists(self.upload_dir):
            os.makedirs(self.upload_dir, exist_ok=True)
            logger.info("Created upload directory", directory=self.upload_dir)
    
    def _validate_file_size(self, file_size: int) -> bool:
        """Validate file size against maximum allowed size."""
        return file_size <= self.max_file_size
    
    def _validate_file_type(self, content_type: str) -> bool:
        """Validate file type against allowed types."""
        return content_type in self.allowed_file_types
    
    def _generate_unique_filename(self, original_filename: str) -> str:
        """Generate a unique filename for uploaded file."""
        file_extension = os.path.splitext(original_filename)[1]
        unique_id = str(uuid.uuid4())
        return f"{unique_id}{file_extension}"
    
    async def upload_file(
        self,
        file: UploadFile,
        user_id: str,
        category: str = "general",
        description: Optional[str] = None
    ) -> dict:
        """Upload a file and return file information."""
        try:
            # Validate file size
            file_content = await file.read()
            file_size = len(file_content)
            
            if not self._validate_file_size(file_size):
                raise ValidationError(
                    f"File size ({file_size} bytes) exceeds maximum allowed size ({self.max_file_size} bytes)"
                )
            
            # Validate file type
            if not self._validate_file_type(file.content_type):
                raise ValidationError(
                    f"File type '{file.content_type}' is not allowed. Allowed types: {self.allowed_file_types}"
                )
            
            # Generate unique filename
            unique_filename = self._generate_unique_filename(file.filename)
            file_path = os.path.join(self.upload_dir, unique_filename)
            
            # Write file to disk
            with open(file_path, "wb") as f:
                f.write(file_content)
            
            # Create file record
            file_info = {
                "id": str(uuid.uuid4()),
                "original_filename": file.filename,
                "stored_filename": unique_filename,
                "file_path": file_path,
                "content_type": file.content_type,
                "file_size": file_size,
                "category": category,
                "description": description,
                "uploaded_by": user_id,
                "uploaded_at": datetime.utcnow(),
                "status": "active"
            }
            
            logger.info(
                "File uploaded successfully",
                file_id=file_info["id"],
                original_filename=file.filename,
                file_size=file_size,
                user_id=user_id
            )
            
            return file_info
            
        except Exception as e:
            logger.error("File upload failed", error=str(e), filename=file.filename, user_id=user_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="File upload failed"
            )
    
    async def upload_multiple_files(
        self,
        files: List[UploadFile],
        user_id: str,
        category: str = "general",
        description: Optional[str] = None
    ) -> List[dict]:
        """Upload multiple files and return file information list."""
        uploaded_files = []
        errors = []
        
        for file in files:
            try:
                file_info = await self.upload_file(file, user_id, category, description)
                uploaded_files.append(file_info)
            except Exception as e:
                errors.append({
                    "filename": file.filename,
                    "error": str(e)
                })
        
        if errors:
            logger.warning(
                "Some files failed to upload",
                uploaded_count=len(uploaded_files),
                error_count=len(errors),
                user_id=user_id
            )
        
        return {
            "uploaded_files": uploaded_files,
            "errors": errors,
            "total_uploaded": len(uploaded_files),
            "total_failed": len(errors)
        }


# Global file upload service
file_upload_service = FileUploadService()


@router.post("/", tags=["File Upload"])
async def upload_single_file(
    file: UploadFile = File(...),
    category: str = "general",
    description: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """Upload a single file."""
    try:
        file_info = await file_upload_service.upload_file(
            file=file,
            user_id=current_user["user_id"],
            category=category,
            description=description
        )
        
        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={
                "message": "File uploaded successfully",
                "file": {
                    "id": file_info["id"],
                    "original_filename": file_info["original_filename"],
                    "content_type": file_info["content_type"],
                    "file_size": file_info["file_size"],
                    "category": file_info["category"],
                    "description": file_info["description"],
                    "uploaded_at": file_info["uploaded_at"].isoformat(),
                    "download_url": f"/api/v1/upload/download/{file_info['id']}"
                }
            }
        )
        
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error("Upload endpoint error", error=str(e), user_id=current_user["user_id"])
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="File upload failed"
        )


@router.post("/multiple", tags=["File Upload"])
async def upload_multiple_files(
    files: List[UploadFile] = File(...),
    category: str = "general",
    description: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """Upload multiple files."""
    try:
        if len(files) > 10:  # Limit to 10 files per request
            raise ValidationError("Maximum 10 files allowed per upload request")
        
        result = await file_upload_service.upload_multiple_files(
            files=files,
            user_id=current_user["user_id"],
            category=category,
            description=description
        )
        
        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={
                "message": "Files processed",
                "total_uploaded": result["total_uploaded"],
                "total_failed": result["total_failed"],
                "uploaded_files": [
                    {
                        "id": file["id"],
                        "original_filename": file["original_filename"],
                        "content_type": file["content_type"],
                        "file_size": file["file_size"],
                        "category": file["category"],
                        "description": file["description"],
                        "uploaded_at": file["uploaded_at"].isoformat(),
                        "download_url": f"/api/v1/upload/download/{file['id']}"
                    }
                    for file in result["uploaded_files"]
                ],
                "errors": result["errors"]
            }
        )
        
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error("Multiple upload endpoint error", error=str(e), user_id=current_user["user_id"])
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Multiple file upload failed"
        )


@router.get("/download/{file_id}", tags=["File Upload"])
async def download_file(
    file_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Download a file by ID."""
    # This would typically involve database lookup and file serving
    # For now, return a placeholder response
    return JSONResponse(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        content={
            "message": "File download endpoint not yet implemented",
            "file_id": file_id
        }
    )


@router.get("/info", tags=["File Upload"])
async def get_upload_info(
    current_user: dict = Depends(get_current_user)
):
    """Get file upload configuration information."""
    return {
        "max_file_size": settings.MAX_FILE_SIZE,
        "max_file_size_mb": settings.MAX_FILE_SIZE / (1024 * 1024),
        "allowed_file_types": settings.ALLOWED_FILE_TYPES,
        "upload_directory": settings.UPLOAD_DIRECTORY
    }


@router.delete("/{file_id}", tags=["File Upload"])
async def delete_file(
    file_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Delete a file by ID."""
    # This would typically involve database lookup and file deletion
    # For now, return a placeholder response
    return JSONResponse(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        content={
            "message": "File deletion endpoint not yet implemented",
            "file_id": file_id
        }
    )
