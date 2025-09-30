"""
MS5.0 Floor Dashboard - Authentication API Routes

This module provides authentication endpoints for the MS5.0 Floor Dashboard API
including login, logout, token refresh, and user profile management.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Body
from fastapi.security import HTTPAuthorizationCredentials
import structlog

from app.auth.jwt_handler import (
    create_tokens, verify_access_token, verify_refresh_token,
    hash_password, verify_password, needs_password_refresh
)
from app.auth.permissions import get_current_user, UserContext, UserRole
from app.database import get_db, execute_query, execute_scalar, execute_update
from app.models.production import BaseProductionModel
from app.utils.exceptions import AuthenticationError, ValidationError, NotFoundError
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger()

router = APIRouter()


# Request/Response Models
class LoginRequest(BaseProductionModel):
    """Login request model."""
    username: str
    password: str
    remember_me: bool = False


class LoginResponse(BaseProductionModel):
    """Login response model."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    user: Dict[str, Any]


class RefreshTokenRequest(BaseProductionModel):
    """Refresh token request model."""
    refresh_token: str


class RefreshTokenResponse(BaseProductionModel):
    """Refresh token response model."""
    access_token: str
    token_type: str = "bearer"
    expires_in: int


class UserProfileResponse(BaseProductionModel):
    """User profile response model."""
    id: UUID
    username: str
    email: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    employee_id: Optional[str]
    role: str
    department: Optional[str]
    shift: Optional[str]
    skills: list
    certifications: list
    is_active: bool
    created_at: datetime
    last_login: Optional[datetime]


class UserProfileUpdate(BaseProductionModel):
    """User profile update model."""
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    department: Optional[str] = None
    shift: Optional[str] = None
    skills: Optional[list] = None
    certifications: Optional[list] = None


class ChangePasswordRequest(BaseProductionModel):
    """Change password request model."""
    current_password: str
    new_password: str
    confirm_password: str


class LogoutResponse(BaseProductionModel):
    """Logout response model."""
    message: str
    logged_out_at: datetime


# Authentication endpoints
@router.post("/login", response_model=LoginResponse, status_code=status.HTTP_200_OK)
async def login(
    login_data: LoginRequest,
    db: AsyncSession = Depends(get_db)
) -> LoginResponse:
    """Authenticate user and return access tokens."""
    try:
        # Get user from database
        user_query = """
        SELECT id, username, email, password_hash, first_name, last_name, 
               employee_id, role, department, shift, skills, certifications, 
               is_active, created_at, last_login
        FROM factory_telemetry.users 
        WHERE username = :username AND is_active = true
        """
        
        user_result = await execute_query(user_query, {"username": login_data.username})
        
        if not user_result:
            logger.warning("Login attempt with invalid username", username=login_data.username)
            raise AuthenticationError("Invalid username or password")
        
        user = user_result[0]
        
        # Verify password
        if not verify_password(login_data.password, user["password_hash"]):
            logger.warning("Login attempt with invalid password", username=login_data.username)
            raise AuthenticationError("Invalid username or password")
        
        # Check if password needs refresh
        if needs_password_refresh(user["password_hash"]):
            logger.info("Password needs refresh", user_id=user["id"])
            # In a real implementation, you might want to force password change
        
        # Update last login
        update_login_query = """
        UPDATE factory_telemetry.users 
        SET last_login = NOW() 
        WHERE id = :user_id
        """
        await execute_update(update_login_query, {"user_id": user["id"]})
        
        # Create tokens
        user_data = {
            "user_id": str(user["id"]),
            "username": user["username"],
            "role": user["role"],
            "department": user["department"],
            "shift": user["shift"],
            "is_active": user["is_active"]
        }
        
        tokens = create_tokens(str(user["id"]), user_data)
        
        # Prepare user profile
        user_profile = {
            "id": user["id"],
            "username": user["username"],
            "email": user["email"],
            "first_name": user["first_name"],
            "last_name": user["last_name"],
            "employee_id": user["employee_id"],
            "role": user["role"],
            "department": user["department"],
            "shift": user["shift"],
            "skills": user["skills"] or [],
            "certifications": user["certifications"] or [],
            "is_active": user["is_active"],
            "created_at": user["created_at"],
            "last_login": user["last_login"]
        }
        
        logger.info("User logged in successfully", user_id=user["id"], username=user["username"])
        
        return LoginResponse(
            access_token=tokens["access_token"],
            refresh_token=tokens["refresh_token"],
            token_type=tokens["token_type"],
            expires_in=tokens["expires_in"],
            user=user_profile
        )
        
    except AuthenticationError:
        raise
    except Exception as e:
        logger.error("Login failed", error=str(e), username=login_data.username)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Login failed"
        )


@router.post("/refresh", response_model=RefreshTokenResponse, status_code=status.HTTP_200_OK)
async def refresh_token(
    refresh_data: RefreshTokenRequest
) -> RefreshTokenResponse:
    """Refresh access token using refresh token."""
    try:
        # Verify refresh token
        payload = verify_refresh_token(refresh_data.refresh_token)
        user_id = payload.get("user_id")
        
        if not user_id:
            raise AuthenticationError("Invalid refresh token")
        
        # Get user data for new token
        user_query = """
        SELECT id, username, role, department, shift, is_active
        FROM factory_telemetry.users 
        WHERE id = :user_id AND is_active = true
        """
        
        user_result = await execute_query(user_query, {"user_id": user_id})
        
        if not user_result:
            raise AuthenticationError("User not found or inactive")
        
        user = user_result[0]
        
        # Create new access token
        user_data = {
            "user_id": str(user["id"]),
            "username": user["username"],
            "role": user["role"],
            "department": user["department"],
            "shift": user["shift"],
            "is_active": user["is_active"]
        }
        
        tokens = create_tokens(str(user["id"]), user_data)
        
        logger.info("Access token refreshed", user_id=user["id"])
        
        return RefreshTokenResponse(
            access_token=tokens["access_token"],
            token_type=tokens["token_type"],
            expires_in=tokens["expires_in"]
        )
        
    except AuthenticationError:
        raise
    except Exception as e:
        logger.error("Token refresh failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Token refresh failed"
        )


@router.post("/logout", response_model=LogoutResponse, status_code=status.HTTP_200_OK)
async def logout(
    current_user: UserContext = Depends(get_current_user)
) -> LogoutResponse:
    """Logout user (client should discard tokens)."""
    logger.info("User logged out", user_id=current_user.user_id)
    
    return LogoutResponse(
        message="Logged out successfully",
        logged_out_at=datetime.utcnow()
    )


@router.get("/profile", response_model=UserProfileResponse, status_code=status.HTTP_200_OK)
async def get_profile(
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> UserProfileResponse:
    """Get current user profile."""
    try:
        user_query = """
        SELECT id, username, email, first_name, last_name, employee_id, 
               role, department, shift, skills, certifications, is_active, 
               created_at, last_login
        FROM factory_telemetry.users 
        WHERE id = :user_id
        """
        
        user_result = await execute_query(user_query, {"user_id": current_user.user_id})
        
        if not user_result:
            raise NotFoundError("User", current_user.user_id)
        
        user = user_result[0]
        
        return UserProfileResponse(
            id=user["id"],
            username=user["username"],
            email=user["email"],
            first_name=user["first_name"],
            last_name=user["last_name"],
            employee_id=user["employee_id"],
            role=user["role"],
            department=user["department"],
            shift=user["shift"],
            skills=user["skills"] or [],
            certifications=user["certifications"] or [],
            is_active=user["is_active"],
            created_at=user["created_at"],
            last_login=user["last_login"]
        )
        
    except NotFoundError:
        raise
    except Exception as e:
        logger.error("Failed to get user profile", error=str(e), user_id=current_user.user_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get user profile"
        )


@router.put("/profile", response_model=UserProfileResponse, status_code=status.HTTP_200_OK)
async def update_profile(
    profile_data: UserProfileUpdate,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> UserProfileResponse:
    """Update current user profile."""
    try:
        # Build update query dynamically
        update_fields = []
        update_values = {"user_id": current_user.user_id}
        
        if profile_data.first_name is not None:
            update_fields.append("first_name = :first_name")
            update_values["first_name"] = profile_data.first_name
        
        if profile_data.last_name is not None:
            update_fields.append("last_name = :last_name")
            update_values["last_name"] = profile_data.last_name
        
        if profile_data.email is not None:
            update_fields.append("email = :email")
            update_values["email"] = profile_data.email
        
        if profile_data.department is not None:
            update_fields.append("department = :department")
            update_values["department"] = profile_data.department
        
        if profile_data.shift is not None:
            update_fields.append("shift = :shift")
            update_values["shift"] = profile_data.shift
        
        if profile_data.skills is not None:
            update_fields.append("skills = :skills")
            update_values["skills"] = profile_data.skills
        
        if profile_data.certifications is not None:
            update_fields.append("certifications = :certifications")
            update_values["certifications"] = profile_data.certifications
        
        if not update_fields:
            raise ValidationError("No fields to update")
        
        update_query = f"""
        UPDATE factory_telemetry.users 
        SET {', '.join(update_fields)}, updated_at = NOW()
        WHERE id = :user_id
        """
        
        await execute_update(update_query, update_values)
        
        # Get updated user profile
        return await get_profile(current_user, db)
        
    except ValidationError:
        raise
    except Exception as e:
        logger.error("Failed to update user profile", error=str(e), user_id=current_user.user_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update user profile"
        )


@router.post("/change-password", status_code=status.HTTP_200_OK)
async def change_password(
    password_data: ChangePasswordRequest,
    current_user: UserContext = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, str]:
    """Change user password."""
    try:
        # Validate new password
        if password_data.new_password != password_data.confirm_password:
            raise ValidationError("New password and confirmation do not match")
        
        if len(password_data.new_password) < 8:
            raise ValidationError("New password must be at least 8 characters long")
        
        # Get current password hash
        user_query = """
        SELECT password_hash FROM factory_telemetry.users 
        WHERE id = :user_id
        """
        
        user_result = await execute_query(user_query, {"user_id": current_user.user_id})
        
        if not user_result:
            raise NotFoundError("User", current_user.user_id)
        
        user = user_result[0]
        
        # Verify current password
        if not verify_password(password_data.current_password, user["password_hash"]):
            raise AuthenticationError("Current password is incorrect")
        
        # Hash new password
        new_password_hash = hash_password(password_data.new_password)
        
        # Update password
        update_query = """
        UPDATE factory_telemetry.users 
        SET password_hash = :new_password_hash, updated_at = NOW()
        WHERE id = :user_id
        """
        
        await execute_update(update_query, {
            "user_id": current_user.user_id,
            "new_password_hash": new_password_hash
        })
        
        logger.info("Password changed successfully", user_id=current_user.user_id)
        
        return {"message": "Password changed successfully"}
        
    except (ValidationError, AuthenticationError, NotFoundError):
        raise
    except Exception as e:
        logger.error("Failed to change password", error=str(e), user_id=current_user.user_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to change password"
        )


@router.get("/roles", status_code=status.HTTP_200_OK)
async def get_available_roles() -> Dict[str, Any]:
    """Get available user roles and their descriptions."""
    roles_info = {
        "admin": {
            "name": "Administrator",
            "description": "Full system access with all permissions",
            "permissions": ["All permissions"]
        },
        "production_manager": {
            "name": "Production Manager",
            "description": "Production management and oversight",
            "permissions": ["Production management", "Scheduling", "Analytics", "Reports"]
        },
        "shift_manager": {
            "name": "Shift Manager",
            "description": "Shift-level management and coordination",
            "permissions": ["Shift management", "Job assignment", "Andon handling"]
        },
        "engineer": {
            "name": "Engineer",
            "description": "Technical and maintenance operations",
            "permissions": ["Equipment management", "Maintenance", "Technical analysis"]
        },
        "operator": {
            "name": "Operator",
            "description": "Production line operations",
            "permissions": ["Job execution", "Quality checks", "Andon reporting"]
        },
        "maintenance": {
            "name": "Maintenance Technician",
            "description": "Equipment maintenance and repair",
            "permissions": ["Maintenance tasks", "Equipment repair", "Andon resolution"]
        },
        "quality": {
            "name": "Quality Control",
            "description": "Quality assurance and control",
            "permissions": ["Quality checks", "Defect tracking", "Quality reports"]
        },
        "viewer": {
            "name": "Viewer",
            "description": "Read-only access to system data",
            "permissions": ["View data", "Read reports", "Monitor status"]
        }
    }
    
    return {"roles": roles_info}


@router.get("/permissions", status_code=status.HTTP_200_OK)
async def get_user_permissions(
    current_user: UserContext = Depends(get_current_user)
) -> Dict[str, Any]:
    """Get current user's permissions."""
    return {
        "user_id": current_user.user_id,
        "role": current_user.role,
        "permissions": list(current_user.permissions)
    }
