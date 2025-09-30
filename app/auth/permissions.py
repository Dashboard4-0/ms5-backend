"""
MS5.0 Floor Dashboard - Permission System

This module defines the permission system for role-based access control
in the MS5.0 Floor Dashboard API. It handles user roles, permissions,
and authorization checks.
"""

from enum import Enum
from typing import Dict, List, Set, Optional, Any
from functools import wraps

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import structlog

from app.auth.jwt_handler import verify_access_token, JWTError
from app.utils.exceptions import AuthenticationError, AuthorizationError

logger = structlog.get_logger()

# Security scheme
security = HTTPBearer()


class UserRole(str, Enum):
    """User role enumeration."""
    ADMIN = "admin"
    PRODUCTION_MANAGER = "production_manager"
    SHIFT_MANAGER = "shift_manager"
    ENGINEER = "engineer"
    OPERATOR = "operator"
    MAINTENANCE = "maintenance"
    QUALITY = "quality"
    VIEWER = "viewer"


class Permission(str, Enum):
    """Permission enumeration."""
    # User Management
    USER_READ = "user:read"
    USER_WRITE = "user:write"
    USER_DELETE = "user:delete"
    
    # Production Management
    PRODUCTION_READ = "production:read"
    PRODUCTION_WRITE = "production:write"
    PRODUCTION_DELETE = "production:delete"
    
    # Line Management
    LINE_READ = "line:read"
    LINE_WRITE = "line:write"
    LINE_DELETE = "line:delete"
    
    # Schedule Management
    SCHEDULE_READ = "schedule:read"
    SCHEDULE_WRITE = "schedule:write"
    SCHEDULE_DELETE = "schedule:delete"
    
    # Job Management
    JOB_READ = "job:read"
    JOB_WRITE = "job:write"
    JOB_ASSIGN = "job:assign"
    JOB_ACCEPT = "job:accept"
    JOB_COMPLETE = "job:complete"
    
    # Checklist Management
    CHECKLIST_READ = "checklist:read"
    CHECKLIST_WRITE = "checklist:write"
    CHECKLIST_COMPLETE = "checklist:complete"
    
    # OEE & Analytics
    OEE_READ = "oee:read"
    OEE_CALCULATE = "oee:calculate"
    ANALYTICS_READ = "analytics:read"
    
    # Downtime Management
    DOWNTIME_READ = "downtime:read"
    DOWNTIME_WRITE = "downtime:write"
    DOWNTIME_CONFIRM = "downtime:confirm"
    
    # Andon System
    ANDON_READ = "andon:read"
    ANDON_CREATE = "andon:create"
    ANDON_ACKNOWLEDGE = "andon:acknowledge"
    ANDON_RESOLVE = "andon:resolve"
    
    # Equipment Management
    EQUIPMENT_READ = "equipment:read"
    EQUIPMENT_WRITE = "equipment:write"
    EQUIPMENT_MAINTENANCE = "equipment:maintenance"
    
    # Reports
    REPORTS_READ = "reports:read"
    REPORTS_WRITE = "reports:write"
    REPORTS_GENERATE = "reports:generate"
    REPORTS_DELETE = "reports:delete"
    REPORTS_SCHEDULE = "reports:schedule"
    REPORTS_TEMPLATE_MANAGE = "reports:template:manage"
    
    # Dashboard
    DASHBOARD_READ = "dashboard:read"
    DASHBOARD_WRITE = "dashboard:write"
    
    # Quality Management
    QUALITY_READ = "quality:read"
    QUALITY_WRITE = "quality:write"
    QUALITY_APPROVE = "quality:approve"
    
    # Maintenance
    MAINTENANCE_READ = "maintenance:read"
    MAINTENANCE_WRITE = "maintenance:write"
    MAINTENANCE_SCHEDULE = "maintenance:schedule"
    
    # System Administration
    SYSTEM_CONFIG = "system:config"
    SYSTEM_MONITOR = "system:monitor"
    SYSTEM_MAINTENANCE = "system:maintenance"


# Role-Permission mapping
ROLE_PERMISSIONS: Dict[UserRole, Set[Permission]] = {
    UserRole.ADMIN: {
        # Admin has all permissions
        *[permission for permission in Permission]
    },
    
    UserRole.PRODUCTION_MANAGER: {
        # Production management
        Permission.PRODUCTION_READ,
        Permission.PRODUCTION_WRITE,
        Permission.PRODUCTION_DELETE,
        Permission.LINE_READ,
        Permission.LINE_WRITE,
        Permission.SCHEDULE_READ,
        Permission.SCHEDULE_WRITE,
        Permission.SCHEDULE_DELETE,
        Permission.JOB_READ,
        Permission.JOB_WRITE,
        Permission.JOB_ASSIGN,
        Permission.CHECKLIST_READ,
        Permission.CHECKLIST_WRITE,
        Permission.OEE_READ,
        Permission.OEE_CALCULATE,
        Permission.ANALYTICS_READ,
        Permission.DOWNTIME_READ,
        Permission.DOWNTIME_WRITE,
        Permission.DOWNTIME_CONFIRM,
        Permission.ANDON_READ,
        Permission.ANDON_ACKNOWLEDGE,
        Permission.ANDON_RESOLVE,
        Permission.EQUIPMENT_READ,
        Permission.REPORTS_READ,
        Permission.REPORTS_GENERATE,
        Permission.DASHBOARD_READ,
        Permission.DASHBOARD_WRITE,
        Permission.QUALITY_READ,
        Permission.QUALITY_WRITE,
        Permission.QUALITY_APPROVE,
        Permission.MAINTENANCE_READ,
        Permission.MAINTENANCE_WRITE,
        Permission.MAINTENANCE_SCHEDULE,
        Permission.SYSTEM_MONITOR,
    },
    
    UserRole.SHIFT_MANAGER: {
        # Shift management
        Permission.PRODUCTION_READ,
        Permission.LINE_READ,
        Permission.SCHEDULE_READ,
        Permission.SCHEDULE_WRITE,
        Permission.JOB_READ,
        Permission.JOB_WRITE,
        Permission.JOB_ASSIGN,
        Permission.CHECKLIST_READ,
        Permission.CHECKLIST_WRITE,
        Permission.OEE_READ,
        Permission.ANALYTICS_READ,
        Permission.DOWNTIME_READ,
        Permission.DOWNTIME_WRITE,
        Permission.DOWNTIME_CONFIRM,
        Permission.ANDON_READ,
        Permission.ANDON_ACKNOWLEDGE,
        Permission.ANDON_RESOLVE,
        Permission.EQUIPMENT_READ,
        Permission.REPORTS_READ,
        Permission.REPORTS_GENERATE,
        Permission.DASHBOARD_READ,
        Permission.DASHBOARD_WRITE,
        Permission.QUALITY_READ,
        Permission.QUALITY_WRITE,
        Permission.MAINTENANCE_READ,
    },
    
    UserRole.ENGINEER: {
        # Engineering and maintenance
        Permission.PRODUCTION_READ,
        Permission.LINE_READ,
        Permission.JOB_READ,
        Permission.OEE_READ,
        Permission.OEE_CALCULATE,
        Permission.ANALYTICS_READ,
        Permission.DOWNTIME_READ,
        Permission.DOWNTIME_WRITE,
        Permission.DOWNTIME_CONFIRM,
        Permission.ANDON_READ,
        Permission.ANDON_ACKNOWLEDGE,
        Permission.ANDON_RESOLVE,
        Permission.EQUIPMENT_READ,
        Permission.EQUIPMENT_WRITE,
        Permission.EQUIPMENT_MAINTENANCE,
        Permission.REPORTS_READ,
        Permission.REPORTS_GENERATE,
        Permission.DASHBOARD_READ,
        Permission.QUALITY_READ,
        Permission.QUALITY_WRITE,
        Permission.MAINTENANCE_READ,
        Permission.MAINTENANCE_WRITE,
        Permission.MAINTENANCE_SCHEDULE,
    },
    
    UserRole.OPERATOR: {
        # Operator permissions
        Permission.PRODUCTION_READ,
        Permission.LINE_READ,
        Permission.JOB_READ,
        Permission.JOB_ACCEPT,
        Permission.JOB_COMPLETE,
        Permission.CHECKLIST_READ,
        Permission.CHECKLIST_COMPLETE,
        Permission.OEE_READ,
        Permission.DOWNTIME_READ,
        Permission.ANDON_READ,
        Permission.ANDON_CREATE,
        Permission.EQUIPMENT_READ,
        Permission.DASHBOARD_READ,
        Permission.QUALITY_READ,
        Permission.QUALITY_WRITE,
        Permission.MAINTENANCE_READ,
    },
    
    UserRole.MAINTENANCE: {
        # Maintenance technician
        Permission.PRODUCTION_READ,
        Permission.LINE_READ,
        Permission.JOB_READ,
        Permission.OEE_READ,
        Permission.ANDON_READ,
        Permission.ANDON_ACKNOWLEDGE,
        Permission.ANDON_RESOLVE,
        Permission.EQUIPMENT_READ,
        Permission.EQUIPMENT_WRITE,
        Permission.EQUIPMENT_MAINTENANCE,
        Permission.DASHBOARD_READ,
        Permission.MAINTENANCE_READ,
        Permission.MAINTENANCE_WRITE,
        Permission.MAINTENANCE_SCHEDULE,
    },
    
    UserRole.QUALITY: {
        # Quality control
        Permission.PRODUCTION_READ,
        Permission.LINE_READ,
        Permission.JOB_READ,
        Permission.OEE_READ,
        Permission.ANDON_READ,
        Permission.ANDON_CREATE,
        Permission.EQUIPMENT_READ,
        Permission.REPORTS_READ,
        Permission.REPORTS_GENERATE,
        Permission.DASHBOARD_READ,
        Permission.QUALITY_READ,
        Permission.QUALITY_WRITE,
        Permission.QUALITY_APPROVE,
        Permission.MAINTENANCE_READ,
    },
    
    UserRole.VIEWER: {
        # Read-only access
        Permission.PRODUCTION_READ,
        Permission.LINE_READ,
        Permission.JOB_READ,
        Permission.OEE_READ,
        Permission.ANALYTICS_READ,
        Permission.ANDON_READ,
        Permission.EQUIPMENT_READ,
        Permission.REPORTS_READ,
        Permission.DASHBOARD_READ,
        Permission.QUALITY_READ,
        Permission.MAINTENANCE_READ,
    }
}


class UserContext:
    """User context for authorization."""
    
    def __init__(self, user_id: str, role: UserRole, permissions: Set[Permission], 
                 additional_data: Optional[Dict[str, Any]] = None):
        self.user_id = user_id
        self.role = role
        self.permissions = permissions
        self.additional_data = additional_data or {}
    
    def has_permission(self, permission: Permission) -> bool:
        """Check if user has a specific permission."""
        return permission in self.permissions
    
    def has_any_permission(self, permissions: List[Permission]) -> bool:
        """Check if user has any of the specified permissions."""
        return any(permission in self.permissions for permission in permissions)
    
    def has_all_permissions(self, permissions: List[Permission]) -> bool:
        """Check if user has all of the specified permissions."""
        return all(permission in self.permissions for permission in permissions)
    
    def is_role(self, role: UserRole) -> bool:
        """Check if user has a specific role."""
        return self.role == role
    
    def is_any_role(self, roles: List[UserRole]) -> bool:
        """Check if user has any of the specified roles."""
        return self.role in roles


def get_user_permissions(role: UserRole) -> Set[Permission]:
    """Get permissions for a specific role."""
    return ROLE_PERMISSIONS.get(role, set())


def create_user_context(user_data: Dict[str, Any]) -> UserContext:
    """Create user context from user data."""
    user_id = user_data.get("user_id")
    role_str = user_data.get("role")
    
    if not user_id:
        raise AuthenticationError("Missing user ID in token")
    
    if not role_str:
        raise AuthenticationError("Missing role in token")
    
    try:
        role = UserRole(role_str)
    except ValueError:
        raise AuthenticationError(f"Invalid role: {role_str}")
    
    permissions = get_user_permissions(role)
    
    return UserContext(
        user_id=user_id,
        role=role,
        permissions=permissions,
        additional_data=user_data
    )


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> UserContext:
    """Get current user from JWT token."""
    try:
        # Verify the token
        payload = verify_access_token(credentials.credentials)
        
        # Create user context
        user_context = create_user_context(payload)
        
        logger.debug("User authenticated", user_id=user_context.user_id, role=user_context.role)
        return user_context
        
    except JWTError as e:
        logger.warning("JWT authentication failed", error=str(e))
        raise AuthenticationError("Invalid or expired token")
    except Exception as e:
        logger.error("Authentication error", error=str(e))
        raise AuthenticationError("Authentication failed")


def require_permission(permission: Permission):
    """Decorator to require a specific permission."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get current user from kwargs
            current_user = kwargs.get('current_user')
            if not current_user:
                raise AuthorizationError("User context not available")
            
            if not current_user.has_permission(permission):
                logger.warning(
                    "Permission denied",
                    user_id=current_user.user_id,
                    required_permission=permission,
                    user_permissions=list(current_user.permissions)
                )
                raise AuthorizationError(f"Permission required: {permission}")
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator


def require_any_permission(permissions: List[Permission]):
    """Decorator to require any of the specified permissions."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            current_user = kwargs.get('current_user')
            if not current_user:
                raise AuthorizationError("User context not available")
            
            if not current_user.has_any_permission(permissions):
                logger.warning(
                    "Permission denied",
                    user_id=current_user.user_id,
                    required_permissions=permissions,
                    user_permissions=list(current_user.permissions)
                )
                raise AuthorizationError(f"One of these permissions required: {permissions}")
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator


def require_all_permissions(permissions: List[Permission]):
    """Decorator to require all of the specified permissions."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            current_user = kwargs.get('current_user')
            if not current_user:
                raise AuthorizationError("User context not available")
            
            if not current_user.has_all_permissions(permissions):
                logger.warning(
                    "Permission denied",
                    user_id=current_user.user_id,
                    required_permissions=permissions,
                    user_permissions=list(current_user.permissions)
                )
                raise AuthorizationError(f"All of these permissions required: {permissions}")
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator


def require_role(role: UserRole):
    """Decorator to require a specific role."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            current_user = kwargs.get('current_user')
            if not current_user:
                raise AuthorizationError("User context not available")
            
            if not current_user.is_role(role):
                logger.warning(
                    "Role denied",
                    user_id=current_user.user_id,
                    required_role=role,
                    user_role=current_user.role
                )
                raise AuthorizationError(f"Role required: {role}")
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator


def require_any_role(roles: List[UserRole]):
    """Decorator to require any of the specified roles."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            current_user = kwargs.get('current_user')
            if not current_user:
                raise AuthorizationError("User context not available")
            
            if not current_user.is_any_role(roles):
                logger.warning(
                    "Role denied",
                    user_id=current_user.user_id,
                    required_roles=roles,
                    user_role=current_user.role
                )
                raise AuthorizationError(f"One of these roles required: {roles}")
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator


# Common permission combinations
def require_production_access():
    """Require production management access."""
    return require_any_permission([
        Permission.PRODUCTION_READ,
        Permission.PRODUCTION_WRITE
    ])


def require_job_management():
    """Require job management access."""
    return require_any_permission([
        Permission.JOB_READ,
        Permission.JOB_WRITE,
        Permission.JOB_ASSIGN
    ])


def require_andon_access():
    """Require Andon system access."""
    return require_any_permission([
        Permission.ANDON_READ,
        Permission.ANDON_CREATE,
        Permission.ANDON_ACKNOWLEDGE,
        Permission.ANDON_RESOLVE
    ])


def require_equipment_access():
    """Require equipment management access."""
    return require_any_permission([
        Permission.EQUIPMENT_READ,
        Permission.EQUIPMENT_WRITE,
        Permission.EQUIPMENT_MAINTENANCE
    ])


def require_report_access():
    """Require report access."""
    return require_any_permission([
        Permission.REPORTS_READ,
        Permission.REPORTS_GENERATE
    ])
