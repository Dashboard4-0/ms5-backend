"""
MS5.0 Floor Dashboard - JWT Token Handler

This module handles JWT token creation, validation, and management
for the MS5.0 Floor Dashboard API authentication system.
"""

import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Union

import jwt
from fastapi import HTTPException, status
from passlib.context import CryptContext
import structlog

from app.config import settings

logger = structlog.get_logger()

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class JWTError(Exception):
    """Custom JWT error class."""
    pass


class JWTManager:
    """JWT token manager for authentication."""
    
    def __init__(self):
        self.secret_key = settings.SECRET_KEY
        self.algorithm = settings.ALGORITHM
        self.access_token_expire_minutes = settings.ACCESS_TOKEN_EXPIRE_MINUTES
        self.refresh_token_expire_days = settings.REFRESH_TOKEN_EXPIRE_DAYS
    
    def create_access_token(
        self,
        subject: Union[str, int],
        expires_delta: Optional[timedelta] = None,
        additional_claims: Optional[Dict[str, Any]] = None
    ) -> str:
        """Create a new access token."""
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=self.access_token_expire_minutes)
        
        to_encode = {
            "sub": str(subject),
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "access"
        }
        
        if additional_claims:
            to_encode.update(additional_claims)
        
        try:
            encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
            logger.info("Access token created", user_id=subject, expires_at=expire)
            return encoded_jwt
        except Exception as e:
            logger.error("Failed to create access token", error=str(e), user_id=subject)
            raise JWTError("Failed to create access token")
    
    def create_refresh_token(
        self,
        subject: Union[str, int],
        expires_delta: Optional[timedelta] = None,
        additional_claims: Optional[Dict[str, Any]] = None
    ) -> str:
        """Create a new refresh token."""
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(days=self.refresh_token_expire_days)
        
        to_encode = {
            "sub": str(subject),
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "refresh"
        }
        
        if additional_claims:
            to_encode.update(additional_claims)
        
        try:
            encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
            logger.info("Refresh token created", user_id=subject, expires_at=expire)
            return encoded_jwt
        except Exception as e:
            logger.error("Failed to create refresh token", error=str(e), user_id=subject)
            raise JWTError("Failed to create refresh token")
    
    def verify_token(self, token: str, token_type: str = "access") -> Dict[str, Any]:
        """Verify and decode a JWT token."""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            # Check token type
            if payload.get("type") != token_type:
                raise JWTError(f"Invalid token type. Expected {token_type}")
            
            # Check expiration
            exp = payload.get("exp")
            if exp and datetime.utcnow() > datetime.fromtimestamp(exp):
                raise JWTError("Token has expired")
            
            logger.debug("Token verified successfully", user_id=payload.get("sub"), token_type=token_type)
            return payload
            
        except jwt.ExpiredSignatureError:
            logger.warning("Token has expired", token_type=token_type)
            raise JWTError("Token has expired")
        except jwt.InvalidTokenError as e:
            logger.warning("Invalid token", error=str(e), token_type=token_type)
            raise JWTError("Invalid token")
        except Exception as e:
            logger.error("Token verification failed", error=str(e), token_type=token_type)
            raise JWTError("Token verification failed")
    
    def refresh_access_token(self, refresh_token: str) -> str:
        """Create a new access token from a refresh token."""
        try:
            # Verify refresh token
            payload = self.verify_token(refresh_token, token_type="refresh")
            
            # Extract user information
            user_id = payload.get("sub")
            if not user_id:
                raise JWTError("Invalid refresh token: missing subject")
            
            # Create new access token with same claims (excluding token-specific ones)
            additional_claims = {k: v for k, v in payload.items() 
                               if k not in ["sub", "exp", "iat", "type"]}
            
            new_access_token = self.create_access_token(
                subject=user_id,
                additional_claims=additional_claims
            )
            
            logger.info("Access token refreshed", user_id=user_id)
            return new_access_token
            
        except JWTError:
            raise
        except Exception as e:
            logger.error("Failed to refresh access token", error=str(e))
            raise JWTError("Failed to refresh access token")
    
    def get_token_payload(self, token: str) -> Dict[str, Any]:
        """Get token payload without verification (for debugging)."""
        try:
            # Decode without verification
            payload = jwt.decode(token, options={"verify_signature": False})
            return payload
        except Exception as e:
            logger.error("Failed to decode token payload", error=str(e))
            raise JWTError("Failed to decode token payload")
    
    def is_token_expired(self, token: str) -> bool:
        """Check if a token is expired without raising an exception."""
        try:
            payload = self.get_token_payload(token)
            exp = payload.get("exp")
            if exp:
                return datetime.utcnow() > datetime.fromtimestamp(exp)
            return True
        except JWTError:
            return True


class PasswordManager:
    """Password hashing and verification manager."""
    
    def __init__(self):
        self.pwd_context = pwd_context
    
    def hash_password(self, password: str) -> str:
        """Hash a password."""
        try:
            hashed = self.pwd_context.hash(password)
            logger.debug("Password hashed successfully")
            return hashed
        except Exception as e:
            logger.error("Failed to hash password", error=str(e))
            raise JWTError("Failed to hash password")
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash."""
        try:
            is_valid = self.pwd_context.verify(plain_password, hashed_password)
            logger.debug("Password verification completed", is_valid=is_valid)
            return is_valid
        except Exception as e:
            logger.error("Failed to verify password", error=str(e))
            return False
    
    def needs_refresh(self, hashed_password: str) -> bool:
        """Check if a password hash needs to be refreshed."""
        try:
            return self.pwd_context.needs_update(hashed_password)
        except Exception as e:
            logger.error("Failed to check password refresh status", error=str(e))
            return False


# Global instances
jwt_manager = JWTManager()
password_manager = PasswordManager()


# Utility functions
def create_tokens(user_id: str, user_data: Dict[str, Any]) -> Dict[str, str]:
    """Create both access and refresh tokens for a user."""
    try:
        # Create access token
        access_token = jwt_manager.create_access_token(
            subject=user_id,
            additional_claims={
                "user_id": user_id,
                "role": user_data.get("role"),
                "department": user_data.get("department"),
                "shift": user_data.get("shift"),
                "is_active": user_data.get("is_active", True)
            }
        )
        
        # Create refresh token
        refresh_token = jwt_manager.create_refresh_token(
            subject=user_id,
            additional_claims={
                "user_id": user_id,
                "role": user_data.get("role")
            }
        )
        
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "expires_in": settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
        }
        
    except Exception as e:
        logger.error("Failed to create tokens", error=str(e), user_id=user_id)
        raise JWTError("Failed to create tokens")


def verify_access_token(token: str) -> Dict[str, Any]:
    """Verify an access token and return its payload."""
    return jwt_manager.verify_token(token, token_type="access")


def verify_refresh_token(token: str) -> Dict[str, Any]:
    """Verify a refresh token and return its payload."""
    return jwt_manager.verify_token(token, token_type="refresh")


def hash_password(password: str) -> str:
    """Hash a password."""
    return password_manager.hash_password(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash."""
    return password_manager.verify_password(plain_password, hashed_password)


def needs_password_refresh(hashed_password: str) -> bool:
    """Check if a password hash needs to be refreshed."""
    return password_manager.needs_refresh(hashed_password)
