"""
MS5.0 Floor Dashboard - Configuration Management

This module handles all configuration settings for the MS5.0 Floor Dashboard API.
It uses Pydantic Settings for environment variable management and validation.
"""

import os
from typing import List, Optional
from pydantic import Field, validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings with environment variable support."""
    
    # Application Settings
    APP_NAME: str = "MS5.0 Floor Dashboard API"
    VERSION: str = "1.0.0"
    ENVIRONMENT: str = Field(default="development", env="ENVIRONMENT")
    DEBUG: bool = Field(default=False, env="DEBUG")
    HOST: str = Field(default="0.0.0.0", env="HOST")
    PORT: int = Field(default=8000, env="PORT")
    LOG_LEVEL: str = Field(default="INFO", env="LOG_LEVEL")
    
    # Security Settings
    SECRET_KEY: str = Field(..., env="SECRET_KEY")
    ALGORITHM: str = Field(default="HS256", env="JWT_ALGORITHM")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(default=30, env="ACCESS_TOKEN_EXPIRE_MINUTES")
    REFRESH_TOKEN_EXPIRE_DAYS: int = Field(default=7, env="REFRESH_TOKEN_EXPIRE_DAYS")
    
    # CORS Settings
    ALLOWED_ORIGINS: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:8080"],
        env="ALLOWED_ORIGINS"
    )
    ALLOWED_HOSTS: List[str] = Field(
        default=["localhost", "127.0.0.1"],
        env="ALLOWED_HOSTS"
    )
    
    # Database Settings
    DATABASE_URL: str = Field(..., env="DATABASE_URL")
    DATABASE_POOL_SIZE: int = Field(default=10, env="DATABASE_POOL_SIZE")
    DATABASE_MAX_OVERFLOW: int = Field(default=20, env="DATABASE_MAX_OVERFLOW")
    DATABASE_ECHO: bool = Field(default=False, env="DATABASE_ECHO")
    
    # Redis Settings (for caching and sessions)
    REDIS_URL: str = Field(default="redis://localhost:6379/0", env="REDIS_URL")
    REDIS_PASSWORD: Optional[str] = Field(default=None, env="REDIS_PASSWORD")
    
    # WebSocket Settings
    WEBSOCKET_HEARTBEAT_INTERVAL: int = Field(default=30, env="WEBSOCKET_HEARTBEAT_INTERVAL")
    WEBSOCKET_MAX_CONNECTIONS: int = Field(default=1000, env="WEBSOCKET_MAX_CONNECTIONS")
    
    # Production Settings
    PRODUCTION_LINE_POLL_INTERVAL: int = Field(default=5, env="PRODUCTION_LINE_POLL_INTERVAL")
    OEE_CALCULATION_INTERVAL: int = Field(default=60, env="OEE_CALCULATION_INTERVAL")
    DOWNTIME_DETECTION_THRESHOLD: int = Field(default=30, env="DOWNTIME_DETECTION_THRESHOLD")
    
    # File Upload Settings
    MAX_FILE_SIZE: int = Field(default=10 * 1024 * 1024, env="MAX_FILE_SIZE")  # 10MB
    UPLOAD_DIRECTORY: str = Field(default="uploads", env="UPLOAD_DIRECTORY")
    ALLOWED_FILE_TYPES: List[str] = Field(
        default=["image/jpeg", "image/png", "application/pdf"],
        env="ALLOWED_FILE_TYPES"
    )
    
    # Email Settings (for notifications)
    SMTP_HOST: Optional[str] = Field(default=None, env="SMTP_HOST")
    SMTP_PORT: int = Field(default=587, env="SMTP_PORT")
    SMTP_USERNAME: Optional[str] = Field(default=None, env="SMTP_USERNAME")
    SMTP_PASSWORD: Optional[str] = Field(default=None, env="SMTP_PASSWORD")
    SMTP_USE_TLS: bool = Field(default=True, env="SMTP_USE_TLS")
    
    # Push Notification Settings
    ENABLE_PUSH_NOTIFICATIONS: bool = Field(default=False, env="ENABLE_PUSH_NOTIFICATIONS")
    FCM_SERVER_KEY: Optional[str] = Field(default=None, env="FCM_SERVER_KEY")
    FCM_PROJECT_ID: Optional[str] = Field(default=None, env="FCM_PROJECT_ID")
    
    # Monitoring Settings
    ENABLE_METRICS: bool = Field(default=True, env="ENABLE_METRICS")
    METRICS_PORT: int = Field(default=9090, env="METRICS_PORT")
    
    # Feature Flags
    ENABLE_OFFLINE_MODE: bool = Field(default=True, env="ENABLE_OFFLINE_MODE")
    ENABLE_PREDICTIVE_MAINTENANCE: bool = Field(default=False, env="ENABLE_PREDICTIVE_MAINTENANCE")
    ENABLE_AI_INSIGHTS: bool = Field(default=False, env="ENABLE_AI_INSIGHTS")
    
    # Integration Settings
    PLC_POLL_INTERVAL: int = Field(default=1, env="PLC_POLL_INTERVAL")
    PLC_TIMEOUT: int = Field(default=5, env="PLC_TIMEOUT")
    PLC_RETRY_ATTEMPTS: int = Field(default=3, env="PLC_RETRY_ATTEMPTS")
    
    # Report Settings
    REPORT_TEMPLATE_DIR: str = Field(default="templates/reports", env="REPORT_TEMPLATE_DIR")
    REPORT_OUTPUT_DIR: str = Field(default="reports", env="REPORT_OUTPUT_DIR")
    REPORT_RETENTION_DAYS: int = Field(default=90, env="REPORT_RETENTION_DAYS")
    
    # Andon Settings
    ANDON_ESCALATION_LEVELS: int = Field(default=3, env="ANDON_ESCALATION_LEVELS")
    ANDON_ACKNOWLEDGMENT_TIMEOUT: int = Field(default=300, env="ANDON_ACKNOWLEDGMENT_TIMEOUT")  # 5 minutes
    ANDON_RESOLUTION_TIMEOUT: int = Field(default=1800, env="ANDON_RESOLUTION_TIMEOUT")  # 30 minutes
    
    # Quality Settings
    QUALITY_CHECK_INTERVAL: int = Field(default=60, env="QUALITY_CHECK_INTERVAL")
    DEFECT_ESCALATION_THRESHOLD: int = Field(default=5, env="DEFECT_ESCALATION_THRESHOLD")
    
    # Maintenance Settings
    MAINTENANCE_REMINDER_DAYS: int = Field(default=7, env="MAINTENANCE_REMINDER_DAYS")
    MAINTENANCE_OVERDUE_DAYS: int = Field(default=3, env="MAINTENANCE_OVERDUE_DAYS")
    
    @validator("ALLOWED_ORIGINS", pre=True)
    def parse_allowed_origins(cls, v):
        """Parse comma-separated origins string."""
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",")]
        return v
    
    @validator("ALLOWED_HOSTS", pre=True)
    def parse_allowed_hosts(cls, v):
        """Parse comma-separated hosts string."""
        if isinstance(v, str):
            return [host.strip() for host in v.split(",")]
        return v
    
    @validator("ALLOWED_FILE_TYPES", pre=True)
    def parse_allowed_file_types(cls, v):
        """Parse comma-separated file types string."""
        if isinstance(v, str):
            return [file_type.strip() for file_type in v.split(",")]
        return v
    
    @validator("ENVIRONMENT")
    def validate_environment(cls, v):
        """Validate environment setting."""
        allowed_envs = ["development", "staging", "production"]
        if v not in allowed_envs:
            raise ValueError(f"ENVIRONMENT must be one of {allowed_envs}")
        return v
    
    @validator("LOG_LEVEL")
    def validate_log_level(cls, v):
        """Validate log level setting."""
        allowed_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in allowed_levels:
            raise ValueError(f"LOG_LEVEL must be one of {allowed_levels}")
        return v.upper()
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


# Create global settings instance
settings = Settings()


# Environment-specific configurations
class DevelopmentSettings(Settings):
    """Development environment settings."""
    DEBUG: bool = True
    DATABASE_ECHO: bool = True
    LOG_LEVEL: str = "DEBUG"


class StagingSettings(Settings):
    """Staging environment settings."""
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"


class ProductionSettings(Settings):
    """Production environment settings."""
    DEBUG: bool = False
    LOG_LEVEL: str = "WARNING"
    DATABASE_ECHO: bool = False


def get_settings() -> Settings:
    """Get settings based on environment."""
    env = os.getenv("ENVIRONMENT", "development")
    
    if env == "development":
        return DevelopmentSettings()
    elif env == "staging":
        return StagingSettings()
    elif env == "production":
        return ProductionSettings()
    else:
        return Settings()


# Export the appropriate settings instance
settings = get_settings()
