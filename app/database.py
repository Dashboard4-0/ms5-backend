"""
MS5.0 Floor Dashboard - Database Layer

This module handles database connections, ORM setup, and database operations
for the MS5.0 Floor Dashboard API. It uses SQLAlchemy with async support
and integrates with the existing factory telemetry database schema.
"""

import asyncio
from typing import AsyncGenerator, Optional
from contextlib import asynccontextmanager

from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy.pool import NullPool, QueuePool
import structlog

from app.config import settings

logger = structlog.get_logger()


class Base(DeclarativeBase):
    """Base class for all database models."""
    pass


# Database engines
sync_engine = None
async_engine = None
async_session_factory = None


async def init_db() -> None:
    """Initialize database connections and create tables."""
    global sync_engine, async_engine, async_session_factory
    
    try:
        # Create sync engine for migrations and admin operations
        sync_engine = create_engine(
            settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://"),
            poolclass=QueuePool,
            pool_size=settings.DATABASE_POOL_SIZE,
            max_overflow=settings.DATABASE_MAX_OVERFLOW,
            echo=settings.DATABASE_ECHO,
            future=True
        )
        
        # Create async engine for application operations
        async_engine = create_async_engine(
            settings.DATABASE_URL,
            poolclass=QueuePool,
            pool_size=settings.DATABASE_POOL_SIZE,
            max_overflow=settings.DATABASE_MAX_OVERFLOW,
            echo=settings.DATABASE_ECHO,
            future=True
        )
        
        # Create async session factory
        async_session_factory = async_sessionmaker(
            async_engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
        # Test database connectivity
        await test_database_connection()
        
        logger.info("Database initialized successfully")
        
    except Exception as e:
        logger.error("Failed to initialize database", error=str(e))
        raise


async def close_db() -> None:
    """Close database connections."""
    global sync_engine, async_engine, async_session_factory
    
    try:
        if async_engine:
            await async_engine.dispose()
            logger.info("Async database engine disposed")
        
        if sync_engine:
            sync_engine.dispose()
            logger.info("Sync database engine disposed")
            
        async_session_factory = None
        
    except Exception as e:
        logger.error("Error closing database connections", error=str(e))


async def test_database_connection() -> None:
    """Test database connectivity."""
    try:
        async with async_engine.begin() as conn:
            result = await conn.execute(text("SELECT 1"))
            result.fetchone()
        logger.info("Database connection test successful")
    except Exception as e:
        logger.error("Database connection test failed", error=str(e))
        raise


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Get database session with automatic cleanup."""
    if not async_session_factory:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency for getting database session in FastAPI endpoints."""
    async with get_db_session() as session:
        yield session


# Database utility functions
async def execute_query(query: str, params: Optional[dict] = None) -> list:
    """Execute a raw SQL query and return results."""
    try:
        async with get_db_session() as session:
            result = await session.execute(text(query), params or {})
            return result.fetchall()
    except Exception as e:
        logger.error("Database query execution failed", 
                    query=query[:100], params=params, error=str(e))
        raise


async def execute_scalar(query: str, params: Optional[dict] = None):
    """Execute a raw SQL query and return a single scalar result."""
    try:
        async with get_db_session() as session:
            result = await session.execute(text(query), params or {})
            return result.scalar()
    except Exception as e:
        logger.error("Database scalar execution failed", 
                    query=query[:100], params=params, error=str(e))
        raise


async def execute_update(query: str, params: Optional[dict] = None) -> int:
    """Execute an update/insert/delete query and return affected rows."""
    try:
        async with get_db_session() as session:
            result = await session.execute(text(query), params or {})
            await session.commit()
            return result.rowcount
    except Exception as e:
        logger.error("Database update execution failed", 
                    query=query[:100], params=params, error=str(e))
        raise


# Database health check
async def check_database_health() -> dict:
    """Check database health and return status information."""
    try:
        # Test basic connectivity
        await test_database_connection()
        
        # Check database size
        size_query = """
        SELECT pg_size_pretty(pg_database_size(current_database())) as size
        """
        db_size = await execute_scalar(size_query)
        
        # Check active connections
        connections_query = """
        SELECT count(*) as active_connections 
        FROM pg_stat_activity 
        WHERE state = 'active'
        """
        active_connections = await execute_scalar(connections_query)
        
        # Check for long-running queries
        long_queries_query = """
        SELECT count(*) as long_queries
        FROM pg_stat_activity 
        WHERE state = 'active' 
        AND query_start < NOW() - INTERVAL '5 minutes'
        """
        long_queries = await execute_scalar(long_queries_query)
        
        return {
            "status": "healthy",
            "database_size": db_size,
            "active_connections": active_connections,
            "long_queries": long_queries,
            "pool_size": settings.DATABASE_POOL_SIZE,
            "max_overflow": settings.DATABASE_MAX_OVERFLOW
        }
        
    except Exception as e:
        logger.error("Database health check failed", error=str(e))
        return {
            "status": "unhealthy",
            "error": str(e)
        }


# Transaction management
class DatabaseTransaction:
    """Context manager for database transactions."""
    
    def __init__(self):
        self.session: Optional[AsyncSession] = None
    
    async def __aenter__(self) -> AsyncSession:
        if not async_session_factory:
            raise RuntimeError("Database not initialized. Call init_db() first.")
        
        self.session = async_session_factory()
        return self.session
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            if exc_type:
                await self.session.rollback()
            else:
                await self.session.commit()
            await self.session.close()


# Database migration utilities
async def run_migrations() -> None:
    """Run database migrations."""
    try:
        # This would typically use Alembic or similar migration tool
        # For now, we'll just log that migrations would run here
        logger.info("Database migrations would run here")
        
        # Example migration check
        migration_check_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'factory_telemetry' 
            AND table_name = 'production_lines'
        ) as table_exists
        """
        
        table_exists = await execute_scalar(migration_check_query)
        
        if not table_exists:
            logger.warning("Production tables not found. Run migrations first.")
        else:
            logger.info("Production tables found. Database schema is up to date.")
            
    except Exception as e:
        logger.error("Migration check failed", error=str(e))
        raise


# Connection pool monitoring
async def get_connection_pool_status() -> dict:
    """Get connection pool status information."""
    try:
        if not async_engine:
            return {"error": "Database not initialized"}
        
        pool = async_engine.pool
        
        return {
            "pool_size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "invalid": pool.invalid()
        }
        
    except Exception as e:
        logger.error("Failed to get connection pool status", error=str(e))
        return {"error": str(e)}


# Database cleanup utilities
async def cleanup_old_data() -> None:
    """Clean up old data based on retention policies."""
    try:
        # Clean up old reports
        cleanup_reports_query = """
        DELETE FROM factory_telemetry.production_reports 
        WHERE generated_at < NOW() - INTERVAL '%s days'
        """ % settings.REPORT_RETENTION_DAYS
        
        deleted_reports = await execute_update(cleanup_reports_query)
        logger.info(f"Cleaned up {deleted_reports} old reports")
        
        # Clean up old OEE calculations (keep last 90 days)
        cleanup_oee_query = """
        DELETE FROM factory_telemetry.oee_calculations 
        WHERE calculation_time < NOW() - INTERVAL '90 days'
        """
        
        deleted_oee = await execute_update(cleanup_oee_query)
        logger.info(f"Cleaned up {deleted_oee} old OEE calculations")
        
    except Exception as e:
        logger.error("Data cleanup failed", error=str(e))
        raise
