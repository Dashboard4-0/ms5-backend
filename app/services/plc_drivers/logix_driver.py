"""
MS5.0 Floor Dashboard - LogixDriver Service

This module provides a comprehensive LogixDriver service for CompactLogix/ControlLogix
PLC communication with enhanced features for production management integration.
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from uuid import UUID
import structlog

from pycomm3 import LogixDriver
from tenacity import retry, stop_after_attempt, wait_exponential

from app.database import execute_query, execute_scalar, execute_update
from app.utils.exceptions import BusinessLogicError, NotFoundError

logger = structlog.get_logger()


class LogixDriverService:
    """Enhanced LogixDriver service for CompactLogix/ControlLogix PLC communication."""
    
    def __init__(self, ip_address: str, name: str = "Logix PLC"):
        """Initialize LogixDriver service."""
        self.ip_address = ip_address
        self.name = name
        self.driver: Optional[LogixDriver] = None
        self.connected = False
        self.connection_attempts = 0
        self.max_connection_attempts = 5
        self.last_connection_time = None
        self.connection_timeout = 30  # seconds
        self.read_timeout = 10  # seconds
        self.write_timeout = 10  # seconds
        
        # Performance monitoring
        self.read_operations = 0
        self.write_operations = 0
        self.failed_reads = 0
        self.failed_writes = 0
        self.last_read_time = None
        self.last_write_time = None
        self.avg_read_time = 0.0
        self.avg_write_time = 0.0
        
        # Tag cache for performance
        self.tag_cache = {}
        self.cache_ttl = 5  # seconds
        self.cache_enabled = True
        
        # Security features
        self.encryption_enabled = False
        self.authentication_required = False
        self.user_name = None
        self.password = None
        
        # Diagnostic data
        self.diagnostic_data = {
            "controller_info": None,
            "module_info": [],
            "tag_info": {},
            "connection_status": "disconnected",
            "last_error": None,
            "error_count": 0
        }
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    async def connect(self) -> bool:
        """Connect to Logix PLC with enhanced error handling."""
        try:
            if self.connected:
                return True
            
            logger.info("Connecting to Logix PLC", name=self.name, ip=self.ip_address)
            
            # Create driver instance
            self.driver = LogixDriver(self.ip_address)
            
            # Set timeouts
            self.driver.timeout = self.connection_timeout
            
            # Open connection
            self.driver.open()
            
            # Verify connection
            if self.driver.connected:
                self.connected = True
                self.connection_attempts = 0
                self.last_connection_time = datetime.utcnow()
                
                # Get controller information
                await self._get_controller_info()
                
                # Update diagnostic data
                self.diagnostic_data["connection_status"] = "connected"
                self.diagnostic_data["last_error"] = None
                
                logger.info(
                    "Logix PLC connected successfully",
                    name=self.name,
                    ip=self.ip_address,
                    controller_info=self.diagnostic_data["controller_info"]
                )
                
                return True
            else:
                raise Exception("Connection verification failed")
                
        except Exception as e:
            self.connection_attempts += 1
            self.connected = False
            self.diagnostic_data["connection_status"] = "failed"
            self.diagnostic_data["last_error"] = str(e)
            self.diagnostic_data["error_count"] += 1
            
            logger.error(
                "Logix PLC connection failed",
                name=self.name,
                ip=self.ip_address,
                attempt=self.connection_attempts,
                error=str(e)
            )
            
            if self.connection_attempts >= self.max_connection_attempts:
                raise BusinessLogicError(f"Failed to connect to PLC {self.name} after {self.max_connection_attempts} attempts")
            
            raise
    
    async def disconnect(self) -> None:
        """Disconnect from Logix PLC."""
        try:
            if self.driver and self.connected:
                self.driver.close()
                self.connected = False
                self.diagnostic_data["connection_status"] = "disconnected"
                
                logger.info("Logix PLC disconnected", name=self.name)
                
        except Exception as e:
            logger.error("Error disconnecting from Logix PLC", name=self.name, error=str(e))
    
    async def read_tags(self, tags: List[str], use_cache: bool = True) -> Dict[str, Any]:
        """Read multiple tags from Logix PLC with caching and performance monitoring."""
        if not self.connected or not self.driver:
            raise RuntimeError(f"PLC {self.name} not connected")
        
        start_time = time.time()
        results = {}
        uncached_tags = []
        
        try:
            # Check cache for tags
            if use_cache and self.cache_enabled:
                for tag in tags:
                    if tag in self.tag_cache:
                        cache_entry = self.tag_cache[tag]
                        if time.time() - cache_entry["timestamp"] < self.cache_ttl:
                            results[tag] = cache_entry["data"]
                        else:
                            uncached_tags.append(tag)
                    else:
                        uncached_tags.append(tag)
            else:
                uncached_tags = tags
            
            # Read uncached tags from PLC
            if uncached_tags:
                plc_results = await self._read_tags_from_plc(uncached_tags)
                
                # Update cache
                if use_cache and self.cache_enabled:
                    for tag, data in plc_results.items():
                        self.tag_cache[tag] = {
                            "data": data,
                            "timestamp": time.time()
                        }
                
                results.update(plc_results)
            
            # Update performance metrics
            read_time = time.time() - start_time
            self.read_operations += 1
            self.last_read_time = datetime.utcnow()
            self.avg_read_time = (self.avg_read_time * (self.read_operations - 1) + read_time) / self.read_operations
            
            logger.debug(
                "Tags read successfully",
                name=self.name,
                total_tags=len(tags),
                cached_tags=len(tags) - len(uncached_tags),
                read_time=read_time
            )
            
            return results
            
        except Exception as e:
            self.failed_reads += 1
            self.diagnostic_data["last_error"] = str(e)
            self.diagnostic_data["error_count"] += 1
            
            logger.error("Failed to read tags from Logix PLC", name=self.name, error=str(e))
            raise
    
    async def write_tags(self, tag_values: Dict[str, Any]) -> Dict[str, bool]:
        """Write multiple tags to Logix PLC with validation."""
        if not self.connected or not self.driver:
            raise RuntimeError(f"PLC {self.name} not connected")
        
        start_time = time.time()
        results = {}
        
        try:
            # Validate tag values
            validated_values = await self._validate_tag_values(tag_values)
            
            # Write tags to PLC
            for tag, value in validated_values.items():
                try:
                    # Set timeout for write operation
                    self.driver.timeout = self.write_timeout
                    
                    # Write tag
                    response = self.driver.write(tag, value)
                    
                    if response.error:
                        results[tag] = False
                        logger.warning(
                            "Tag write error",
                            name=self.name,
                            tag=tag,
                            value=value,
                            error=response.error
                        )
                    else:
                        results[tag] = True
                        
                        # Update cache if tag exists
                        if tag in self.tag_cache:
                            self.tag_cache[tag] = {
                                "data": {"value": value, "error": None},
                                "timestamp": time.time()
                            }
                        
                        logger.debug("Tag written successfully", name=self.name, tag=tag, value=value)
                
                except Exception as e:
                    results[tag] = False
                    logger.error("Failed to write tag", name=self.name, tag=tag, error=str(e))
            
            # Update performance metrics
            write_time = time.time() - start_time
            self.write_operations += 1
            self.last_write_time = datetime.utcnow()
            self.avg_write_time = (self.avg_write_time * (self.write_operations - 1) + write_time) / self.write_operations
            
            successful_writes = sum(1 for success in results.values() if success)
            if successful_writes < len(tag_values):
                self.failed_writes += 1
            
            logger.info(
                "Tag write operation completed",
                name=self.name,
                total_tags=len(tag_values),
                successful_writes=successful_writes,
                write_time=write_time
            )
            
            return results
            
        except Exception as e:
            self.failed_writes += 1
            self.diagnostic_data["last_error"] = str(e)
            self.diagnostic_data["error_count"] += 1
            
            logger.error("Failed to write tags to Logix PLC", name=self.name, error=str(e))
            raise
    
    async def read_bool_array(self, tag_base: str, size: int = 64) -> List[bool]:
        """Read BOOL array from Logix PLC."""
        tag = f"{tag_base}{{{size}}}"
        result = await self.read_tags([tag])
        
        if tag in result and result[tag].get("error") is None:
            return result[tag]["value"]
        
        return [False] * size
    
    async def write_bool_array(self, tag_base: str, values: List[bool]) -> bool:
        """Write BOOL array to Logix PLC."""
        tag = f"{tag_base}{{{len(values)}}}"
        result = await self.write_tags({tag: values})
        
        return result.get(tag, False)
    
    async def get_controller_info(self) -> Dict[str, Any]:
        """Get comprehensive controller information."""
        if not self.connected or not self.driver:
            raise RuntimeError(f"PLC {self.name} not connected")
        
        try:
            info = self.driver.info
            
            controller_info = {
                "name": info.get("name", "Unknown"),
                "vendor": info.get("vendor", "Unknown"),
                "product_type": info.get("product_type", "Unknown"),
                "product_code": info.get("product_code", "Unknown"),
                "revision": info.get("revision", "Unknown"),
                "serial_number": info.get("serial_number", "Unknown"),
                "ip_address": self.ip_address,
                "connection_time": self.last_connection_time.isoformat() if self.last_connection_time else None,
                "status": "connected" if self.connected else "disconnected"
            }
            
            # Update diagnostic data
            self.diagnostic_data["controller_info"] = controller_info
            
            return controller_info
            
        except Exception as e:
            logger.error("Failed to get controller info", name=self.name, error=str(e))
            raise
    
    async def get_module_info(self) -> List[Dict[str, Any]]:
        """Get information about all modules in the controller."""
        if not self.connected or not self.driver:
            raise RuntimeError(f"PLC {self.name} not connected")
        
        try:
            modules = self.driver.get_module_info()
            
            module_info = []
            for module in modules:
                module_data = {
                    "slot": module.get("slot", 0),
                    "name": module.get("name", "Unknown"),
                    "vendor": module.get("vendor", "Unknown"),
                    "product_type": module.get("product_type", "Unknown"),
                    "product_code": module.get("product_code", "Unknown"),
                    "revision": module.get("revision", "Unknown"),
                    "serial_number": module.get("serial_number", "Unknown"),
                    "status": module.get("status", "Unknown")
                }
                module_info.append(module_data)
            
            # Update diagnostic data
            self.diagnostic_data["module_info"] = module_info
            
            return module_info
            
        except Exception as e:
            logger.error("Failed to get module info", name=self.name, error=str(e))
            raise
    
    async def get_tag_info(self, tag_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific tag."""
        if not self.connected or not self.driver:
            raise RuntimeError(f"PLC {self.name} not connected")
        
        try:
            tag_info = self.driver.get_tag_info(tag_name)
            
            if tag_info:
                tag_data = {
                    "name": tag_info.get("name", tag_name),
                    "data_type": tag_info.get("data_type", "Unknown"),
                    "dimensions": tag_info.get("dimensions", []),
                    "size": tag_info.get("size", 0),
                    "description": tag_info.get("description", ""),
                    "attributes": tag_info.get("attributes", {}),
                    "scope": tag_info.get("scope", "Unknown")
                }
                
                # Update diagnostic data
                self.diagnostic_data["tag_info"][tag_name] = tag_data
                
                return tag_data
            else:
                raise NotFoundError("Tag", tag_name)
                
        except Exception as e:
            logger.error("Failed to get tag info", name=self.name, tag=tag_name, error=str(e))
            raise
    
    async def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics for the PLC connection."""
        return {
            "connection_status": "connected" if self.connected else "disconnected",
            "connection_attempts": self.connection_attempts,
            "last_connection_time": self.last_connection_time.isoformat() if self.last_connection_time else None,
            "read_operations": self.read_operations,
            "write_operations": self.write_operations,
            "failed_reads": self.failed_reads,
            "failed_writes": self.failed_writes,
            "last_read_time": self.last_read_time.isoformat() if self.last_read_time else None,
            "last_write_time": self.last_write_time.isoformat() if self.last_write_time else None,
            "avg_read_time": round(self.avg_read_time, 4),
            "avg_write_time": round(self.avg_write_time, 4),
            "cache_enabled": self.cache_enabled,
            "cache_size": len(self.tag_cache),
            "success_rate": {
                "reads": round((self.read_operations - self.failed_reads) / max(1, self.read_operations) * 100, 2),
                "writes": round((self.write_operations - self.failed_writes) / max(1, self.write_operations) * 100, 2)
            }
        }
    
    async def get_diagnostic_data(self) -> Dict[str, Any]:
        """Get comprehensive diagnostic data."""
        return {
            **self.diagnostic_data,
            "performance_stats": await self.get_performance_stats(),
            "connection_info": {
                "ip_address": self.ip_address,
                "name": self.name,
                "connected": self.connected,
                "connection_attempts": self.connection_attempts,
                "last_connection_time": self.last_connection_time.isoformat() if self.last_connection_time else None
            }
        }
    
    async def clear_cache(self) -> None:
        """Clear the tag cache."""
        self.tag_cache.clear()
        logger.info("Tag cache cleared", name=self.name)
    
    async def enable_cache(self, enabled: bool = True) -> None:
        """Enable or disable tag caching."""
        self.cache_enabled = enabled
        if not enabled:
            await self.clear_cache()
        
        logger.info("Tag cache enabled", name=self.name, enabled=enabled)
    
    async def set_cache_ttl(self, ttl_seconds: int) -> None:
        """Set cache time-to-live in seconds."""
        self.cache_ttl = ttl_seconds
        logger.info("Cache TTL updated", name=self.name, ttl_seconds=ttl_seconds)
    
    async def _read_tags_from_plc(self, tags: List[str]) -> Dict[str, Any]:
        """Read tags directly from PLC."""
        results = {}
        
        try:
            # Set timeout for read operation
            self.driver.timeout = self.read_timeout
            
            # Batch read all tags at once for efficiency
            responses = self.driver.read(*tags)
            
            # Handle single tag read (returns single response)
            if not isinstance(responses, list):
                responses = [responses]
            
            for tag, response in zip(tags, responses):
                if response.error:
                    results[tag] = {"value": None, "error": response.error}
                    logger.warning(
                        "Tag read error",
                        name=self.name,
                        tag=tag,
                        error=response.error
                    )
                else:
                    results[tag] = {"value": response.value, "error": None}
            
            return results
            
        except Exception as e:
            logger.error("PLC read operation failed", name=self.name, error=str(e))
            raise
    
    async def _validate_tag_values(self, tag_values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate tag values before writing."""
        validated_values = {}
        
        for tag, value in tag_values.items():
            try:
                # Basic validation - could be enhanced with tag type checking
                if value is None:
                    raise ValueError(f"Tag {tag} value cannot be None")
                
                validated_values[tag] = value
                
            except Exception as e:
                logger.error("Tag value validation failed", name=self.name, tag=tag, value=value, error=str(e))
                raise BusinessLogicError(f"Invalid value for tag {tag}: {str(e)}")
        
        return validated_values
    
    async def _get_controller_info(self) -> None:
        """Get and store controller information."""
        try:
            info = self.driver.info
            self.diagnostic_data["controller_info"] = {
                "name": info.get("name", "Unknown"),
                "vendor": info.get("vendor", "Unknown"),
                "product_type": info.get("product_type", "Unknown"),
                "revision": info.get("revision", "Unknown"),
                "serial_number": info.get("serial_number", "Unknown")
            }
        except Exception as e:
            logger.warning("Failed to get controller info", name=self.name, error=str(e))
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
