"""
MS5.0 Floor Dashboard - SLCDriver Service

This module provides a comprehensive SLCDriver service for SLC 5/05
PLC communication with enhanced features for production management integration.
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from uuid import UUID
import structlog

from pycomm3 import SLCDriver
from tenacity import retry, stop_after_attempt, wait_exponential

from app.database import execute_query, execute_scalar, execute_update
from app.utils.exceptions import BusinessLogicError, NotFoundError

logger = structlog.get_logger()


class SLCDriverService:
    """Enhanced SLCDriver service for SLC 5/05 PLC communication."""
    
    def __init__(self, ip_address: str, name: str = "SLC PLC"):
        """Initialize SLCDriver service."""
        self.ip_address = ip_address
        self.name = name
        self.driver: Optional[SLCDriver] = None
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
        
        # Address cache for performance
        self.address_cache = {}
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
            "address_info": {},
            "connection_status": "disconnected",
            "last_error": None,
            "error_count": 0
        }
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    async def connect(self) -> bool:
        """Connect to SLC PLC with enhanced error handling."""
        try:
            if self.connected:
                return True
            
            logger.info("Connecting to SLC PLC", name=self.name, ip=self.ip_address)
            
            # Create driver instance
            self.driver = SLCDriver(self.ip_address)
            
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
                    "SLC PLC connected successfully",
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
                "SLC PLC connection failed",
                name=self.name,
                ip=self.ip_address,
                attempt=self.connection_attempts,
                error=str(e)
            )
            
            if self.connection_attempts >= self.max_connection_attempts:
                raise BusinessLogicError(f"Failed to connect to PLC {self.name} after {self.max_connection_attempts} attempts")
            
            raise
    
    async def disconnect(self) -> None:
        """Disconnect from SLC PLC."""
        try:
            if self.driver and self.connected:
                self.driver.close()
                self.connected = False
                self.diagnostic_data["connection_status"] = "disconnected"
                
                logger.info("SLC PLC disconnected", name=self.name)
                
        except Exception as e:
            logger.error("Error disconnecting from SLC PLC", name=self.name, error=str(e))
    
    async def read_addresses(self, addresses: List[str], use_cache: bool = True) -> Dict[str, Any]:
        """Read multiple addresses from SLC PLC with caching and performance monitoring."""
        if not self.connected or not self.driver:
            raise RuntimeError(f"PLC {self.name} not connected")
        
        start_time = time.time()
        results = {}
        uncached_addresses = []
        
        try:
            # Check cache for addresses
            if use_cache and self.cache_enabled:
                for address in addresses:
                    if address in self.address_cache:
                        cache_entry = self.address_cache[address]
                        if time.time() - cache_entry["timestamp"] < self.cache_ttl:
                            results[address] = cache_entry["data"]
                        else:
                            uncached_addresses.append(address)
                    else:
                        uncached_addresses.append(address)
            else:
                uncached_addresses = addresses
            
            # Read uncached addresses from PLC
            if uncached_addresses:
                plc_results = await self._read_addresses_from_plc(uncached_addresses)
                
                # Update cache
                if use_cache and self.cache_enabled:
                    for address, data in plc_results.items():
                        self.address_cache[address] = {
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
                "Addresses read successfully",
                name=self.name,
                total_addresses=len(addresses),
                cached_addresses=len(addresses) - len(uncached_addresses),
                read_time=read_time
            )
            
            return results
            
        except Exception as e:
            self.failed_reads += 1
            self.diagnostic_data["last_error"] = str(e)
            self.diagnostic_data["error_count"] += 1
            
            logger.error("Failed to read addresses from SLC PLC", name=self.name, error=str(e))
            raise
    
    async def write_addresses(self, address_values: Dict[str, Any]) -> Dict[str, bool]:
        """Write multiple addresses to SLC PLC with validation."""
        if not self.connected or not self.driver:
            raise RuntimeError(f"PLC {self.name} not connected")
        
        start_time = time.time()
        results = {}
        
        try:
            # Validate address values
            validated_values = await self._validate_address_values(address_values)
            
            # Write addresses to PLC
            for address, value in validated_values.items():
                try:
                    # Set timeout for write operation
                    self.driver.timeout = self.write_timeout
                    
                    # Write address
                    response = self.driver.write(address, value)
                    
                    if response.error:
                        results[address] = False
                        logger.warning(
                            "Address write error",
                            name=self.name,
                            address=address,
                            value=value,
                            error=response.error
                        )
                    else:
                        results[address] = True
                        
                        # Update cache if address exists
                        if address in self.address_cache:
                            self.address_cache[address] = {
                                "data": {"value": value, "error": None},
                                "timestamp": time.time()
                            }
                        
                        logger.debug("Address written successfully", name=self.name, address=address, value=value)
                
                except Exception as e:
                    results[address] = False
                    logger.error("Failed to write address", name=self.name, address=address, error=str(e))
            
            # Update performance metrics
            write_time = time.time() - start_time
            self.write_operations += 1
            self.last_write_time = datetime.utcnow()
            self.avg_write_time = (self.avg_write_time * (self.write_operations - 1) + write_time) / self.write_operations
            
            successful_writes = sum(1 for success in results.values() if success)
            if successful_writes < len(address_values):
                self.failed_writes += 1
            
            logger.info(
                "Address write operation completed",
                name=self.name,
                total_addresses=len(address_values),
                successful_writes=successful_writes,
                write_time=write_time
            )
            
            return results
            
        except Exception as e:
            self.failed_writes += 1
            self.diagnostic_data["last_error"] = str(e)
            self.diagnostic_data["error_count"] += 1
            
            logger.error("Failed to write addresses to SLC PLC", name=self.name, error=str(e))
            raise
    
    async def read_bit(self, word_address: str, bit_index: int) -> bool:
        """Read a specific bit from an SLC word."""
        try:
            # Read the word
            word_result = await self.read_addresses([word_address])
            
            if word_address in word_result and word_result[word_address].get("error") is None:
                word_value = word_result[word_address]["value"]
                
                # Extract bit
                if isinstance(word_value, int):
                    return bool((word_value >> bit_index) & 1)
                else:
                    logger.warning("Invalid word value for bit extraction", name=self.name, word_address=word_address, value=word_value)
                    return False
            else:
                logger.warning("Failed to read word for bit extraction", name=self.name, word_address=word_address)
                return False
                
        except Exception as e:
            logger.error("Failed to read bit", name=self.name, word_address=word_address, bit_index=bit_index, error=str(e))
            return False
    
    async def write_bit(self, word_address: str, bit_index: int, value: bool) -> bool:
        """Write a specific bit to an SLC word."""
        try:
            # Read current word value
            word_result = await self.read_addresses([word_address])
            
            if word_address in word_result and word_result[word_address].get("error") is None:
                current_value = word_result[word_address]["value"]
                
                if isinstance(current_value, int):
                    # Modify the specific bit
                    if value:
                        new_value = current_value | (1 << bit_index)
                    else:
                        new_value = current_value & ~(1 << bit_index)
                    
                    # Write the modified word
                    write_result = await self.write_addresses({word_address: new_value})
                    return write_result.get(word_address, False)
                else:
                    logger.warning("Invalid word value for bit modification", name=self.name, word_address=word_address, value=current_value)
                    return False
            else:
                logger.warning("Failed to read word for bit modification", name=self.name, word_address=word_address)
                return False
                
        except Exception as e:
            logger.error("Failed to write bit", name=self.name, word_address=word_address, bit_index=bit_index, value=value, error=str(e))
            return False
    
    async def read_word(self, word_address: str) -> Optional[int]:
        """Read a word (16-bit integer) from SLC PLC."""
        try:
            result = await self.read_addresses([word_address])
            
            if word_address in result and result[word_address].get("error") is None:
                return result[word_address]["value"]
            
            return None
            
        except Exception as e:
            logger.error("Failed to read word", name=self.name, word_address=word_address, error=str(e))
            return None
    
    async def write_word(self, word_address: str, value: int) -> bool:
        """Write a word (16-bit integer) to SLC PLC."""
        try:
            result = await self.write_addresses({word_address: value})
            return result.get(word_address, False)
            
        except Exception as e:
            logger.error("Failed to write word", name=self.name, word_address=word_address, value=value, error=str(e))
            return False
    
    async def read_dword(self, dword_address: str) -> Optional[int]:
        """Read a double word (32-bit integer) from SLC PLC."""
        try:
            result = await self.read_addresses([dword_address])
            
            if dword_address in result and result[dword_address].get("error") is None:
                return result[dword_address]["value"]
            
            return None
            
        except Exception as e:
            logger.error("Failed to read dword", name=self.name, dword_address=dword_address, error=str(e))
            return None
    
    async def write_dword(self, dword_address: str, value: int) -> bool:
        """Write a double word (32-bit integer) to SLC PLC."""
        try:
            result = await self.write_addresses({dword_address: value})
            return result.get(dword_address, False)
            
        except Exception as e:
            logger.error("Failed to write dword", name=self.name, dword_address=dword_address, value=value, error=str(e))
            return False
    
    async def read_float(self, float_address: str) -> Optional[float]:
        """Read a float from SLC PLC."""
        try:
            result = await self.read_addresses([float_address])
            
            if float_address in result and result[float_address].get("error") is None:
                return result[float_address]["value"]
            
            return None
            
        except Exception as e:
            logger.error("Failed to read float", name=self.name, float_address=float_address, error=str(e))
            return None
    
    async def write_float(self, float_address: str, value: float) -> bool:
        """Write a float to SLC PLC."""
        try:
            result = await self.write_addresses({float_address: value})
            return result.get(float_address, False)
            
        except Exception as e:
            logger.error("Failed to write float", name=self.name, float_address=float_address, value=value, error=str(e))
            return False
    
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
            "cache_size": len(self.address_cache),
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
        """Clear the address cache."""
        self.address_cache.clear()
        logger.info("Address cache cleared", name=self.name)
    
    async def enable_cache(self, enabled: bool = True) -> None:
        """Enable or disable address caching."""
        self.cache_enabled = enabled
        if not enabled:
            await self.clear_cache()
        
        logger.info("Address cache enabled", name=self.name, enabled=enabled)
    
    async def set_cache_ttl(self, ttl_seconds: int) -> None:
        """Set cache time-to-live in seconds."""
        self.cache_ttl = ttl_seconds
        logger.info("Cache TTL updated", name=self.name, ttl_seconds=ttl_seconds)
    
    async def _read_addresses_from_plc(self, addresses: List[str]) -> Dict[str, Any]:
        """Read addresses directly from PLC."""
        results = {}
        
        try:
            # Set timeout for read operation
            self.driver.timeout = self.read_timeout
            
            # Batch read all addresses at once for efficiency
            responses = self.driver.read(*addresses)
            
            # Handle single address read (returns single response)
            if not isinstance(responses, list):
                responses = [responses]
            
            for address, response in zip(addresses, responses):
                if response.error:
                    results[address] = {"value": None, "error": response.error}
                    logger.warning(
                        "Address read error",
                        name=self.name,
                        address=address,
                        error=response.error
                    )
                else:
                    results[address] = {"value": response.value, "error": None}
            
            return results
            
        except Exception as e:
            logger.error("PLC read operation failed", name=self.name, error=str(e))
            raise
    
    async def _validate_address_values(self, address_values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate address values before writing."""
        validated_values = {}
        
        for address, value in address_values.items():
            try:
                # Basic validation - could be enhanced with address type checking
                if value is None:
                    raise ValueError(f"Address {address} value cannot be None")
                
                validated_values[address] = value
                
            except Exception as e:
                logger.error("Address value validation failed", name=self.name, address=address, value=value, error=str(e))
                raise BusinessLogicError(f"Invalid value for address {address}: {str(e)}")
        
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
