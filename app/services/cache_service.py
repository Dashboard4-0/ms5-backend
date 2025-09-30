# MS5.0 Floor Dashboard - Phase 10.2 API Optimization
# Cache Service Implementation for Performance Optimization

import json
import asyncio
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta
import logging
from functools import wraps
import hashlib

# Redis imports (if available)
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None

# In-memory cache fallback
from collections import defaultdict, OrderedDict
import threading
import time

logger = logging.getLogger(__name__)

class CacheService:
    """
    Comprehensive caching service for MS5.0 Floor Dashboard API optimization.
    Supports both Redis and in-memory caching with TTL, LRU eviction, and cache warming.
    """
    
    def __init__(self, redis_url: str = None, default_ttl: int = 300):
        """
        Initialize cache service.
        
        Args:
            redis_url: Redis connection URL (optional)
            default_ttl: Default TTL in seconds (5 minutes)
        """
        self.default_ttl = default_ttl
        self.redis_client = None
        self.use_redis = False
        
        # In-memory cache fallback
        self.memory_cache = OrderedDict()
        self.cache_ttl = {}
        self.cache_lock = threading.RLock()
        self.max_memory_size = 1000  # Maximum number of items in memory cache
        
        # Cache statistics
        self.stats = {
            'hits': 0,
            'misses': 0,
            'sets': 0,
            'deletes': 0,
            'expired': 0
        }
        
        # Initialize Redis if available
        if REDIS_AVAILABLE and redis_url:
            try:
                self.redis_client = redis.from_url(redis_url)
                self.use_redis = True
                logger.info("Cache service initialized with Redis")
            except Exception as e:
                logger.warning(f"Failed to connect to Redis: {e}. Using in-memory cache.")
                self.use_redis = False
        else:
            logger.info("Cache service initialized with in-memory cache")
    
    async def _get_from_memory(self, key: str) -> Optional[Any]:
        """Get value from in-memory cache."""
        with self.cache_lock:
            if key in self.memory_cache:
                # Check TTL
                if key in self.cache_ttl and time.time() > self.cache_ttl[key]:
                    # Expired
                    del self.memory_cache[key]
                    del self.cache_ttl[key]
                    self.stats['expired'] += 1
                    return None
                
                # Move to end (LRU)
                value = self.memory_cache.pop(key)
                self.memory_cache[key] = value
                return value
            return None
    
    async def _set_to_memory(self, key: str, value: Any, ttl: int = None) -> bool:
        """Set value in in-memory cache."""
        with self.cache_lock:
            # Remove oldest items if cache is full
            while len(self.memory_cache) >= self.max_memory_size:
                oldest_key = next(iter(self.memory_cache))
                del self.memory_cache[oldest_key]
                if oldest_key in self.cache_ttl:
                    del self.cache_ttl[oldest_key]
            
            # Set value
            self.memory_cache[key] = value
            if ttl:
                self.cache_ttl[key] = time.time() + ttl
            return True
    
    async def _delete_from_memory(self, key: str) -> bool:
        """Delete value from in-memory cache."""
        with self.cache_lock:
            if key in self.memory_cache:
                del self.memory_cache[key]
                if key in self.cache_ttl:
                    del self.cache_ttl[key]
                return True
            return False
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found/expired
        """
        try:
            if self.use_redis:
                value = await self.redis_client.get(key)
                if value is not None:
                    self.stats['hits'] += 1
                    return json.loads(value)
                else:
                    self.stats['misses'] += 1
                    return None
            else:
                value = await self._get_from_memory(key)
                if value is not None:
                    self.stats['hits'] += 1
                    return value
                else:
                    self.stats['misses'] += 1
                    return None
        except Exception as e:
            logger.error(f"Cache get error for key {key}: {e}")
            self.stats['misses'] += 1
            return None
    
    async def set(self, key: str, value: Any, ttl: int = None) -> bool:
        """
        Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds (uses default if not provided)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            ttl = ttl or self.default_ttl
            
            if self.use_redis:
                await self.redis_client.setex(key, ttl, json.dumps(value, default=str))
            else:
                await self._set_to_memory(key, value, ttl)
            
            self.stats['sets'] += 1
            return True
        except Exception as e:
            logger.error(f"Cache set error for key {key}: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """
        Delete value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if deleted, False otherwise
        """
        try:
            if self.use_redis:
                result = await self.redis_client.delete(key)
            else:
                result = await self._delete_from_memory(key)
            
            self.stats['deletes'] += 1
            return result
        except Exception as e:
            logger.error(f"Cache delete error for key {key}: {e}")
            return False
    
    async def exists(self, key: str) -> bool:
        """
        Check if key exists in cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if exists, False otherwise
        """
        try:
            if self.use_redis:
                return await self.redis_client.exists(key) > 0
            else:
                with self.cache_lock:
                    if key in self.memory_cache:
                        # Check TTL
                        if key in self.cache_ttl and time.time() > self.cache_ttl[key]:
                            del self.memory_cache[key]
                            del self.cache_ttl[key]
                            return False
                        return True
                    return False
        except Exception as e:
            logger.error(f"Cache exists error for key {key}: {e}")
            return False
    
    async def clear_pattern(self, pattern: str) -> int:
        """
        Clear all keys matching pattern.
        
        Args:
            pattern: Key pattern (supports * wildcard)
            
        Returns:
            Number of keys deleted
        """
        try:
            if self.use_redis:
                keys = await self.redis_client.keys(pattern)
                if keys:
                    return await self.redis_client.delete(*keys)
                return 0
            else:
                with self.cache_lock:
                    keys_to_delete = []
                    for key in self.memory_cache.keys():
                        if self._match_pattern(key, pattern):
                            keys_to_delete.append(key)
                    
                    for key in keys_to_delete:
                        del self.memory_cache[key]
                        if key in self.cache_ttl:
                            del self.cache_ttl[key]
                    
                    return len(keys_to_delete)
        except Exception as e:
            logger.error(f"Cache clear pattern error for pattern {pattern}: {e}")
            return 0
    
    def _match_pattern(self, key: str, pattern: str) -> bool:
        """Simple pattern matching for in-memory cache."""
        import fnmatch
        return fnmatch.fnmatch(key, pattern)
    
    async def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        total_requests = self.stats['hits'] + self.stats['misses']
        hit_rate = (self.stats['hits'] / total_requests * 100) if total_requests > 0 else 0
        
        stats = {
            'hits': self.stats['hits'],
            'misses': self.stats['misses'],
            'hit_rate': round(hit_rate, 2),
            'sets': self.stats['sets'],
            'deletes': self.stats['deletes'],
            'expired': self.stats['expired'],
            'backend': 'redis' if self.use_redis else 'memory',
            'memory_size': len(self.memory_cache) if not self.use_redis else None
        }
        
        if self.use_redis:
            try:
                info = await self.redis_client.info()
                stats['redis_memory'] = info.get('used_memory_human')
                stats['redis_connected_clients'] = info.get('connected_clients')
            except Exception as e:
                logger.error(f"Error getting Redis info: {e}")
        
        return stats
    
    async def warm_cache(self, cache_items: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Warm cache with multiple items.
        
        Args:
            cache_items: List of dicts with 'key', 'value', and optional 'ttl'
            
        Returns:
            Dictionary with warm results
        """
        results = {'success': 0, 'failed': 0}
        
        for item in cache_items:
            try:
                key = item['key']
                value = item['value']
                ttl = item.get('ttl', self.default_ttl)
                
                if await self.set(key, value, ttl):
                    results['success'] += 1
                else:
                    results['failed'] += 1
            except Exception as e:
                logger.error(f"Cache warm error for item {item}: {e}")
                results['failed'] += 1
        
        return results
    
    async def close(self):
        """Close cache connections."""
        if self.use_redis and self.redis_client:
            await self.redis_client.close()


# Global cache instance
_cache_service = None

def get_cache_service() -> CacheService:
    """Get global cache service instance."""
    global _cache_service
    if _cache_service is None:
        # Initialize with environment variables or defaults
        redis_url = None  # Set from environment
        _cache_service = CacheService(redis_url=redis_url)
    return _cache_service


def cache_key(*args, **kwargs) -> str:
    """
    Generate cache key from arguments.
    
    Args:
        *args: Positional arguments
        **kwargs: Keyword arguments
        
    Returns:
        Generated cache key
    """
    # Sort kwargs for consistent keys
    sorted_kwargs = sorted(kwargs.items())
    
    # Create hash of arguments
    key_data = str(args) + str(sorted_kwargs)
    key_hash = hashlib.md5(key_data.encode()).hexdigest()
    
    return f"ms5:{key_hash}"


def cached(ttl: int = None, key_prefix: str = None):
    """
    Decorator for caching function results.
    
    Args:
        ttl: Time to live in seconds
        key_prefix: Prefix for cache key
        
    Returns:
        Decorated function
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            cache = get_cache_service()
            
            # Generate cache key
            prefix = key_prefix or f"{func.__module__}:{func.__name__}"
            key = f"{prefix}:{cache_key(*args, **kwargs)}"
            
            # Try to get from cache
            cached_result = await cache.get(key)
            if cached_result is not None:
                return cached_result
            
            # Execute function and cache result
            result = await func(*args, **kwargs)
            await cache.set(key, result, ttl)
            
            return result
        
        return wrapper
    return decorator


def invalidate_cache(pattern: str):
    """
    Decorator for invalidating cache after function execution.
    
    Args:
        pattern: Cache key pattern to invalidate
        
    Returns:
        Decorated function
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            result = await func(*args, **kwargs)
            
            # Invalidate cache after successful execution
            cache = get_cache_service()
            await cache.clear_pattern(pattern)
            
            return result
        
        return wrapper
    return decorator


class RequestBatchingService:
    """
    Service for batching multiple API requests to improve performance.
    """
    
    def __init__(self, batch_size: int = 10, batch_timeout: float = 0.1):
        """
        Initialize request batching service.
        
        Args:
            batch_size: Maximum number of requests per batch
            batch_timeout: Maximum time to wait for batch completion (seconds)
        """
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.pending_requests = {}
        self.batch_lock = asyncio.Lock()
    
    async def batch_request(self, request_id: str, request_func, *args, **kwargs):
        """
        Add request to batch or execute immediately.
        
        Args:
            request_id: Unique identifier for the request
            request_func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Request result
        """
        async with self.batch_lock:
            # Check if we can batch this request
            if len(self.pending_requests) < self.batch_size:
                # Add to batch
                future = asyncio.Future()
                self.pending_requests[request_id] = {
                    'future': future,
                    'func': request_func,
                    'args': args,
                    'kwargs': kwargs
                }
                
                # Set timeout for batch execution
                if len(self.pending_requests) == 1:
                    asyncio.create_task(self._execute_batch_after_timeout())
                
                # Wait for batch execution
                return await future
            else:
                # Execute immediately
                return await request_func(*args, **kwargs)
    
    async def _execute_batch_after_timeout(self):
        """Execute batch after timeout."""
        await asyncio.sleep(self.batch_timeout)
        await self._execute_batch()
    
    async def _execute_batch(self):
        """Execute all pending requests in batch."""
        if not self.pending_requests:
            return
        
        # Create tasks for all pending requests
        tasks = []
        for request_id, request_data in self.pending_requests.items():
            task = asyncio.create_task(
                self._execute_single_request(request_id, request_data)
            )
            tasks.append(task)
        
        # Clear pending requests
        self.pending_requests.clear()
        
        # Wait for all tasks to complete
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _execute_single_request(self, request_id: str, request_data: Dict[str, Any]):
        """Execute single request and resolve future."""
        try:
            result = await request_data['func'](
                *request_data['args'],
                **request_data['kwargs']
            )
            request_data['future'].set_result(result)
        except Exception as e:
            request_data['future'].set_exception(e)


# Global batching service
_batching_service = RequestBatchingService()

def get_batching_service() -> RequestBatchingService:
    """Get global batching service instance."""
    return _batching_service


def batched_request(request_id: str = None):
    """
    Decorator for batching requests.
    
    Args:
        request_id: Request identifier (auto-generated if not provided)
        
    Returns:
        Decorated function
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate request ID if not provided
            req_id = request_id or f"{func.__name__}:{cache_key(*args, **kwargs)}"
            
            batching_service = get_batching_service()
            return await batching_service.batch_request(req_id, func, *args, **kwargs)
        
        return wrapper
    return decorator


class RateLimiter:
    """
    Rate limiting service for API endpoints.
    """
    
    def __init__(self, requests_per_minute: int = 60, burst_size: int = 10):
        """
        Initialize rate limiter.
        
        Args:
            requests_per_minute: Maximum requests per minute
            burst_size: Maximum burst requests
        """
        self.requests_per_minute = requests_per_minute
        self.burst_size = burst_size
        self.request_times = defaultdict(list)
        self.burst_tokens = defaultdict(lambda: burst_size)
        self.lock = asyncio.Lock()
    
    async def is_allowed(self, identifier: str) -> bool:
        """
        Check if request is allowed for identifier.
        
        Args:
            identifier: Unique identifier (user ID, IP, etc.)
            
        Returns:
            True if allowed, False if rate limited
        """
        async with self.lock:
            now = time.time()
            
            # Clean old requests (older than 1 minute)
            self.request_times[identifier] = [
                req_time for req_time in self.request_times[identifier]
                if now - req_time < 60
            ]
            
            # Check if under rate limit
            if len(self.request_times[identifier]) >= self.requests_per_minute:
                return False
            
            # Check burst limit
            if self.burst_tokens[identifier] <= 0:
                return False
            
            # Allow request
            self.request_times[identifier].append(now)
            self.burst_tokens[identifier] -= 1
            
            # Refill burst tokens (1 token per 6 seconds)
            if now % 6 < 1:  # Approximate refill
                self.burst_tokens[identifier] = min(
                    self.burst_size,
                    self.burst_tokens[identifier] + 1
                )
            
            return True
    
    async def get_remaining_requests(self, identifier: str) -> int:
        """
        Get remaining requests for identifier.
        
        Args:
            identifier: Unique identifier
            
        Returns:
            Number of remaining requests
        """
        async with self.lock:
            now = time.time()
            
            # Clean old requests
            self.request_times[identifier] = [
                req_time for req_time in self.request_times[identifier]
                if now - req_time < 60
            ]
            
            return max(0, self.requests_per_minute - len(self.request_times[identifier]))


# Global rate limiter
_rate_limiter = RateLimiter()

def get_rate_limiter() -> RateLimiter:
    """Get global rate limiter instance."""
    return _rate_limiter


def rate_limited(requests_per_minute: int = 60, identifier_func=None):
    """
    Decorator for rate limiting endpoints.
    
    Args:
        requests_per_minute: Maximum requests per minute
        identifier_func: Function to get identifier from request
        
    Returns:
        Decorated function
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get identifier (default to 'default')
            if identifier_func:
                identifier = identifier_func(*args, **kwargs)
            else:
                identifier = 'default'
            
            rate_limiter = get_rate_limiter()
            
            if not await rate_limiter.is_allowed(identifier):
                from fastapi import HTTPException
                raise HTTPException(
                    status_code=429,
                    detail="Rate limit exceeded. Please try again later."
                )
            
            return await func(*args, **kwargs)
        
        return wrapper
    return decorator


# Cache warming functions for common data
async def warm_common_cache():
    """Warm cache with commonly accessed data."""
    cache = get_cache_service()
    
    # Common cache items to warm
    cache_items = [
        {
            'key': 'production_lines:active',
            'value': [],  # Will be populated by actual data
            'ttl': 300  # 5 minutes
        },
        {
            'key': 'equipment_config:all',
            'value': {},  # Will be populated by actual data
            'ttl': 600  # 10 minutes
        },
        {
            'key': 'user_roles:all',
            'value': [],  # Will be populated by actual data
            'ttl': 1800  # 30 minutes
        }
    ]
    
    results = await cache.warm_cache(cache_items)
    logger.info(f"Cache warming completed: {results}")
    return results


# Performance monitoring
class PerformanceMonitor:
    """Monitor API performance metrics."""
    
    def __init__(self):
        self.metrics = defaultdict(list)
        self.lock = asyncio.Lock()
    
    async def record_request(self, endpoint: str, duration: float, status_code: int):
        """Record request metrics."""
        async with self.lock:
            self.metrics[endpoint].append({
                'duration': duration,
                'status_code': status_code,
                'timestamp': time.time()
            })
            
            # Keep only last 1000 requests per endpoint
            if len(self.metrics[endpoint]) > 1000:
                self.metrics[endpoint] = self.metrics[endpoint][-1000:]
    
    async def get_metrics(self, endpoint: str = None) -> Dict[str, Any]:
        """Get performance metrics."""
        async with self.lock:
            if endpoint:
                metrics = self.metrics.get(endpoint, [])
            else:
                metrics = []
                for ep_metrics in self.metrics.values():
                    metrics.extend(ep_metrics)
            
            if not metrics:
                return {}
            
            durations = [m['duration'] for m in metrics]
            status_codes = [m['status_code'] for m in metrics]
            
            return {
                'total_requests': len(metrics),
                'avg_duration': sum(durations) / len(durations),
                'min_duration': min(durations),
                'max_duration': max(durations),
                'p95_duration': sorted(durations)[int(len(durations) * 0.95)],
                'p99_duration': sorted(durations)[int(len(durations) * 0.99)],
                'success_rate': len([s for s in status_codes if s < 400]) / len(status_codes) * 100,
                'error_rate': len([s for s in status_codes if s >= 400]) / len(status_codes) * 100
            }


# Global performance monitor
_performance_monitor = PerformanceMonitor()

def get_performance_monitor() -> PerformanceMonitor:
    """Get global performance monitor instance."""
    return _performance_monitor


def monitor_performance(endpoint: str = None):
    """
    Decorator for monitoring endpoint performance.
    
    Args:
        endpoint: Endpoint name (auto-detected if not provided)
        
    Returns:
        Decorated function
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            
            try:
                result = await func(*args, **kwargs)
                status_code = 200
                return result
            except Exception as e:
                status_code = 500
                raise
            finally:
                duration = time.time() - start_time
                ep_name = endpoint or f"{func.__module__}:{func.__name__}"
                
                monitor = get_performance_monitor()
                await monitor.record_request(ep_name, duration, status_code)
        
        return wrapper
    return decorator
