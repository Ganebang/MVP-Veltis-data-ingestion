"""
Base connector class for all data sources.

Provides common interface for data fetching with retry logic,
error handling, and caching.
"""

from abc import ABC, abstractmethod
import logging
import time
from typing import Optional, Any, List
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """Abstract base class for all data source connectors."""
    
    def __init__(self, name: str, base_url: str, timeout: int = 30, 
                 max_retries: int = 3, retry_backoff: int = 5):
        """
        Initialize base connector.
        
        Args:
            name: Connector name (e.g., 'DataGouv', 'HAS')
            base_url: API base URL
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries
            retry_backoff: Backoff time in seconds between retries
        """
        self.name = name
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.session = self._create_session()
        self.last_fetch_time = None
        self.cache = {}
    
    def _create_session(self) -> requests.Session:
        """
        Create requests session with retry strategy.
        
        Returns:
            Configured requests Session object
        """
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.retry_backoff,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _make_request(self, method: str, url: str, **kwargs) -> Optional[Any]:
        """
        Make HTTP request with error handling and retries.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full URL for request
            **kwargs: Additional arguments to pass to session method
            
        Returns:
            Response object or None if failed
        """
        try:
            logger.debug(f"[{self.name}] Making {method} request to {url}")
            response = self.session.request(
                method,
                url,
                timeout=self.timeout,
                **kwargs
            )
            response.raise_for_status()
            self.last_fetch_time = datetime.utcnow()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"[{self.name}] Request failed: {e}")
            return None
    
    def get(self, url: str, **kwargs) -> Optional[Any]:
        """Make GET request."""
        return self._make_request("GET", url, **kwargs)
    
    def post(self, url: str, **kwargs) -> Optional[Any]:
        """Make POST request."""
        return self._make_request("POST", url, **kwargs)
    
    @abstractmethod
    def fetch_data(self, **kwargs) -> Optional[Any]:
        """
        Fetch data from source.
        
        Must be implemented by subclasses.
        
        Returns:
            Data in appropriate format (DataFrame, dict, etc.)
        """
        pass
    
    @abstractmethod
    def validate_response(self, response: Any) -> bool:
        """
        Validate response data.
        
        Must be implemented by subclasses.
        
        Args:
            response: Response data to validate
            
        Returns:
            True if valid, False otherwise
        """
        pass
    
    def cache_data(self, key: str, data: Any, ttl_hours: int = 24):
        """
        Cache fetched data.
        
        Args:
            key: Cache key
            data: Data to cache
            ttl_hours: Time to live in hours
        """
        self.cache[key] = {
            'data': data,
            'timestamp': datetime.utcnow(),
            'ttl': timedelta(hours=ttl_hours)
        }
        logger.debug(f"[{self.name}] Cached data with key: {key}")
    
    def get_cached_data(self, key: str) -> Optional[Any]:
        """
        Retrieve cached data if not expired.
        
        Args:
            key: Cache key
            
        Returns:
            Cached data if valid, None otherwise
        """
        if key not in self.cache:
            return None
        
        cache_entry = self.cache[key]
        elapsed = datetime.utcnow() - cache_entry['timestamp']
        
        if elapsed > cache_entry['ttl']:
            del self.cache[key]
            logger.debug(f"[{self.name}] Cache expired for key: {key}")
            return None
        
        logger.debug(f"[{self.name}] Using cached data for key: {key}")
        return cache_entry['data']
    
    def clear_cache(self):
        """Clear all cached data."""
        self.cache.clear()
        logger.debug(f"[{self.name}] Cache cleared")
    
    def close(self):
        """Close session."""
        self.session.close()
        logger.debug(f"[{self.name}] Session closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
