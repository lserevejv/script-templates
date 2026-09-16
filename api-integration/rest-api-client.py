"""
REST API Client Template
Production-ready REST API client with authentication, rate limiting, retries, and error handling.

Features:
- Automatic retry with exponential backoff
- Rate limiting and circuit breaker
- Comprehensive error handling
- Request/response logging
- Support for multiple authentication methods
- Connection pooling for performance
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import hmac
import hashlib
import json
import threading
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AuthType(Enum):
    """Authentication types supported"""
    API_KEY = "api_key"
    BEARER_TOKEN = "bearer_token"
    BASIC_AUTH = "basic_auth"
    OAUTH2 = "oauth2"
    CUSTOM = "custom"


@dataclass
class RetryConfig:
    """Retry configuration"""
    max_retries: int = 3
    backoff_factor: float = 1
    status_forcelist: List[int] = None
    allowed_methods: List[str] = None
    
    def __post_init__(self):
        if self.status_forcelist is None:
            self.status_forcelist = [429, 500, 502, 503, 504]
        if self.allowed_methods is None:
            self.allowed_methods = ["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE"]


@dataclass
class RateLimitConfig:
    """Rate limiting configuration"""
    calls_per_second: int = 10
    calls_per_minute: int = 100
    calls_per_hour: int = 1000


class CircuitBreaker:
    """Circuit breaker to prevent cascading failures"""
    
    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.last_failure_time = None
        self.state = 'closed'  # closed, open, half-open
        self.lock = threading.Lock()
    
    def call(self, func):
        """Execute function with circuit breaker protection"""
        import threading
        
        with self.lock:
            if self.state == 'open':
                if time.time() - self.last_failure_time > self.timeout:
                    self.state = 'half-open'
                else:
                    raise Exception("Circuit breaker is OPEN - service unavailable")
        
        try:
            result = func()
            if self.state == 'half-open':
                self.state = 'closed'
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = 'open'
            raise


class RateLimiter:
    """Rate limiter to prevent API abuse"""
    
    def __init__(self, config: RateLimitConfig):
        self.config = config
        self.calls = []
        self.lock = threading.Lock()
    
    def wait(self):
        """Wait if rate limit would be exceeded"""
        import threading
        
        with self.lock:
            now = time.time()
            
            # Remove calls older than 1 hour
            self.calls = [call_time for call_time in self.calls 
                          if now - call_time < 3600]
            
            # Check per-second limit
            second_calls = [call_time for call_time in self.calls 
                          if now - call_time < 1]
            if len(second_calls) >= self.config.calls_per_second:
                sleep_time = 1.0 / self.config.calls_per_second
                time.sleep(sleep_time)
            
            # Check per-minute limit
            minute_calls = [call_time for call_time in self.calls 
                           if now - call_time < 60]
            if len(minute_calls) >= self.config.calls_per_minute:
                sleep_time = 60.0 / self.config.calls_per_minute
                time.sleep(sleep_time)
            
            # Check per-hour limit
            if len(self.calls) >= self.config.calls_per_hour:
                sleep_time = 3600.0 / self.config.calls_per_hour
                time.sleep(sleep_time)
            
            self.calls.append(now)


class RESTClient:
    """Production-ready REST API client"""
    
    def __init__(
        self,
        base_url: str,
        auth_type: AuthType = AuthType.API_KEY,
        auth_credentials: Optional[Dict[str, str]] = None,
        retry_config: Optional[RetryConfig] = None,
        rate_limit_config: Optional[RateLimitConfig] = None,
        enable_circuit_breaker: bool = True,
        default_headers: Optional[Dict[str, str]] = None
    ):
        self.base_url = base_url.rstrip('/')
        self.auth_type = auth_type
        self.auth_credentials = auth_credentials or {}
        self.retry_config = retry_config or RetryConfig()
        self.rate_limit_config = rate_limit_config or RateLimitConfig()
        self.enable_circuit_breaker = enable_circuit_breaker
        self.default_headers = default_headers or {}
        
        # Initialize components
        self.session = self._create_session()
        self.rate_limiter = RateLimiter(self.rate_limit_config)
        self.circuit_breaker = CircuitBreaker() if enable_circuit_breaker else None
        
        logger.info(f"REST client initialized for {base_url}")
    
    def _create_session(self) -> requests.Session:
        """Create configured session with retry logic"""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.retry_config.max_retries,
            backoff_factor=self.retry_config.backoff_factor,
            status_forcelist=self.retry_config.status_forcelist,
            allowed_methods=self.retry_config.allowed_methods
        )
        
        # Mount adapter with connection pooling
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=10
        )
        
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        return session
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Generate authentication headers"""
        if self.auth_type == AuthType.API_KEY:
            api_key = self.auth_credentials.get('api_key')
            return {'Authorization': f'Bearer {api_key}'}
        
        elif self.auth_type == AuthType.BEARER_TOKEN:
            token = self.auth_credentials.get('token')
            return {'Authorization': f'Bearer {token}'}
        
        elif self.auth_type == AuthType.BASIC_AUTH:
            import base64
            username = self.auth_credentials.get('username')
            password = self.auth_credentials.get('password')
            credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
            return {'Authorization': f'Basic {credentials}'}
        
        elif self.auth_type == AuthType.CUSTOM:
            return self.auth_credentials.get('custom_headers', {})
        
        return {}
    
    def _build_url(self, endpoint: str) -> str:
        """Build full URL from endpoint"""
        return f"{self.base_url}/{endpoint.lstrip('/')}"
    
    def _log_request(self, method: str, url: str, **kwargs):
        """Log request details"""
        logger.info(f"API Request: {method} {url}")
        if 'params' in kwargs:
            logger.debug(f"Params: {kwargs['params']}")
        if 'json' in kwargs:
            logger.debug(f"Body keys: {list(kwargs['json'].keys())}")
    
    def _log_response(self, response: requests.Response):
        """Log response details"""
        logger.info(f"API Response: {response.status_code} {len(response.content)} bytes")
        if response.status_code >= 400:
            logger.warning(f"Error response: {response.text[:200]}")
    
    def request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated API request with all safety features"""
        # Apply rate limiting
        self.rate_limiter.wait()
        
        # Build URL and headers
        url = self._build_url(endpoint)
        headers = {**self.default_headers, **self._get_auth_headers()}
        
        # Log request
        self._log_request(method, url, **kwargs)
        
        # Apply circuit breaker if enabled
        if self.circuit_breaker:
            def make_request():
                return self.session.request(
                    method,
                    url,
                    headers=headers,
                    **kwargs
                )
            
            response = self.circuit_breaker.call(make_request)
        else:
            response = self.session.request(
                method,
                url,
                headers=headers,
                **kwargs
            )
        
        # Log response
        self._log_response(response)
        
        # Raise for HTTP errors
        response.raise_for_status()
        
        return response
    
    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """GET request"""
        response = self.request('GET', endpoint, params=params)
        return response.json()
    
    def post(self, endpoint: str, data: Optional[Dict] = None, json: Optional[Dict] = None) -> Dict[str, Any]:
        """POST request"""
        response = self.request('POST', endpoint, data=data, json=json)
        return response.json()
    
    def put(self, endpoint: str, data: Optional[Dict] = None, json: Optional[Dict] = None) -> Dict[str, Any]:
        """PUT request"""
        response = self.request('PUT', endpoint, data=data, json=json)
        return response.json()
    
    def delete(self, endpoint: str) -> Dict[str, Any]:
        """DELETE request"""
        response = self.request('DELETE', endpoint)
        return response.json() if response.content else {}
    
    def patch(self, endpoint: str, data: Optional[Dict] = None, json: Optional[Dict] = None) -> Dict[str, Any]:
        """PATCH request"""
        response = self.request('PATCH', endpoint, data=data, json=json)
        return response.json()


# Example usage
if __name__ == "__main__":
    # Initialize client
    client = RESTClient(
        base_url="https://api.example.com",
        auth_type=AuthType.API_KEY,
        auth_credentials={"api_key": "your_api_key_here"},
        retry_config=RetryConfig(max_retries=3),
        rate_limit_config=RateLimitConfig(calls_per_second=10)
    )
    
    # Make requests
    try:
        # GET request
        users = client.get("/users")
        print(f"Found {len(users)} users")
        
        # POST request
        new_user = client.post("/users", json={"name": "John Doe", "email": "john@example.com"})
        print(f"Created user: {new_user['id']}")
        
        # PUT request
        updated_user = client.put(f"/users/{new_user['id']}", json={"name": "Jane Doe"})
        print(f"Updated user: {updated_user['name']}")
        
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error: {e}")
    except Exception as e:
        logger.error(f"Request failed: {e}")