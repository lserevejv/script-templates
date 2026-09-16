"""
Health Check Template
Production-ready health check system for monitoring services and dependencies.

Features:
- Service health monitoring
- Database connection checks
- External API availability
- Custom health checks
- Response time tracking
- Alert thresholds
- JSON health endpoint
"""

import requests
import time
import logging
from typing import Dict, Any, Callable, List
from dataclasses import dataclass
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"


@dataclass
class HealthCheck:
    name: str
    check_func: Callable
    timeout: int = 5
    critical: bool = True


class HealthMonitor:
    def __init__(self):
        self.checks: List[HealthCheck] = []
        self.results: Dict[str, Dict[str, Any]] = {}
        
        logger.info("Health monitor initialized")
    
    def add_check(self, check: HealthCheck):
        self.checks.append(check)
        logger.info(f"Added health check: {check.name}")
    
    def run_checks(self) -> Dict[str, Any]:
        overall_status = HealthStatus.HEALTHY
        check_results = {}
        
        for check in self.checks:
            start_time = time.time()
            
            try:
                result = check.check_func()
                duration = time.time() - start_time
                
                check_results[check.name] = {
                    'status': 'healthy',
                    'duration': duration,
                    'critical': check.critical
                }
                
            except Exception as e:
                duration = time.time() - start_time
                check_results[check.name] = {
                    'status': 'unhealthy',
                    'error': str(e),
                    'duration': duration,
                    'critical': check.critical
                }
                
                if check.critical:
                    overall_status = HealthStatus.UNHEALTHY
                else:
                    overall_status = HealthStatus.DEGRADED
        
        self.results = check_results
        
        return {
            'status': overall_status.value,
            'timestamp': time.time(),
            'checks': check_results
        }
    
    def get_json_response(self) -> str:
        import json
        return json.dumps(self.run_checks(), indent=2)


# Example health check functions
def check_database():
    """Check database connection"""
    # Replace with actual database check
    import sqlite3
    conn = sqlite3.connect(':memory:')
    conn.execute('SELECT 1')
    conn.close()
    return True


def check_redis():
    """Check Redis connection"""
    # Replace with actual Redis check
    # Try: import redis; r = redis.Redis(); r.ping()
    return True


def check_external_api():
    """Check external API availability"""
    response = requests.get('https://api.example.com/health', timeout=5)
    response.raise_for_status()
    return True


def check_disk_space():
    """Check disk space"""
    import shutil
    usage = shutil.disk_usage('/')
    if usage.percent > 90:
        raise Exception(f"Disk usage too high: {usage.percent}%")
    return True


if __name__ == "__main__":
    monitor = HealthMonitor()
    
    monitor.add_check(HealthCheck(name="database", check_func=check_database, critical=True))
    monitor.add_check(HealthCheck(name="redis", check_func=check_redis, critical=True))
    monitor.add_check(HealthCheck(name="external_api", check_func=check_external_api, critical=False))
    monitor.add_check(HealthCheck(name="disk_space", check_func=check_disk_space, critical=True))
    
    # Run health checks
    health_status = monitor.run_checks()
    print(health_status)