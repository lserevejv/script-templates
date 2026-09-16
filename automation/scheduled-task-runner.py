"""
Scheduled Task Runner Template
Production-ready task scheduler with error handling and logging.
"""

import schedule
import time
import threading
import logging
from typing import Callable, Dict, Any
from dataclasses import dataclass
from datetime import datetime
import signal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class TaskConfig:
    name: str
    func: Callable
    schedule: str
    timeout: int = 300
    max_retries: int = 3
    enabled: bool = True


class ScheduledTaskRunner:
    def __init__(self):
        self.tasks: Dict[str, TaskConfig] = {}
        self.running = True
        self.execution_history = []
        self.thread = None
        
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
        
        logger.info("Scheduled task runner initialized")
    
    def _shutdown(self, signum, frame):
        logger.info("Shutting down...")
        self.running = False
    
    def add_task(self, config: TaskConfig):
        self.tasks[config.name] = config
        logger.info(f"Added task: {config.name}")
    
    def _execute_task(self, config: TaskConfig):
        if not config.enabled:
            return
        
        for attempt in range(config.max_retries):
            try:
                result = config.func()
                logger.info(f"Task {config.name} completed successfully")
                return
                
            except Exception as e:
                logger.error(f"Task {config.name} failed: {e}")
                if attempt < config.max_retries - 1:
                    time.sleep(2 ** attempt)
    
    def _setup_schedule(self):
        schedule.clear()
        
        for task_name, config in self.tasks.items():
            if config.enabled:
                if "every" in config.schedule.lower():
                    parts = config.schedule.lower().split()
                    if "minute" in parts:
                        interval = int(parts[1])
                        schedule.every(interval).minutes.do(self._execute_task, config)
                    elif "hour" in parts:
                        interval = int(parts[1])
                        schedule.every(interval).hours.do(self._execute_task, config)
                    elif "day" in parts:
                        schedule.every().day.do(self._execute_task, config)
        
        logger.info(f"Setup {len(schedule.jobs)} scheduled tasks")
    
    def run(self):
        logger.info("Starting task scheduler")
        self._setup_schedule()
        
        while self.running:
            schedule.run_pending()
            time.sleep(1)
        
        logger.info("Task scheduler stopped")
    
    def start(self):
        if self.thread and self.thread.is_alive():
            return
        
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()
        logger.info("Task scheduler started")
    
    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("Task scheduler stopped")


if __name__ == "__main__":
    def daily_task():
        logger.info("Running daily task...")
        time.sleep(2)
    
    runner = ScheduledTaskRunner()
    runner.add_task(TaskConfig(name="daily", func=daily_task, schedule="every 1 minute"))
    runner.start()
    
    try:
        time.sleep(60)
    except KeyboardInterrupt:
        pass
    finally:
        runner.stop()