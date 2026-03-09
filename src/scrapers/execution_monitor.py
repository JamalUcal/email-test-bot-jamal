"""
Execution timeout monitoring for web scrapers.

Monitors execution time and signals when to stop gracefully before
Cloud Function timeout occurs.
"""

import time
from typing import Optional

from utils.logger import setup_logger

logger = setup_logger(__name__)


class ExecutionMonitor:
    """Monitors execution time and signals timeout approaching."""
    
    def __init__(self, max_duration_seconds: int, buffer_seconds: int = 180):
        """
        Initialize execution monitor.
        
        Args:
            max_duration_seconds: Maximum execution duration
            buffer_seconds: Stop this many seconds before timeout (default: 180)
        """
        self.start_time = time.time()
        self.max_duration = max_duration_seconds
        self.buffer = buffer_seconds
        self.pause_time: Optional[float] = None
        self.paused_duration: float = 0.0
        
        logger.debug(
            "ExecutionMonitor initialized",
            max_duration_seconds=max_duration_seconds,
            buffer_seconds=buffer_seconds,
            stop_threshold=max_duration_seconds - buffer_seconds
        )
    
    def should_stop(self) -> bool:
        """
        Check if we should stop due to timeout approaching.
        
        Returns:
            True if elapsed time >= (max_duration - buffer)
        """
        elapsed = self.elapsed_time()
        threshold = self.max_duration - self.buffer
        
        should_stop = elapsed >= threshold
        
        if should_stop:
            logger.warning(
                "Execution timeout approaching - should stop",
                elapsed_seconds=round(elapsed, 2),
                threshold_seconds=threshold,
                time_remaining=round(self.max_duration - elapsed, 2)
            )
        
        return should_stop
    
    def elapsed_time(self) -> float:
        """
        Get elapsed time since start, excluding paused time.
        
        Returns:
            Elapsed seconds
        """
        current_time = time.time()
        elapsed = current_time - self.start_time - self.paused_duration
        
        # If currently paused, don't count pause time
        if self.pause_time is not None:
            elapsed -= (current_time - self.pause_time)
        
        return elapsed
    
    def time_remaining(self) -> float:
        """
        Get remaining time before buffer kicks in.
        
        Returns:
            Remaining seconds before stop threshold
        """
        elapsed = self.elapsed_time()
        threshold = self.max_duration - self.buffer
        remaining = max(0, threshold - elapsed)
        
        return remaining
    
    def time_until_timeout(self) -> float:
        """
        Get time remaining until actual timeout (including buffer).
        
        Returns:
            Remaining seconds until max_duration
        """
        elapsed = self.elapsed_time()
        return max(0, self.max_duration - elapsed)
    
    def pause(self) -> None:
        """
        Pause the timer.
        
        Useful for excluding wait times from execution monitoring
        (e.g., waiting for page loads, network requests).
        """
        if self.pause_time is None:
            self.pause_time = time.time()
            logger.debug("ExecutionMonitor paused")
    
    def resume(self) -> None:
        """
        Resume the timer after pause.
        """
        if self.pause_time is not None:
            pause_duration = time.time() - self.pause_time
            self.paused_duration += pause_duration
            self.pause_time = None
            logger.debug(
                "ExecutionMonitor resumed",
                paused_for_seconds=round(pause_duration, 2)
            )
    
    def get_progress_info(self) -> dict[str, float]:
        """
        Get execution progress information.
        
        Returns:
            Dictionary with timing information
        """
        elapsed = self.elapsed_time()
        threshold = self.max_duration - self.buffer
        
        return {
            'elapsed_seconds': round(elapsed, 2),
            'max_duration_seconds': self.max_duration,
            'buffer_seconds': self.buffer,
            'threshold_seconds': threshold,
            'time_remaining_seconds': round(self.time_remaining(), 2),
            'time_until_timeout_seconds': round(self.time_until_timeout(), 2),
            'paused_duration_seconds': round(self.paused_duration, 2),
            'progress_percentage': round((elapsed / threshold) * 100, 1)
        }
    
    def log_progress(self, context: str = "") -> None:
        """
        Log current execution progress.
        
        Args:
            context: Optional context string to include in log
        """
        info = self.get_progress_info()
        
        logger.info(
            f"Execution progress{': ' + context if context else ''}",
            **info
        )


