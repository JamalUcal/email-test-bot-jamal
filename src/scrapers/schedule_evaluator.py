"""
Schedule evaluation logic for web scrapers.

Determines when scrapers should execute based on their schedule configuration
and current state.
"""

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
import pytz

from utils.logger import setup_logger

logger = setup_logger(__name__)


class ScheduleEvaluator:
    """Evaluates if a scraper should run based on schedule and state."""
    
    def should_run_scraper(
        self,
        scraper_config: Dict[str, Any],
        supplier_state: Dict[str, Any],
        current_time: datetime,
        force: bool = False
    ) -> bool:
        """
        Determine if scraper should run now.
        
        Args:
            scraper_config: Scraper configuration dictionary
            supplier_state: Current state for this supplier
            current_time: Current datetime (with timezone)
            force: If True, ignore schedule and run immediately
            
        Returns:
            True if scraper should run, False otherwise
            
        Logic:
        - If force=True, always run
        - If interrupted, resume immediately
        - Check frequency (daily/weekly/monthly)
        - Check time window
        - Check if already ran in current period
        """
        supplier = scraper_config.get('supplier', 'unknown')
        
        # Force execution overrides everything
        if force:
            logger.info(f"Force execution enabled for {supplier}")
            return True
        
        # Resume interrupted runs immediately
        if supplier_state.get('interrupted', False):
            logger.info(f"Resuming interrupted run for {supplier}")
            return True
        
        # Get schedule config
        schedule = scraper_config.get('schedule', {})
        if not schedule:
            logger.warning(f"No schedule configured for {supplier}, skipping")
            return False
        
        frequency = schedule.get('frequency', 'daily')
        scheduled_time = schedule.get('time', '09:00')
        tz_name = schedule.get('timezone', 'UTC')
        
        try:
            tz = pytz.timezone(tz_name)
        except pytz.exceptions.UnknownTimeZoneError:
            logger.error(f"Unknown timezone {tz_name} for {supplier}, using UTC")
            tz = pytz.UTC
        
        # Convert current time to supplier's timezone
        current_local = current_time.astimezone(tz)
        
        # Get last run time
        last_run_str = supplier_state.get('last_run')
        last_run: Optional[datetime] = None
        if last_run_str:
            last_run = datetime.fromisoformat(last_run_str).astimezone(tz)
        
        # Check if already ran in current period
        if last_run and self._already_ran_in_period(
            last_run, current_local, frequency, schedule
        ):
            logger.debug(
                f"Skipping {supplier} - already ran in current period",
                last_run=last_run.isoformat(),
                frequency=frequency
            )
            return False
        
        # Check if we're in the time window
        if not self._is_in_time_window(current_local, scheduled_time):
            logger.debug(
                f"Skipping {supplier} - outside time window",
                current_time=current_local.strftime('%H:%M'),
                scheduled_time=scheduled_time
            )
            return False
        
        logger.info(
            f"Scraper {supplier} should run",
            frequency=frequency,
            scheduled_time=scheduled_time,
            last_run=last_run.isoformat() if last_run else "never"
        )
        return True
    
    def get_next_run_time(
        self,
        scraper_config: Dict[str, Any],
        last_run: Optional[datetime]
    ) -> datetime:
        """
        Calculate next scheduled run time.
        
        Args:
            scraper_config: Scraper configuration dictionary
            last_run: Last run datetime (with timezone), or None if never run
            
        Returns:
            Next scheduled run time
        """
        schedule = scraper_config.get('schedule', {})
        frequency = schedule.get('frequency', 'daily')
        scheduled_time = schedule.get('time', '09:00')
        tz_name = schedule.get('timezone', 'UTC')
        
        try:
            tz = pytz.timezone(tz_name)
        except pytz.exceptions.UnknownTimeZoneError:
            logger.error(f"Unknown timezone {tz_name}, using UTC")
            tz = pytz.UTC
        
        # Parse scheduled time (HH:MM format)
        try:
            hour, minute = map(int, scheduled_time.split(':'))
        except (ValueError, AttributeError):
            logger.error(f"Invalid time format {scheduled_time}, using 09:00")
            hour, minute = 9, 0
        
        # Start from last run or now
        base_time = last_run if last_run else datetime.now(tz)
        
        # Calculate next run based on frequency
        if frequency == 'daily':
            next_run = base_time + timedelta(days=1)
        elif frequency == 'weekly':
            day_of_week = schedule.get('day_of_week', 'monday')
            next_run = self._get_next_weekday(base_time, day_of_week)
        elif frequency == 'monthly':
            day_of_month = schedule.get('day_of_month', 1)
            next_run = self._get_next_month_day(base_time, day_of_month)
        else:
            logger.warning(f"Unknown frequency {frequency}, defaulting to daily")
            next_run = base_time + timedelta(days=1)
        
        # Set to scheduled time
        next_run = next_run.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        return next_run
    
    def _already_ran_in_period(
        self,
        last_run: datetime,
        current_time: datetime,
        frequency: str,
        schedule: Dict[str, Any]
    ) -> bool:
        """
        Check if scraper already ran in the current period.
        
        Args:
            last_run: Last run datetime
            current_time: Current datetime
            frequency: Frequency string (daily/weekly/monthly)
            schedule: Schedule configuration
            
        Returns:
            True if already ran in current period
        """
        if frequency == 'daily':
            # Same day
            return (
                last_run.year == current_time.year and
                last_run.month == current_time.month and
                last_run.day == current_time.day
            )
        
        elif frequency == 'weekly':
            # Same week
            day_of_week = schedule.get('day_of_week', 'monday')
            target_weekday = self._parse_weekday(day_of_week)
            
            # Get start of current week (Monday)
            week_start = current_time - timedelta(days=current_time.weekday())
            week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
            
            return last_run >= week_start
        
        elif frequency == 'monthly':
            # Same month
            return (
                last_run.year == current_time.year and
                last_run.month == current_time.month
            )
        
        return False
    
    def _is_in_time_window(self, current_time: datetime, scheduled_time: str) -> bool:
        """
        Check if current time is within acceptable window of scheduled time.
        
        We allow a 1-hour window after scheduled time to account for hourly
        Cloud Function triggers.
        
        Args:
            current_time: Current datetime
            scheduled_time: Scheduled time string (HH:MM)
            
        Returns:
            True if within window
        """
        try:
            hour, minute = map(int, scheduled_time.split(':'))
        except (ValueError, AttributeError):
            logger.error(f"Invalid time format {scheduled_time}")
            return False
        
        scheduled = current_time.replace(hour=hour, minute=minute, second=0, microsecond=0)
        window_end = scheduled + timedelta(hours=1)
        
        return scheduled <= current_time <= window_end
    
    def _get_next_weekday(self, from_date: datetime, day_name: str) -> datetime:
        """
        Get next occurrence of specified weekday.
        
        Args:
            from_date: Start date
            day_name: Day name (e.g., 'monday', 'tuesday')
            
        Returns:
            Next occurrence of that weekday
        """
        target_weekday = self._parse_weekday(day_name)
        current_weekday = from_date.weekday()
        
        days_ahead = target_weekday - current_weekday
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7
        
        return from_date + timedelta(days=days_ahead)
    
    def _get_next_month_day(self, from_date: datetime, day: int) -> datetime:
        """
        Get next occurrence of specified day of month.
        
        Args:
            from_date: Start date
            day: Day of month (1-31)
            
        Returns:
            Next occurrence of that day
        """
        # Try next month
        if from_date.month == 12:
            next_month = from_date.replace(year=from_date.year + 1, month=1, day=1)
        else:
            next_month = from_date.replace(month=from_date.month + 1, day=1)
        
        # Handle invalid days (e.g., day 31 in February)
        try:
            return next_month.replace(day=min(day, 28))  # Safe default
        except ValueError:
            return next_month.replace(day=1)
    
    def _parse_weekday(self, day_name: str) -> int:
        """
        Parse weekday name to integer (0=Monday, 6=Sunday).
        
        Args:
            day_name: Day name (case-insensitive)
            
        Returns:
            Weekday integer (0-6)
        """
        weekdays = {
            'monday': 0,
            'tuesday': 1,
            'wednesday': 2,
            'thursday': 3,
            'friday': 4,
            'saturday': 5,
            'sunday': 6
        }
        
        return weekdays.get(day_name.lower(), 0)  # Default to Monday


