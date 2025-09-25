"""
Utility functions for parsing and formatting specific BAI2 data types.

This module provides robust, type-hinted functions for handling the specific
date, time, and type code formats required by the BAI2 specification.
"""

import datetime
from typing import Optional

from .constants import TypeCodes
from .exceptions import NotSupportedYetException


def parse_date(value: str) -> Optional[datetime.date]:
    """
    Parses a BAI2-specific date string (YYMMDD) into a date object.

    Args:
        value: The date string to parse.

    Returns:
        A datetime.date object, or None if the input is empty.
    """
    if not value:
        return None
    try:
        return datetime.datetime.strptime(value, '%y%m%d').date()
    except ValueError:
        raise ValueError(f"Invalid date format for value: '{value}'. Expected YYMMDD.")


def write_date(date_obj: datetime.date) -> str:
    """
    Formats a date object into a BAI2-specific date string (YYMMDD).

    Args:
        date_obj: The datetime.date object to format.

    Returns:
        The formatted YYMMDD string.
    """
    return date_obj.strftime('%y%m%d')


def parse_time(value: str) -> Optional[datetime.time]:
    """
    Parses a BAI2 time string into a time object.

    This function is generic enough to handle both military format (HHMM) and
    clock format (HH:MM:SS), including special BAI2 conventions for end-of-day.

    - Handles '2400' and '9999' as end-of-day (23:59:59.999999).
    - Handles standard military time like '0930' or '1700'.
    - Handles standard clock time like '09:30:00'.

    Args:
        value: The time string to parse.

    Returns:
        A datetime.time object, or None if the input is empty.
    """
    if not value:
        return None

    # Handle special BAI2 end-of-day conventions first.
    if value in ('2400', '9999'):
        return datetime.time.max

    try:
        # Check for clock format (HH:MM:SS)
        if ':' in value:
            return datetime.datetime.strptime(value, '%H:%M:%S').time()
        
        # Assume military format (HHMM), ensuring it's 4 digits.
        padded_value = value.zfill(4)
        return datetime.datetime.strptime(padded_value, '%H%M').time()
    
    except ValueError:
        raise ValueError(f"Invalid time format for value: '{value}'. Expected HHMM or HH:MM:SS.")


def write_time(time_obj: datetime.time, clock_format_for_intra_day: bool = False) -> str:
    """
    Formats a time object into a BAI2-compliant string.

    Args:
        time_obj: The datetime.time object to format.
        clock_format_for_intra_day: If True, uses HH:MM:SS for times that are
                                      not the end-of-day maximum. Defaults to False.

    Returns:
        The formatted time string.
    """
    if clock_format_for_intra_day and time_obj != datetime.time.max:
        return _write_clock_time(time_obj)
    
    return _write_military_time(time_obj)


def _write_clock_time(time_obj: datetime.time) -> str:
    """Formats a time object as HH:MM:SS."""
    return time_obj.strftime('%H:%M:%S')


def _write_military_time(time_obj: datetime.time) -> str:
    """Formats a time object as HHMM, handling the end-of-day convention."""
    if time_obj == datetime.time.max:
        return '2400'
    return time_obj.strftime('%H%M')


def parse_type_code(value: str):
    """
    Looks up a type code string and returns the corresponding rich TypeCode object.

    This uses a highly efficient dictionary lookup.

    Args:
        value: The 3-digit type code string.

    Returns:
        The matching TypeCode named tuple from constants.

    Raises:
        NotSupportedYetException: If the type code is not found in the constants map.
    """
    type_code = TypeCodes.get(value)
    if not type_code:
        raise NotSupportedYetException(f"Type code '{value}' is not defined in constants.")
    return type_code


def convert_to_string(value: Optional[any]) -> str:
    """
    Safely converts any value to its string representation.

    Handles `None` by returning an empty string, which is standard for BAI2 fields.

    Args:
        value: The value to convert.

    Returns:
        The string representation of the value, or '' if the value is None.
    """
    return '' if value is None else str(value)