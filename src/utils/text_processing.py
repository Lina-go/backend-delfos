"""Text processing utilities."""

import re
from datetime import datetime


def normalize_text(text: str) -> str:
    """
    Normalize text for processing.

    Args:
        text: Input text

    Returns:
        Normalized text
    """
    # Remove extra whitespace
    text = re.sub(r"\s+", " ", text)
    # Trim
    text = text.strip()
    return text


def parse_date(date_str: str) -> datetime | None:
    """
    Parse date string to datetime.

    Args:
        date_str: Date string

    Returns:
        Datetime object or None
    """
    try:
        # Try common date formats
        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y-%m-%d %H:%M:%S",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return None
    except Exception:
        return None
