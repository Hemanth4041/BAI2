"""

This file exposes the primary functions for parsing and writing BAI2 data,
abstracting away the underlying parser classes for easy use by the main pipeline.
"""
from typing import IO, Iterable

from .helpers import IteratorHelper
from .parsers import Bai2FileParser
from .writers import Bai2FileWriter
from .models import Bai2File


def parse_from_lines(lines: Iterable[str], **kwargs) -> Bai2File:
    """Parses a BAI2 file from an iterable of lines."""
    helper = IteratorHelper(lines)
    parser = Bai2FileParser(helper, **kwargs)
    return parser.parse()


def parse_from_string(s: str, **kwargs) -> Bai2File:
    """Parses a BAI2 file from a single string."""
    # Filter out empty lines and strip whitespace from each line
    lines = filter(None, (line.strip() for line in s.splitlines()))
    return parse_from_lines(lines, **kwargs)


def parse_from_file(f: IO[str], **kwargs) -> Bai2File:
    """Parses a BAI2 file from a file-like object."""
    return parse_from_string(f.read(), **kwargs)


def write(bai2_obj: Bai2File, **kwargs) -> str:
    """Serializes a Bai2File object into a BAI2 formatted string."""
    writer = Bai2FileWriter(bai2_obj, **kwargs)
    return '\n'.join(writer.write())