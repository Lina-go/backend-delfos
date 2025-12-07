"""Tests for JSON parser."""

import pytest
from src.utils.json_parser import JSONParser


def test_extract_json_simple():
    """Test extracting simple JSON."""
    text = '{"key": "value"}'
    result = JSONParser.extract_json(text)
    assert result == {"key": "value"}


def test_extract_json_in_code_block():
    """Test extracting JSON from code block."""
    text = '```json\n{"key": "value"}\n```'
    result = JSONParser.extract_json(text)
    assert result == {"key": "value"}


def test_extract_json_in_answer_tags():
    """Test extracting JSON from answer tags."""
    text = '<answer>{"key": "value"}</answer>'
    result = JSONParser.extract_json(text)
    assert result == {"key": "value"}


def test_extract_json_invalid():
    """Test extracting invalid JSON returns empty dict."""
    text = "not json at all"
    result = JSONParser.extract_json(text)
    assert result == {}

