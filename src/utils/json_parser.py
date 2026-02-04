"""
JSON Parser utility for extracting JSON from LLM responses.
"""

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


class JSONParser:
    """Helper class to extract clean JSON from LLM responses."""

    @staticmethod
    def _extract_first_json_object(text: str) -> dict[str, Any] | None:
        """Extract the first complete JSON object from text using balanced brace matching.

        This handles cases where multiple JSON objects are concatenated together.
        Returns None if no valid JSON object is found.
        """
        brace_count = 0
        start_idx = -1
        in_string = False
        escape_next = False

        for i, char in enumerate(text):
            if escape_next:
                escape_next = False
                continue
            if char == '\\' and in_string:
                escape_next = True
                continue
            if char == '"' and not escape_next:
                in_string = not in_string
                continue
            if in_string:
                continue

            if char == "{":
                if start_idx == -1:
                    start_idx = i
                brace_count += 1
            elif char == "}":
                brace_count -= 1
                if brace_count == 0 and start_idx != -1:
                    json_str = text[start_idx : i + 1]
                    try:
                        data = json.loads(json_str)
                        if isinstance(data, dict):
                            return data
                    except json.JSONDecodeError:
                        pass
                    # Reset and try to find another object
                    start_idx = -1
        return None

    @staticmethod
    def extract_json(text: str) -> dict[str, Any]:
        """Attempts to extract a JSON object from text.

        If no valid JSON object can be extracted, returns an empty dict.
        Handles concatenated JSON objects by extracting the first valid one.
        """
        # Try pure JSON first (if the whole text is valid JSON)
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                return data
        except json.JSONDecodeError:
            pass

        # PRIORITY 1: Try to find JSON inside <answer> tags first (used by visualization service)
        # This is checked early because <answer> is the expected output format
        answer_match = re.search(r"<answer>(.*?)</answer>", text, re.DOTALL | re.IGNORECASE)
        if not answer_match:
            # Try to match unclosed <answer> tag (for truncated responses)
            answer_match = re.search(r"<answer>(.*)", text, re.DOTALL | re.IGNORECASE)

        if answer_match:
            answer_content = answer_match.group(1).strip()
            logger.debug(f"Found <answer> tag content (length: {len(answer_content)})")

            # First try to parse the entire answer content as JSON
            try:
                data = json.loads(answer_content)
                if isinstance(data, dict):
                    return data
            except json.JSONDecodeError:
                pass

            # Try to find JSON in code blocks
            json_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", answer_content, re.DOTALL)
            if json_match:
                try:
                    data = json.loads(json_match.group(1))
                    if isinstance(data, dict):
                        return data
                except json.JSONDecodeError:
                    pass

            # Use balanced brace matching (more reliable than greedy regex)
            first_obj = JSONParser._extract_first_json_object(answer_content)
            if first_obj:
                return first_obj

        # PRIORITY 2: Try to find JSON inside <classification> tags (used by triage)
        classification_match = re.search(
            r"<classification>(.*?)</classification>", text, re.DOTALL | re.IGNORECASE
        )
        if classification_match:
            classification_content = classification_match.group(1).strip()
            # Try to find JSON in the classification content
            json_match = re.search(
                r"```(?:json)?\s*(\{.*?\})\s*```", classification_content, re.DOTALL
            )
            if json_match:
                try:
                    data = json.loads(json_match.group(1))
                    if isinstance(data, dict):
                        return data
                except json.JSONDecodeError:
                    pass
            # Try to find any JSON object in classification content (more precise matching)
            # Use a balanced brace matcher to find complete JSON objects
            brace_count = 0
            start_idx = -1
            for i, char in enumerate(classification_content):
                if char == "{":
                    if start_idx == -1:
                        start_idx = i
                    brace_count += 1
                elif char == "}":
                    brace_count -= 1
                    if brace_count == 0 and start_idx != -1:
                        json_str = classification_content[start_idx : i + 1]
                        try:
                            data = json.loads(json_str)
                            if isinstance(data, dict):
                                return data
                        except json.JSONDecodeError:
                            pass
                        start_idx = -1
            # Fallback: try simple regex if balanced matching didn't work
            json_match = re.search(
                r"(\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\})", classification_content, re.DOTALL
            )
            if json_match:
                try:
                    data = json.loads(json_match.group(1))
                    if isinstance(data, dict):
                        return data
                except json.JSONDecodeError:
                    pass

        # PRIORITY 3: Generic fallbacks
        # Try extracting first complete JSON object from full text
        first_obj = JSONParser._extract_first_json_object(text)
        if first_obj:
            return first_obj

        # Fallback: try to find JSON in code blocks
        match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group(1))
                if isinstance(data, dict):
                    return data
            except json.JSONDecodeError:
                pass
        # Fallback: try to find any JSON object
        match = re.search(r"(\{.*\})", text, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group(1))
                if isinstance(data, dict):
                    return data
            except json.JSONDecodeError:
                pass

        # Log warning and return empty dict
        # Callers should check for empty dict and handle appropriately
        logger.warning(
            f"JSONParser: Could not extract JSON from text (length: {len(text)} chars). "
            f"First 200 chars: {text[:200]}"
        )
        return {}
