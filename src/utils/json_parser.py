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
    def extract_json(text: str) -> dict[str, Any]:
        """Attempts to extract a JSON object from text.

        If no valid JSON object can be extracted, returns an empty dict.
        """
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                return data
        except json.JSONDecodeError:
            # If the whole text is not valid JSON, fall through to heuristics below.
            pass

        # First, try to find JSON inside <classification> tags (used by triage)
        classification_match = re.search(
            r"<classification>\s*(.*?)\s*</classification>", text, re.DOTALL | re.IGNORECASE
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

        # Try to find JSON inside <answer> tags
        answer_match = re.search(r"<answer>\s*(.*?)\s*</answer>", text, re.DOTALL | re.IGNORECASE)
        if answer_match:
            answer_content = answer_match.group(1)
            # Try to find JSON in the answer content
            json_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", answer_content, re.DOTALL)
            if json_match:
                try:
                    data = json.loads(json_match.group(1))
                    if isinstance(data, dict):
                        return data
                except json.JSONDecodeError:
                    pass
            # Try to find any JSON object in answer content
            json_match = re.search(r"(\{.*\})", answer_content, re.DOTALL)
            if json_match:
                try:
                    data = json.loads(json_match.group(1))
                    if isinstance(data, dict):
                        return data
                except json.JSONDecodeError:
                    pass

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
