"""JSON Parser utility for extracting JSON from LLM responses."""

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


class JSONParser:
    """Helper class to extract clean JSON from LLM responses."""

    @staticmethod
    def _try_parse(text: str) -> dict[str, Any] | None:
        """Try to parse text as a JSON dict. Returns None on failure."""
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                return data
        except (json.JSONDecodeError, ValueError):
            pass
        return None

    @staticmethod
    def _extract_first_json_object(text: str) -> dict[str, Any] | None:
        """Extract the first complete JSON object from text using balanced brace matching."""
        brace_count = 0
        start_idx = -1
        in_string = False
        escape_next = False

        for i, char in enumerate(text):
            if escape_next:
                escape_next = False
                continue
            if char == "\\" and in_string:
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
                    result = JSONParser._try_parse(text[start_idx : i + 1])
                    if result is not None:
                        return result
                    start_idx = -1
        return None

    @staticmethod
    def _try_code_block(text: str) -> dict[str, Any] | None:
        """Try to extract JSON from a markdown code block."""
        match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
        if match:
            return JSONParser._try_parse(match.group(1))
        return None

    @staticmethod
    def extract_json(text: str) -> dict[str, Any]:
        """Attempts to extract a JSON object from text.

        If no valid JSON object can be extracted, returns an empty dict.
        """
        # Try pure JSON first
        result = JSONParser._try_parse(text)
        if result is not None:
            return result

        # PRIORITY 1: <answer> tags (used by visualization service)
        answer_match = re.search(r"<answer>(.*?)</answer>", text, re.DOTALL | re.IGNORECASE)
        if not answer_match:
            answer_match = re.search(r"<answer>(.*)", text, re.DOTALL | re.IGNORECASE)

        if answer_match:
            content = answer_match.group(1).strip()
            logger.debug("Found <answer> tag content (length: %s)", len(content))

            result = JSONParser._try_parse(content)
            if result is not None:
                return result

            result = JSONParser._try_code_block(content)
            if result is not None:
                return result

            result = JSONParser._extract_first_json_object(content)
            if result is not None:
                return result

        # PRIORITY 2: <classification> tags (used by triage)
        classification_match = re.search(
            r"<classification>(.*?)</classification>", text, re.DOTALL | re.IGNORECASE
        )
        if classification_match:
            content = classification_match.group(1).strip()

            result = JSONParser._try_code_block(content)
            if result is not None:
                return result

            result = JSONParser._extract_first_json_object(content)
            if result is not None:
                return result

        # PRIORITY 3: Generic fallbacks
        result = JSONParser._extract_first_json_object(text)
        if result is not None:
            return result

        result = JSONParser._try_code_block(text)
        if result is not None:
            return result

        # Last resort: greedy regex
        match = re.search(r"(\{.*\})", text, re.DOTALL)
        if match:
            result = JSONParser._try_parse(match.group(1))
            if result is not None:
                return result

        logger.warning(
            "JSONParser: Could not extract JSON from text (length: %s chars). First 200 chars: %s",
            len(text),
            text[:200],
        )
        return {}
