"""
JSON Parser utility for extracting JSON from LLM responses.
"""
import json
import re
from typing import Dict, Any


class JSONParser:
    """Helper class to extract clean JSON from LLM responses."""

    @staticmethod
    def extract_json(text: str) -> Dict[str, Any]:
        """Attempts to extract a JSON block from text."""
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # First, try to find JSON inside <answer> tags
            answer_match = re.search(
                r"<answer>\s*(.*?)\s*</answer>", text, re.DOTALL | re.IGNORECASE
            )
            if answer_match:
                answer_content = answer_match.group(1)
                # Try to find JSON in the answer content
                json_match = re.search(
                    r"```(?:json)?\s*(\{.*?\})\s*```", answer_content, re.DOTALL
                )
                if json_match:
                    try:
                        return json.loads(json_match.group(1))
                    except json.JSONDecodeError:
                        pass
                # Try to find any JSON object in answer content
                json_match = re.search(r"(\{.*\})", answer_content, re.DOTALL)
                if json_match:
                    try:
                        return json.loads(json_match.group(1))
                    except json.JSONDecodeError:
                        pass

            # Fallback: try to find JSON in code blocks
            match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(1))
                except json.JSONDecodeError:
                    pass
            # Fallback: try to find any JSON object
            match = re.search(r"(\{.*\})", text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(1))
                except json.JSONDecodeError:
                    pass
            return {}

