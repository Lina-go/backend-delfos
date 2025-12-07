"""
JSON Parser utility for extracting JSON from LLM responses.
"""
import json
import logging
import re
from typing import Dict, Any

logger = logging.getLogger(__name__)


class JSONParser:
    """Helper class to extract clean JSON from LLM responses."""

    @staticmethod
    def extract_json(text: str) -> Dict[str, Any]:
        """Attempts to extract a JSON block from text."""
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # First, try to find JSON inside <classification> tags (used by triage)
            classification_match = re.search(
                r"<classification>\s*(.*?)\s*</classification>", text, re.DOTALL | re.IGNORECASE
            )
            if classification_match:
                classification_content = classification_match.group(1)
                # Try to find JSON in the classification content
                json_match = re.search(
                    r"```(?:json)?\s*(\{.*?\})\s*```", classification_content, re.DOTALL
                )
                if json_match:
                    try:
                        return json.loads(json_match.group(1))
                    except json.JSONDecodeError:
                        pass
                # Try to find any JSON object in classification content
                json_match = re.search(r"(\{.*\})", classification_content, re.DOTALL)
                if json_match:
                    try:
                        return json.loads(json_match.group(1))
                    except json.JSONDecodeError:
                        pass
            
            # Try to find JSON inside <answer> tags
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
            # Return empty dict as fallback (caller should handle this)
            # Note: Consider returning None and updating callers if needed
            logger.warning("JSONParser: Could not extract JSON from text, returning empty dict")
            return {}

