"""System prompts for NL2SQL pipeline agents."""

from src.config.prompts.formatting import build_format_prompt
from src.config.prompts.handlers import FOLLOW_UP_PROMPT_TEMPLATE, GENERAL_HANDLER_PROMPT
from src.config.prompts.intent import build_intent_system_prompt
from src.config.prompts.intent_hierarchical import build_intent_hierarchical_prompt
from src.config.prompts.sql import (
    build_sql_execution_system_prompt,
    build_sql_formatting_system_prompt,
    build_sql_generation_system_prompt,
    build_sql_retry_user_input,
)
from src.config.prompts.triage import build_triage_system_prompt
from src.config.prompts.verification import (
    build_verification_system_prompt,
    build_verification_user_input,
)
from src.config.prompts.viz import build_viz_mapping_prompt
from src.config.prompts.unified_intent_viz import build_unified_intent_viz_prompt
from src.config.prompts.labels import build_suggest_labels_system_prompt

__all__ = [
    "FOLLOW_UP_PROMPT_TEMPLATE",
    "GENERAL_HANDLER_PROMPT",
    "build_format_prompt",
    "build_intent_hierarchical_prompt",
    "build_intent_system_prompt",
    "build_sql_execution_system_prompt",
    "build_sql_formatting_system_prompt",
    "build_sql_generation_system_prompt",
    "build_sql_retry_user_input",
    "build_suggest_labels_system_prompt",
    "build_triage_system_prompt",
    "build_verification_system_prompt",
    "build_verification_user_input",
    "build_unified_intent_viz_prompt",
    "build_viz_mapping_prompt",
]
