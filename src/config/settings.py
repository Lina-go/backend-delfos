"""Application settings using Pydantic BaseSettings."""

import logging
from functools import lru_cache

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    app_name: str = "Delfos NL2SQL"
    app_version: str = "0.1.0"
    debug: bool = False
    log_level: str = "INFO"

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        upper = v.upper()
        if upper not in _VALID_LOG_LEVELS:
            raise ValueError(f"log_level must be one of {_VALID_LOG_LEVELS}, got '{v}'")
        return upper

    @model_validator(mode="after")
    def validate_timeouts_positive(self) -> "Settings":
        for field_name in (
            "response_timeout",
            "triage_timeout",
            "intent_timeout",
            "sql_generation_timeout",
            "sql_validation_timeout",
            "sql_execution_timeout",
            "verification_timeout",
            "viz_timeout",
            "format_timeout",
        ):
            value = getattr(self, field_name)
            if value <= 0:
                raise ValueError(f"{field_name} must be positive, got {value}")
        return self

    @model_validator(mode="after")
    def warn_wildcard_origins(self) -> "Settings":
        if self.allowed_origins == ["*"]:
            logging.getLogger(__name__).warning(
                "allowed_origins is set to ['*'] — consider restricting in production"
            )
        return self

    @model_validator(mode="after")
    def validate_fabric_config(self) -> "Settings":
        if self.use_direct_db:
            if not self.wh_server:
                raise ValueError("wh_server is required when use_direct_db=True")
            if not self.wh_database:
                raise ValueError("wh_database is required when use_direct_db=True")
            if not self.db_server:
                raise ValueError("db_server is required when use_direct_db=True")
            if not self.db_database:
                raise ValueError("db_database is required when use_direct_db=True")
        if self.use_service_principal:
            if not all([self.azure_tenant_id, self.azure_client_id, self.azure_client_secret]):
                raise ValueError(
                    "azure_tenant_id, azure_client_id, and azure_client_secret "
                    "are required when use_service_principal=True"
                )
        return self

    # Azure AI Foundry
    azure_ai_project_endpoint: str = ""

    # Anthropic
    anthropic_api_key: str | None = None
    use_anthropic_api_for_claude: bool = False
    anthropic_foundry_api_key: str | None = None
    anthropic_foundry_resource: str | None = None

    # Triage Agent
    triage_agent_model: str = "gpt-4.1"
    triage_temperature: float = 0.0
    triage_max_tokens: int = 400

    # Intent Agent
    intent_agent_model: str = "gpt-4o-mini"
    intent_temperature: float = 0.0
    intent_max_tokens: int = 500

    # SQL Agent
    sql_agent_model: str = "claude-sonnet-4-5"
    sql_temperature: float = 0.0
    sql_max_tokens: int = 16384

    # SQL Executor Agent
    sql_executor_agent_model: str = "gpt-4o-mini"
    sql_executor_temperature: float = 0.0
    sql_executor_max_tokens: int = 4096

    # Verification Agent
    verification_agent_model: str = "gpt-4o-mini"
    verification_temperature: float = 0.0
    verification_max_tokens: int = 1024

    # Viz Agent
    viz_agent_model: str = "gpt-4o-mini"
    viz_temperature: float = 0.0
    viz_max_tokens: int = 4096
    viz_max_categories: int = 6

    # Format Agent
    format_agent_model: str = "gpt-4o-mini"
    format_temperature: float = 0.0
    format_max_tokens: int = 1024

    # Suggest Labels Agent
    suggest_labels_agent_model: str = "gpt-4o-mini"
    suggest_labels_temperature: float = 0.3
    suggest_labels_max_tokens: int = 1024
    suggest_labels_timeout: float = 15.0

    # Advisor Agent
    advisor_agent_model: str = "gpt-4o-mini"
    advisor_max_tokens: int = 4096
    advisor_temperature: float = 0.3

    # Chat V2 Agent
    chat_v2_agent_model: str = "claude-sonnet-4-5"
    chat_v2_max_tokens: int = 1024
    chat_v2_temperature: float = 0.2
    chat_v2_classifier_model: str = "claude-sonnet-4-5"

    # Chat V2 Compaction (session memory)
    chat_v2_compaction_soft_threshold: int = 20
    chat_v2_compaction_hard_threshold: int = 40
    chat_v2_compaction_keep_recent: int = 4
    chat_v2_compaction_model: str = "gpt-4o-mini"

    # Semantic Cache (Azure OpenAI Embeddings)
    aoai_embedding_endpoint: str = ""
    aoai_embedding_key: str = ""
    aoai_embedding_deployment: str = "text-embedding-3-small"
    semantic_cache_threshold: float = 0.82
    semantic_cache_ttl: int = 1800
    semantic_cache_max_size: int = 200

    # CORS
    allowed_origins: list[str] = ["*"]

    # Pipeline
    pipeline_name: str = "nl2sql"
    pipeline_version: str = "0.1.0"
    pipeline_description: str = "NL2SQL pipeline"
    use_llm_verification: bool = False
    use_llm_formatting: bool = False
    max_history_turns: int = 10

    # SQL retries
    sql_max_retries: int = 2
    sql_max_verification_retries: int = 2
    sql_retry_delay: int = 1
    retry_backoff_factor: float = 2.0

    # Schema cache
    schema_cache_max_size: int = 100
    schema_cache_ttl: int = 3600

    # Timeouts
    response_timeout: int = 120
    llm_max_concurrent_requests: int = 2
    triage_timeout: float = 5.0
    intent_timeout: float = 5.0
    sql_generation_timeout: float = 120.0
    sql_validation_timeout: float = 10.0
    sql_execution_timeout: float = 50.0
    verification_timeout: float = 10.0
    viz_timeout: float = 15.0
    format_timeout: float = 10.0

    # Power BI
    powerbi_workspace_id: str | None = None
    powerbi_report_id: str | None = None

    # Warehouse (READS - agent data queries)
    wh_server: str = ""
    wh_database: str = ""
    wh_schema: str = "gold"

    # SQL Database (WRITES - app CRUD, agent_output)
    db_server: str = ""
    db_database: str = ""
    db_schema: str = "dbo"

    use_direct_db: bool = False

    # Authentication (Service Principal)
    use_service_principal: bool = False
    azure_tenant_id: str = ""
    azure_client_id: str = ""
    azure_client_secret: str = ""


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
