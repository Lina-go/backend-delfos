"""
Application settings using Pydantic BaseSettings.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    ######################################
    # Application information
    ######################################

    app_name: str = "Delfos NL2SQL"
    app_version: str = "0.1.0"
    debug: bool = False
    log_level: str = "INFO"

    ######################################
    # Azure AI Foundry
    ######################################

    azure_ai_project_endpoint: str = ""

    ######################################
    # Anthropic
    ######################################

    anthropic_api_key: str | None = None
    # Use Anthropic API directly instead of Azure Foundry for Claude models
    # Set to True to use Anthropic API, False to use Azure Foundry deployment
    use_anthropic_api_for_claude: bool = False
    
    # Anthropic Foundry configuration (for Claude models on Foundry)
    # These are read by AsyncAnthropicFoundry from environment variables
    anthropic_foundry_api_key: str | None = None
    anthropic_foundry_resource: str | None = None

    ######################################
    # Agent model configurations
    ######################################

    # Triage Agent
    triage_agent_model: str = "gpt-4.1"
    triage_temperature: float = 0.0
    triage_max_tokens: int = 400

    # Intent Agent
    intent_agent_model: str = "gpt-4o-mini"
    intent_temperature: float = 0.0
    intent_max_tokens: int = 300

    # SQL Agent
    sql_agent_model: str = "claude-sonnet-4-5"
    sql_temperature: float = 0.0
    sql_max_tokens: int = 4096

    # SQL Executor Agent
    sql_executor_agent_model: str = "gpt-4o-mini"
    sql_executor_temperature: float = 0.0
    sql_executor_max_tokens: int = 4096

    # Verification Agent (optional)
    verification_agent_model: str = "gpt-4o-mini"
    verification_temperature: float = 0.0
    verification_max_tokens: int = 1024

    # Viz Agent
    viz_agent_model: str = "gpt-4o-mini"
    viz_temperature: float = 0.0
    viz_max_tokens: int = 4096

    # Graph Agent
    graph_agent_model: str = "gpt-4o-mini"
    graph_temperature: float = 0.0
    graph_max_tokens: int = 4096

    # Format Agent (optional)
    format_agent_model: str = "gpt-4o-mini"
    format_temperature: float = 0.0
    format_max_tokens: int = 1024

    ######################################
    # MCP Server
    ######################################

    mcp_server_url: str = "https://func-mcp-n2z2m7tmh3kvk.azurewebsites.net/mcp"
    mcp_timeout: int = 60
    mcp_sse_timeout: int = 30

    ######################################
    # Azure Blob Storage
    ######################################

    azure_storage_account_url: str = "https://delfoscharts.blob.core.windows.net"
    azure_storage_container_name: str = "charts"
    azure_storage_connection_string: str = ""

    ######################################
    # Pipeline configuration
    ######################################

    # Pipeline settings
    pipeline_name: str = "nl2sql"
    pipeline_version: str = "0.1.0"
    pipeline_description: str = "NL2SQL pipeline"

    use_llm_verification: bool = False
    use_llm_formatting: bool = False

    # SQL retries
    sql_max_retries: int = 2
    sql_max_verification_retries: int = 2
    sql_retry_delay: int = 1
    retry_backoff_factor: float = 2.0

    # Schema cache settings
    schema_cache_max_size: int = 100
    schema_cache_ttl: int = 3600

    # Global response timeout
    response_timeout: int = 120

    # LLM concurrency limit
    llm_max_concurrent_requests: int = 2

    # Steps timeout
    triage_timeout: float = 5.0
    intent_timeout: float = 5.0
    sql_generation_timeout: float = 120.0
    sql_validation_timeout: float = 10.0
    sql_execution_timeout: float = 50.0
    verification_timeout: float = 10.0
    viz_timeout: float = 15.0
    graph_timeout: float = 15.0
    format_timeout: float = 10.0

    ######################################
    # Chart server configuration
    ######################################
    chart_default_width: int = 800
    chart_default_height: int = 600
    chart_output_format: str = "html"  # html|png
    chart_color_palette: list[str] = [
        "#0057A4",  # Dark Blue
        "#4A90E2",  # Light Blue
        "#003A70",  # Navy Blue
        "#E61E25",  # Red
        "#A11218",  # Dark Red
        "#E5E8EC",  # Light Gray
        "#4A4F55",  # Dark Gray
        "#4CAF50",  # Green
    ]
    chart_font_family: str = "Arial, sans-serif"
    chart_font_size: int = 12

    ######################################
    # PowerBi configuration
    ######################################
    powerbi_workspace_id: str | None = None
    powerbi_report_id: str | None = None

    ######################################
    # Database configuration
    ######################################
    database_name: str = "SuperDB"
    database_schema: str = "dbo"
    database_connection_string: str = ""


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
