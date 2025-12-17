# Delfos NL2SQL Pipeline

A multi-step natural language to SQL pipeline with intelligent orchestration, schema understanding, and visualization capabilities.

## Architecture

The pipeline consists of 8 main steps:

1. **Triage**: Classify query as `data_question`, `general`, or `out_of_scope`
2. **Intent**: For data questions, classify as `nivel_puntual` or `requiere_viz`
3. **Schema**: Extract and select relevant tables/columns
4. **SQL Generation**: Generate SQL query with schema context
5. **SQL Execution**: Execute query via MCP server
6. **Verification**: Verify results make sense
7. **Visualization**: Generate charts (if required)
8. **Response Formatting**: Format final response

## Setup

1. Install dependencies:
```bash
pip install -e .
```

2. Copy `.env.example` to `.env` and configure:
```bash
cp .env.example .env
```

3. Run the application:
```bash
uvicorn src.app:app --reload
uv run uvicorn src.app:app --reload
```

## Docker

Build and run with Docker:
```bash
docker-compose up --build
```

## API Endpoints

- `POST /api/chat` - Main chat endpoint
- `GET /api/health` - Health check
- `GET /api/schema` - Get database schema

## Project Structure

```
src/
├── app.py                 # FastAPI entry point
├── config/               # Configuration
├── api/                  # API routes and models
├── orchestrator/         # Pipeline orchestration
├── services/             # Business logic services
├── infrastructure/       # LLM, MCP, logging
└── utils/                # Utilities
```

## Development

Run tests:
```bash
pytest
```

Format code:
```bash
black src/
ruff check src/
uv run ruff check .
uv run python -m mypy src                                                             
uv run ruff format .    
```

