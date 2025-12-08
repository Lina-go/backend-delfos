## Plotly chart upload demo

- Builds an Oceania life expectancy line chart with Plotly Express and nicer styling.
- Saves both interactive HTML and high-res PNG to `src/examples/output/`.
- Uploads both files to Azure Blob Storage using the existing async client.

### Prerequisites
- Python 3.11 + repo dependencies installed (`plotly`, `kaleido`, `azure-storage-blob` already listed in `pyproject.toml`).
- Azure Storage credentials via environment variables (any that apply):
  - `AZURE_STORAGE_CONNECTION_STRING`
  - `AZURE_STORAGE_ACCOUNT_URL`
  - `AZURE_STORAGE_CONTAINER_NAME` (defaults to `charts`)

### Run
```bash
python src/examples/plotly_blob_demo.py
```

Outputs:
- Local files: `src/examples/output/oceania_life_expectancy.html` and `.png`
- Logs with uploaded blob URLs (when credentials are valid); upload is skipped with a warning if not configured.

