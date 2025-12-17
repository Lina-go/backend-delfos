# Use an official Python 3.11 slim image
FROM python:3.11-slim

# Install uv directly from the official binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set the working directory
WORKDIR /app

# Install system dependencies and Microsoft ODBC Driver 18
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    gcc \
    g++ \
    make \
    unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && sed -i 's/arch=amd64/& signed-by=\/usr\/share\/keyrings\/microsoft-prod.gpg/' /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files (both pyproject.toml and uv.lock)
# This allows uv to perform a deterministic install
COPY pyproject.toml uv.lock ./

# Install dependencies using uv
# --frozen ensures it uses the exact versions in uv.lock
# --no-install-project skips installing the actual app code in this step for better caching
RUN uv sync --frozen --no-install-project --prerelease=allow

# Copy application code
COPY src/ ./src/

# Install the project itself
RUN uv sync --frozen --prerelease=allow

# Create logs directory
RUN mkdir -p logs

# Expose port
EXPOSE 8000

# Run the application using uv to ensure it uses the synchronized virtual environment
CMD ["uv", "run", "uvicorn", "src.app:app", "--host", "0.0.0.0", "--port", "8000"]