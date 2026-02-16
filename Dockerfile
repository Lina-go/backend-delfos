# Use an official Python 3.11 slim image
FROM python:3.11-slim

# Install uv directly from the official binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set the working directory
WORKDIR /app

# Install system dependencies, Microsoft ODBC Driver 18, and Azure CLI
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    gcc \
    g++ \
    make \
    unixodbc-dev \
    ca-certificates \
    apt-transport-https \
    lsb-release \
    && curl --proto '=https' -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl --proto '=https' -fsSL https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && sed -i 's/arch=amd64/& signed-by=\/usr\/share\/keyrings\/microsoft-prod.gpg/' /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && curl --proto '=https' -sL https://aka.ms/InstallAzureCLIDeb | bash \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files (both pyproject.toml and uv.lock)
COPY pyproject.toml uv.lock ./

# Install dependencies using uv
RUN uv sync --frozen --no-install-project --prerelease=allow

# Copy application code
COPY src/ ./src/

# Install the project itself
RUN uv sync --frozen --prerelease=allow

# Create logs directory
RUN mkdir -p logs

# Create non-root user and set permissions
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser \
    && chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8000

# Run the application
CMD ["uv", "run", "uvicorn", "src.app:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips", "*"]