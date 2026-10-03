# ---------------------------
# Stage 1: Builder
# ---------------------------

ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim AS builder 

# Copy uv binary from official image (fast modern dependency manager)
COPY --from=ghcr.io/astral-sh/uv:0.4.20 /uv /bin/uv

# Enable uv to use system Python instead of isolated environment
ENV UV_SYSTEM_PYTHON=1

# Set working directory inside builder container
WORKDIR /app

# Copy only dependency definition first (better Docker layer caching)
COPY pyproject.toml ./

# Copy application source code
COPY scripts/ ./scripts/

# Install dependencies using uv (creates environment / installs packages)
RUN uv pip install --system --requirement pyproject.toml


# ---------------------------
# Stage 2: Runtime
# ---------------------------

FROM python:${PYTHON_VERSION}-slim AS runtime 

# Copy uv binary from official image (fast modern dependency manager)
COPY --from=ghcr.io/astral-sh/uv:0.4.20 /uv /bin/uv

# Set working directory in final container
WORKDIR /app

# Copy installed Python environment from builder stage
COPY --from=builder /usr/local /usr/local

# Copy application source code only (no build tools)
COPY --from=builder /app/scripts ./scripts

# Ensure Python output is not buffered (useful for logs in Docker)
ENV PYTHONUNBUFFERED=1

# Command to start the notebook server when the container runs
CMD ["marimo", "edit", "--no-token", "--host", "0.0.0.0", "-p", "8080"]