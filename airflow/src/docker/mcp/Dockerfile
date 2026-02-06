# syntax=docker/dockerfile:1.7

ARG AIRFLOW_MCP_REPO="https://github.com/abhishekbhakat/airflow-mcp-server.git"
ARG AIRFLOW_MCP_REF="0.9.1"

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

ARG AIRFLOW_MCP_REPO
ARG AIRFLOW_MCP_REF

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /src

RUN apt-get update \
    && apt-get install --yes --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --branch "${AIRFLOW_MCP_REF}" --depth 1 "${AIRFLOW_MCP_REPO}" /src

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

FROM python:3.12-slim-bookworm AS runtime

ENV PYTHONUNBUFFERED=1 \
    APP_HOME=/app \
    VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:/root/.local/bin:${PATH}"

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --home "${APP_HOME}" --shell /usr/sbin/nologin mcp

WORKDIR /app

COPY --from=builder /root/.local /root/.local
COPY --from=builder /src/.venv /app/.venv
COPY --from=builder /src /app

RUN rm -rf .git tests dev_notes server.json .github \
    && chown -R mcp:mcp /app

USER mcp

EXPOSE 3000

ENTRYPOINT ["airflow-mcp-server"]
CMD ["--http", "--host", "0.0.0.0", "--port", "3000"]
