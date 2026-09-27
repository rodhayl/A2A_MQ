FROM python:3.13.7-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip install --no-cache-dir '.[bridge,workflow,postgres,security,mcp]' \
    && python -m pip check \
    && useradd --uid 10001 --create-home --shell /usr/sbin/nologin a2amq

USER 10001:10001
EXPOSE 8000

CMD ["python", "-m", "uvicorn", "a2a_mq.production_env:create_api_from_env", "--factory", "--host", "0.0.0.0", "--port", "8000", "--no-access-log", "--limit-concurrency", "256"]
