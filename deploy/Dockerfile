FROM python:3.13-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir . && useradd --uid 10001 --create-home app \
    && mkdir /state && chown app:app /state
USER app
CMD ["python", "-m", "uvicorn", "a2a_mq.demo:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--limit-concurrency", "64"]
