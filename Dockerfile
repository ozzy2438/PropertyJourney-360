ARG PYTHON_IMAGE="python:3.12.14-slim-bookworm@sha256:a116514e19457bcb7af7efe9c3dd0b9b71e85b317694e7882a1c52aa15a78134"

FROM ${PYTHON_IMAGE} AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    VIRTUAL_ENV=/opt/venv

RUN python -m venv "${VIRTUAL_ENV}"
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

WORKDIR /build
COPY pyproject.toml README.md ./
COPY uv.lock ./
COPY src ./src
RUN python -m pip install --no-deps "uv==0.12.5" \
    && UV_PROJECT_ENVIRONMENT="${VIRTUAL_ENV}" uv sync --locked --no-dev --no-editable --extra cloud

FROM ${PYTHON_IMAGE} AS runtime

ARG APP_UID=10001
ARG APP_GID=10001

ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PJ360_ENVIRONMENT=local \
    PJ360_LOG_LEVEL=INFO \
    PORT=8080

RUN addgroup --system --gid "${APP_GID}" app \
    && adduser --system --uid "${APP_UID}" --gid "${APP_GID}" \
        --home /nonexistent --no-create-home --disabled-login app

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
RUN install -d -o app -g app /app/data

USER app:app
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD ["python", "-c", "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:'+os.environ.get('PORT','8080')+'/health', timeout=2)"]

CMD ["uvicorn", "propertyjourney360.api.main:app", "--host", "0.0.0.0", "--port", "8080", "--no-access-log", "--proxy-headers", "--forwarded-allow-ips=*"]
