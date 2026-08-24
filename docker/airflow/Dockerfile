ARG AIRFLOW_IMAGE="apache/airflow:3.3.1-python3.12@sha256:b01a795dfbd113bbbfdf3ee169b8f27e9a0090ccef105f1a452b3594a11ed316"
FROM ${AIRFLOW_IMAGE}

ARG PJ360_SOURCE_TREE_SHA256=UNVERIFIED

LABEL io.propertyjourney.source-tree-sha256="${PJ360_SOURCE_TREE_SHA256}"

COPY --chown=airflow:root pyproject.toml README.md uv.lock /opt/propertyjourney360/
COPY --chown=airflow:root src /opt/propertyjourney360/src
COPY --chown=airflow:root docker/airflow/dbt /opt/dbt-runtime

USER root
RUN python -m venv /opt/pj360-venv \
    && python -m venv /opt/dbt-venv \
    && chown -R airflow:root /opt/pj360-venv /opt/dbt-venv

USER airflow
RUN pip check \
    && /opt/pj360-venv/bin/python -m pip install --no-cache-dir --no-deps "uv==0.12.5" \
    && cd /opt/propertyjourney360 \
    && UV_PROJECT_ENVIRONMENT=/opt/pj360-venv /opt/pj360-venv/bin/uv sync \
      --locked --no-dev --no-editable --extra cloud --extra postgres \
    && /opt/pj360-venv/bin/pip check \
    && /opt/dbt-venv/bin/python -m pip install --no-cache-dir --no-deps "uv==0.12.5" \
    && cd /opt/dbt-runtime \
    && UV_PROJECT_ENVIRONMENT=/opt/dbt-venv /opt/dbt-venv/bin/uv sync \
      --locked --no-dev --no-install-project \
    && /opt/dbt-venv/bin/pip check

ENV PATH="/opt/pj360-venv/bin:/opt/dbt-venv/bin:${PATH}"
