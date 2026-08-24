ARG AIRFLOW_IMAGE="apache/airflow:3.3.1-python3.12@sha256:b01a795dfbd113bbbfdf3ee169b8f27e9a0090ccef105f1a452b3594a11ed316"
FROM ${AIRFLOW_IMAGE}

ARG AIRFLOW_VERSION="3.3.1"
ARG AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt"

COPY --chown=airflow:root pyproject.toml README.md /opt/propertyjourney360/
COPY --chown=airflow:root src /opt/propertyjourney360/src
COPY --chown=airflow:root docker/airflow/requirements.txt /opt/propertyjourney360/requirements.txt

USER root
RUN python -m venv /opt/dbt-venv \
    && chown -R airflow:root /opt/dbt-venv

USER airflow
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      --constraint "${AIRFLOW_CONSTRAINTS_URL}" \
    && pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      "/opt/propertyjourney360" \
    && pip check \
    && /opt/dbt-venv/bin/pip install --no-cache-dir \
      -r /opt/propertyjourney360/requirements.txt \
    && /opt/dbt-venv/bin/pip check

ENV PATH="/opt/dbt-venv/bin:${PATH}"
