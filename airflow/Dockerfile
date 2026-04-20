FROM apache/airflow:slim-3.2.1rc2-python3.14 

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /opt/airflow

COPY requirements.txt .

RUN uv pip install --system --no-cache -r requirements.txt

USER airflow
