FROM python:3.11.13-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends make && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/app/dbt
COPY requirements.txt /tmp/requirements-dbt.txt
RUN pip install --no-cache-dir -r /tmp/requirements-dbt.txt
ENV DBT_PROFILES_DIR=/usr/app/dbt
ENTRYPOINT ["dbt"]
