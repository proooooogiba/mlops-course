#!/bin/sh
set -eu
psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    --set=ON_ERROR_STOP=1 --set=reader_password="$GRAFANA_DB_PASSWORD" <<'SQL'
CREATE USER grafana_reader WITH PASSWORD :'reader_password';
GRANT CONNECT ON DATABASE scores TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON TABLE scores TO grafana_reader;
SQL
