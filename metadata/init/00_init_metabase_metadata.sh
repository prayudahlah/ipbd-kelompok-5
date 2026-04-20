#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE $METABASE_METADATA_DB;
EOSQL

echo "[SUKSES] Database $METABASE_METADATA_DB berhasil dibuat"
