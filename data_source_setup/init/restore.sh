#!/bin/bash
set -e

echo "Restoring PostgreSQL dump..."

pg_restore \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  /docker-entrypoint-initdb.d/freshcart_db.dump

echo "Restore completed"
