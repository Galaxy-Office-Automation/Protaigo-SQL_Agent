#!/bin/bash
set -e

# Database credentials
DB_NAME="app_db"
DB_USER="app_user"
export PGPASSWORD="StrongPassword123!"

echo "Starting long running query (30 seconds sleep)..."
psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "SELECT pg_sleep(30);"

echo "Long query finished."
