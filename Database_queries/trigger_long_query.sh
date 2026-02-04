#!/bin/bash
# Script to trigger a long-running query in PostgreSQL for testing

DB_NAME="app_db"
DB_USER="app_user"
DURATION=${1:-30}  # Default 30 seconds, can override via argument

export PGPASSWORD="StrongPassword123!"

echo "Starting long-running query (${DURATION} seconds)..."
psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "SELECT pg_sleep(${DURATION});"
echo "Long query completed."
