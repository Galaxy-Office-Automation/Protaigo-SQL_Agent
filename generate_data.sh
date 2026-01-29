#!/bin/bash
set -e

# Database credentials
DB_NAME="app_db"
DB_USER="app_user"
export PGPASSWORD="StrongPassword123!"

echo "Initializing pgbench tables..."
# Initialize pgbench tables with scale factor 50
# Each scale factor unit is approx 100,000 rows.
# Scale 50 = ~5,000,000 rows in pgbench_accounts.
pgbench -h localhost -U "$DB_USER" -i -s 50 "$DB_NAME"

echo "Data generation complete."
