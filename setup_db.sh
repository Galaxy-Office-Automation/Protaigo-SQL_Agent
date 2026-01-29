#!/bin/bash
set -e

# Create a PostgreSQL user 'app_user' with a password
sudo -u postgres psql -c "CREATE USER app_user WITH PASSWORD 'StrongPassword123!';" || echo "User might already exist"

# Create a database 'app_db' owned by 'app_user'
sudo -u postgres psql -c "CREATE DATABASE app_db OWNER app_user;" || echo "Database might already exist"

# Grant privileges (redundant if owner, but good practice to be explicit if schemas involved later)
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE app_db TO app_user;"
