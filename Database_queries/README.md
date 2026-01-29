# Database Long-Running Query Scripts

Python scripts for executing long-running database queries (20-25 minutes).
Used for testing Zabbix monitoring alerts.

## Scripts

| Script | Duration | Type |
|--------|----------|------|
| `long_query_postgres_sleep.py` | 20 min (exact) | Sleep-based |
| `long_query_postgres_compute.py` | 20-25 min | CPU-intensive |

## Requirements

```bash
pip install psycopg2-binary
```

## Usage

```bash
# Sleep-based (guaranteed 20 minutes)
python3 long_query_postgres_sleep.py

# Compute-intensive (actual database work)
python3 long_query_postgres_compute.py
```

## PostgreSQL Connection

- **Host**: localhost
- **Port**: 5432
- **Database**: app_db
- **User**: app_user
