# Database Analysis Queries

Python scripts for banking data analysis on the pgbench dataset.

## Reports

| Script | Description |
|--------|-------------|
| `long_query_postgres_sleep.py` | Account Balance Reconciliation Report |
| `long_query_postgres_compute.py` | Fraud Detection Analysis |

## Usage

```bash
# Balance Reconciliation Report
python3 long_query_postgres_sleep.py

# Fraud Detection Analysis  
python3 long_query_postgres_compute.py
```

## Database

- **Database**: app_db
- **User**: app_user
- **Port**: 5432
