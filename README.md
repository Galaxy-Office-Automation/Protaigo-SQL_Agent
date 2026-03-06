# PostgreSQL Setup & Documentation

## 1. Installation Status
PostgreSQL has been installed on this system (Ubuntu 20.04).

- **Version**: PostgreSQL 12
- **Service Status**: Active/Enabled
- **Port**: 5432

## 2. Database Configuration
A setup script (`setup_db.sh`) was executed to initialize the database and user.

### Credentials
- **Username**: `app_user`
- **Password**: `StrongPassword123!`
- **Database Name**: `app_db`

## 3. Data Generation & Testing
To facilitate Zabbix load testing, we have populated the database using `pgbench` and created a script to simulate slow queries.

### Scripts
| Script | Description |
| :--- | :--- |
| `generate_data.sh` | Generates ~5,000,000 rows of test data using `pgbench`. |
| `trigger_long_query.sh` | Executes a query that sleeps for 30 seconds to simulate a long-running transaction. |

### Usage
**Generate Data (Already executed):**
```bash
./generate_data.sh
```

**Trigger Long Query:**
```bash
./trigger_long_query.sh
```

## 4. How to Connect
```bash
PGPASSWORD='StrongPassword123!' psql -h localhost -U app_user -d app_db
```

## 5. Database Schema
The database is populated with the standard TPC-B like tables from `pgbench`.

### `pgbench_accounts`
Represents bank accounts.
- **aid** (integer, PK): Account ID
- **bid** (integer): Branch ID (FK to `pgbench_branches`)
- **abalance** (integer): Account balance
- **filler** (char(84)): Padding to simulate record size

### `pgbench_branches`
Represents bank branches.
- **bid** (integer, PK): Branch ID
- **bbalance** (integer): Branch balance
- **filler** (char(88)): Padding

### `pgbench_tellers`
Represents bank tellers.
- **tid** (integer, PK): Teller ID
- **bid** (integer): Branch ID (FK to `pgbench_branches`)
- **tbalance** (integer): Teller balance
- **filler** (char(84)): Padding

### `pgbench_history`
Stores transaction history.
- **tid** (integer): Teller ID
- **bid** (integer): Branch ID
- **aid** (integer): Account ID
- **delta** (integer): Amount changed
- **mtime** (timestamp): Transaction time
- **filler** (char(22)): Padding

## 6. MySQL Database (Docker)
A MySQL 8 database is running in a Docker container named `mysql-db`.

- **Port**: 3306 (Mapped to host)
- **Database**: `sample_db`
- **Table**: `employees` (Contains sample data)

### Credentials
- **Root Password**: `RootPass123!`
- **User**: `app_user`
- **Password**: `AppPass123!`

### Connection Command
```bash
mysql -h 127.0.0.1 -P 3306 -u app_user -pAppPass123! sample_db
```
or via Docker:
```bash
sudo docker exec -it mysql-db mysql -uapp_user -pAppPass123! sample_db
```

## 7. Directory Structure
```text
DB_setup/
├── alert_resolution_api.py
├── Database_queries/
│   ├── count_long_queries.py
│   ├── detect_long_queries.py
│   ├── long_query_detection_test.ipynb
│   ├── long_query_detection_test_output.ipynb
│   ├── long_query_heavy_1.py
│   ├── long_query_heavy_2.py
│   ├── long_query_heavy_3.py
│   ├── long_query_postgres_compute.py
│   ├── long_query_postgres_sleep.py
│   ├── long_query_testing_agentic.py
│   ├── pgbench_schema.yml
│   ├── README.md
│   ├── sqls/
│   └── trigger_long_query.sh
├── database_queries.ipynb
├── DB_Sql_queires/
│   ├── detect_long_queries_details.sql
│   ├── detect_max_duration.sql
│   ├── fraud_detection.sql
│   ├── optimized_fraud_detection.sql
│   └── original_fraud_detection.sql
├── docker-compose.yml
├── Dockerfile
├── generate_data.sh
├── manual_error_trigger/
│   └── vmware_tools_alert.md
├── MYSQL_DOCUMENTATION.md
├── pass.txt
├── populate_mysql.sh
├── README.md
├── setup_db.sh
├── sql_optimizer_agent/
│   ├── agent/
│   ├── analyzer/
│   ├── api_server.log
│   ├── api_server.py
│   ├── config.py
│   ├── .env
│   ├── fastapi_server.log
│   ├── fastapi_server_nollm.py
│   ├── fastapi_server.py
│   ├── main.py
│   ├── optimizer/
│   ├── output/
│   ├── README.md
│   ├── requirements.txt
│   ├── server.log
│   ├── server_nollm.log
│   ├── tests/
│   └── validator/
├── temp/
│   ├── connect.txt
│   ├── long_running_query_optimized.sql
│   ├── long_running_query.sql
│   └── tally_db_access_notebook.ipynb
├── testing_queries/
│   ├── documentation.md
│   ├── fraud_detection_fast.sql
│   ├── fraud_detection_slow_REMOVED_FROM_ZABBIX.sql.bak
│   └── fraud_detection_slow.sql
└── trigger_long_query.sh
```
