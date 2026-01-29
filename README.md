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
