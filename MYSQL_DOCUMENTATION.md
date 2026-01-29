# MySQL Docker Database Documentation

## Overview
A MySQL 8 database running in a Docker container for testing and development purposes.

## Container Details
| Property | Value |
|----------|-------|
| **Container Name** | `mysql-db` |
| **Image** | `mysql:8` |
| **Host Port** | `3306` |
| **Container Port** | `3306` |
| **Database** | `sample_db` |

## Credentials
| User | Password | Access |
|------|----------|--------|
| `root` | `RootPass123!` | Full admin |
| `app_user` | `AppPass123!` | `sample_db` only |

## Connection Strings

### MySQL CLI (via Docker)
```bash
sudo docker exec -it mysql-db mysql -uapp_user -pAppPass123! sample_db
```

### MySQL CLI (from host)
```bash
mysql -h 127.0.0.1 -P 3306 -u app_user -pAppPass123! sample_db
```

### Python (SQLAlchemy)
```python
mysql+pymysql://app_user:AppPass123!@localhost:3306/sample_db
```

### JDBC
```
jdbc:mysql://localhost:3306/sample_db?user=app_user&password=AppPass123!
```

---

## Setup Commands

### Start Container
```bash
sudo docker run --name mysql-db \
  -e MYSQL_ROOT_PASSWORD=RootPass123! \
  -e MYSQL_DATABASE=sample_db \
  -e MYSQL_USER=app_user \
  -e MYSQL_PASSWORD=AppPass123! \
  -p 3306:3306 -d mysql:8
```

### Stop/Start/Remove
```bash
sudo docker stop mysql-db
sudo docker start mysql-db
sudo docker rm mysql-db
```

---

## Zabbix Monitoring

### Prerequisites
1. Install `zabbix-agent2` on the Docker host.
2. Enable the MySQL plugin in Zabbix Agent 2.

### Configure Zabbix Agent 2 for MySQL
Edit `/etc/zabbix/zabbix_agent2.d/plugins.d/mysql.conf`:
```ini
Plugins.Mysql.Sessions.sample_db.Uri=tcp://127.0.0.1:3306
Plugins.Mysql.Sessions.sample_db.User=app_user
Plugins.Mysql.Sessions.sample_db.Password=AppPass123!
```

Restart agent:
```bash
sudo systemctl restart zabbix-agent2
```

### Key Metrics to Monitor
| Metric | Zabbix Key |
|--------|------------|
| Status | `mysql.ping[sample_db]` |
| Uptime | `mysql.uptime[sample_db]` |
| Connections | `mysql.status[sample_db,Threads_connected]` |
| Slow Queries | `mysql.status[sample_db,Slow_queries]` |
| Queries/sec | `mysql.status[sample_db,Questions]` |

### Long-Running Query Alert
Create a trigger in Zabbix:
- **Name**: `MySQL: Long running query detected`
- **Expression**: `last(/Host/mysql.status[sample_db,Slow_queries])>0`
- **Severity**: Warning

### Enable Slow Query Log in MySQL
```sql
SET GLOBAL slow_query_log = 'ON';
SET GLOBAL long_query_time = 5;  -- Queries > 5 seconds
```

---

## Sample Data
The `employees` table contains 80 sample records.

```sql
SELECT * FROM employees LIMIT 5;
```

| id | name | position | salary | hire_date |
|----|------|----------|--------|-----------|
| 1 | Alice Smith | Engineer | 90000 | 2023-01-15 |
| 2 | Bob Jones | Manager | 120000 | 2022-05-20 |
| 3 | Charlie Brown | Analyst | 75000 | 2023-08-10 |
| 4 | Diana Prince | Director | 150000 | 2021-11-01 |
| 5 | Evan Wright | Developer | 85000 | 2023-03-30 |
