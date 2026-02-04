-- SQL query to count long running queries in PostgreSQL
-- Returns the number of queries running longer than the specified threshold
-- Suitable for Zabbix monitoring (returns single integer)

SELECT COUNT(*)
FROM pg_stat_activity
WHERE state = 'active'
  AND query NOT ILIKE '%pg_stat_activity%'
  AND NOW() - query_start > INTERVAL '5 seconds';
