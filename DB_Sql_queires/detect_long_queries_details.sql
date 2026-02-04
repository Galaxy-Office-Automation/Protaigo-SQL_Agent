-- Long Running Query Details for Zabbix Monitoring
-- Returns details of queries running longer than 15 minutes
-- Run interval: Every 3 seconds

SELECT 
    pid,
    usename,
    datname,
    state,
    EXTRACT(EPOCH FROM (NOW() - query_start))::INTEGER as duration_seconds,
    ROUND(EXTRACT(EPOCH FROM (NOW() - query_start)) / 60, 2) as duration_minutes,
    LEFT(query, 100) as query_preview,
    query_start,
    client_addr
FROM pg_stat_activity
WHERE state = 'active'
  AND query NOT LIKE '%pg_stat_activity%'
  AND query_start < NOW() - INTERVAL '15 minutes'
  AND datname = 'app_db'
ORDER BY query_start ASC;
