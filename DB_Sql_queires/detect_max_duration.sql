-- Maximum Query Duration for Zabbix Monitoring
-- Returns max duration in seconds of any active query
-- Zabbix can trigger alert if value > threshold
-- Run interval: Every 1-3 seconds

SELECT COALESCE(
    MAX(EXTRACT(EPOCH FROM (NOW() - query_start)))::INTEGER,
    0
) as max_query_duration_seconds
FROM pg_stat_activity
WHERE state = 'active'
  AND query NOT LIKE '%pg_stat_activity%'
  AND datname = 'app_db';
