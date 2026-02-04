-- SQL query to detect exact long running queries in PostgreSQL
-- Returns details of queries running longer than the specified threshold

SELECT 
    pid,
    usename AS username,
    datname AS database,
    client_addr,
    query_start,
    NOW() - query_start AS duration,
    state,
    LEFT(query, 200) AS query_text
FROM pg_stat_activity
WHERE state = 'active'
  AND query NOT ILIKE '%pg_stat_activity%'
  AND NOW() - query_start > INTERVAL '5 seconds'
ORDER BY duration DESC;
