SELECT a.aid AS account_id, a.bid AS branch_id, a.abalance AS account_balance, b.bbalance AS branch_balance, (
SELECT COUNT(*) 
FROM pgbench_history h 
WHERE h.aid = a.aid) AS total_transactions, (
SELECT COALESCE(SUM(delta), 0) 
FROM pgbench_history h 
WHERE h.aid = a.aid) AS total_transaction_volume, (
SELECT MAX(mtime) 
FROM pgbench_history h 
WHERE h.aid = a.aid) AS last_transaction_time 
FROM pgbench_accounts a JOIN pgbench_branches b ON a.bid = b.bid 
WHERE a.abalance > ( 
SELECT AVG(a2.abalance) 
FROM pgbench_accounts a2 
WHERE a2.bid = a.bid ) AND a.aid IN ( 
SELECT h.aid 
FROM pgbench_history h 
GROUP BY h.aid 
HAVING COUNT(*) >= 2 ) AND CAST(a.abalance AS TEXT) LIKE '%5%' 
ORDER BY total_transactions DESC, account_balance DESC 
LIMIT 500;
