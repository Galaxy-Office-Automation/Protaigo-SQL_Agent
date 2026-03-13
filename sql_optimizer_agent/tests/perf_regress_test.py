import sys
import os
sys.path.insert(0, os.getcwd())
import psycopg2
from config import DB_CONFIG
from optimizer.rewriter import QueryRewriter

Q = """
SELECT
  a.aid AS account_id,
  a.bid AS branch_id,
  a.abalance AS account_balance,
  b.bbalance AS branch_balance,
  (SELECT COUNT(*) FROM pgbench_history h WHERE h.aid = a.aid) AS total_transactions,
  (SELECT COALESCE(SUM(delta), 0) FROM pgbench_history h WHERE h.aid = a.aid) AS total_transaction_volume,
  (SELECT MAX(mtime) FROM pgbench_history h WHERE h.aid = a.aid) AS last_transaction_time
FROM pgbench_accounts a
JOIN pgbench_branches b ON a.bid = b.bid
WHERE a.abalance > (
  SELECT AVG(a2.abalance)
  FROM pgbench_accounts a2
  WHERE a2.bid = a.bid
)
AND a.aid IN (
  SELECT h.aid
  FROM pgbench_history h
  GROUP BY h.aid
  HAVING COUNT(*) >= 2
)
AND CAST(a.abalance AS TEXT) LIKE '%5%'
ORDER BY total_transactions DESC, account_balance DESC
LIMIT 500;
"""

def test_perf():
    rewriter = QueryRewriter()
    optimized = rewriter.rewrite_correlated_subqueries(Q)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print("--- ORIGINAL PLAN ---")
    cur.execute("EXPLAIN ANALYZE " + Q)
    for row in cur.fetchall():
        print(row[0])
        
    print("\n--- OPTIMIZED QUERY ---")
    print(optimized)
    
    print("\n--- OPTIMIZED PLAN ---")
    try:
        cur.execute("EXPLAIN ANALYZE " + optimized)
        for row in cur.fetchall():
            print(row[0])
    except Exception as e:
        print(f"FAILED: {e}")
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    test_perf()
