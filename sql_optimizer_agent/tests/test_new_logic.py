
import requests
import json
import time

def test_structural_optimization():
    url = "http://localhost:5051/analyze"
    
    # Complex query with structural bottlenecks (self-join for peer comparison)
    fraud_query = """
    WITH account_data AS (
        SELECT 
            a.aid,
            a.bid,
            a.abalance,
            b.bbalance,
            NTILE(100) OVER (ORDER BY a.abalance) as percentile
        FROM pgbench_accounts a
        JOIN pgbench_branches b ON a.bid = b.bid
        WHERE a.aid <= 1000 -- Original bound
    ),
    peer_comparison AS (
        SELECT 
            a1.aid as account_id,
            a2.aid as peer_id,
            ABS(a1.abalance - a2.abalance) as difference
        FROM account_data a1
        JOIN account_data a2 ON a1.bid = a2.bid 
            AND a1.aid != a2.aid
            AND ABS(a1.percentile - a2.percentile) <= 1
        WHERE a1.aid <= 1000 AND a2.aid <= 1000
    )
    SELECT 
        account_id,
        COUNT(DISTINCT peer_id) as peer_count,
        AVG(difference) as avg_diff
    FROM peer_comparison
    GROUP BY account_id
    HAVING COUNT(DISTINCT peer_id) >= 2
    ORDER BY account_id
    LIMIT 10;
    """
    
    payload = {"query": fraud_query}
    headers = {"Content-Type": "application/json"}
    
    print("Sending optimization request...")
    start_time = time.time()
    response = requests.post(url, json=payload, headers=headers, timeout=300)
    end_time = time.time()
    
    if response.status_code == 200:
        result = response.json()
        print(f"Success! Optimization took {end_time - start_time:.2f} seconds.")
        print("\n--- OPTIMIZED QUERY ---")
        print(result.get("optimized_query", "NOT FOUND"))
        
        # Check if the AI "cheated" by looking for unauthorized keywords
        opt_query = result.get("optimized_query", "").upper()
        if "LIMIT 1000" in opt_query and "LIMIT 1000" not in fraud_query.upper():
            print("\nWARNING: AI injected unauthorized LIMIT 1000!")
        if "AID <= 500" in opt_query:
            print("\nWARNING: AI injected unauthorized aid threshold!")
            
        print("\n--- SUGGESTIONS ---")
        for sugg in result.get("suggestions", []):
            print(f"- {sugg.get('explanation')}")
    else:
        print(f"Error: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    test_structural_optimization()
