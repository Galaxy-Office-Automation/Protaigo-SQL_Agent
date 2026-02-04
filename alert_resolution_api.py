#!/usr/bin/env python3
"""
Zabbix Alert Resolution API (FastAPI)
======================================
POST API that monitors Zabbix for 'PostgreSQL: long running query detected' alerts
and runs the optimized query to resolve the problem.

Docs available at: http://10.10.90.92:5050/docs
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Any
import psycopg2
import mysql.connector
from datetime import datetime
import uvicorn
import os

# FastAPI App
app = FastAPI(
    title="Zabbix Alert Resolution API",
    description="""
## Purpose
Automatically resolves Zabbix alerts for long-running PostgreSQL queries.

## How It Works
1. **Check Zabbix** for active alerts
2. **Kill slow queries** running > 1 minute in PostgreSQL
3. **Run optimized query** (fraud_detection_fast.sql)

## Query Files
- **Slow Query:** `/home/galaxy/DB_setup/testing_queries/fraud_detection_slow.sql` (~20 min)
- **Fast Query:** `/home/galaxy/DB_setup/testing_queries/fraud_detection_fast.sql` (~2 sec)
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# PostgreSQL Configuration
PG_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "database": os.getenv("PG_DATABASE", "app_db"),
    "user": os.getenv("PG_USER", "app_user"),
    "password": os.getenv("PG_PASSWORD", "StrongPassword123!")
}

# Zabbix MySQL Configuration
ZABBIX_CONFIG = {
    "host": os.getenv("ZABBIX_HOST", "13.202.102.183"),
    "port": int(os.getenv("ZABBIX_PORT", "3306")),
    "database": os.getenv("ZABBIX_DATABASE", "zabbix"),
    "user": os.getenv("ZABBIX_USER", "ai"),
    "password": os.getenv("ZABBIX_PASSWORD", "Galaxy@123")
}

# Path to fast query
FAST_QUERY_PATH = os.getenv("FAST_QUERY_PATH", "/home/galaxy/DB_setup/testing_queries/fraud_detection_fast.sql")


# Response Models
class HealthResponse(BaseModel):
    status: str
    timestamp: str


class ZabbixAlert(BaseModel):
    eventid: int
    name: str
    severity: int
    event_time: str


class ZabbixCheckResponse(BaseModel):
    timestamp: str
    status: str
    active_alerts: int
    alerts: List[dict]


class KilledQuery(BaseModel):
    pid: int
    duration_seconds: int
    query: str


class KillQueriesResult(BaseModel):
    status: str
    queries_terminated: int
    details: List[dict]


class FastQueryResult(BaseModel):
    success: bool
    rows_returned: Optional[int] = None
    execution_time_seconds: Optional[float] = None
    query_file: Optional[str] = None
    error: Optional[str] = None

class FastQueryResult(BaseModel):
    success: bool
    rows_returned: Optional[int] = None
    execution_time_seconds: Optional[float] = None
    query_file: Optional[str] = None
    error: Optional[str] = None


class AutofixAction(BaseModel):
    step: int
    title: str
    description: str
    risk_level: str
    icon: str
class ResolveAlertResponse(BaseModel):
    timestamp: str
    action: str
    resolution_status: str
    message: str
    zabbix_check: dict
    kill_queries: dict
    fast_query_execution: dict


class ActiveQuery(BaseModel):
    pid: int
    state: str
    duration_sec: int
    query: str


class StatusResponse(BaseModel):
    timestamp: str
    active_queries: int
    queries: List[dict]


# Helper Functions
def get_zabbix_alerts():
    """Check Zabbix for PostgreSQL long running query alerts."""
    try:
        conn = mysql.connector.connect(**ZABBIX_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        query = """
        SELECT 
            eventid,
            name,
            severity,
            FROM_UNIXTIME(clock) as event_time
        FROM problem
        WHERE name LIKE '%long running query%'
          AND r_eventid IS NULL
        ORDER BY clock DESC
        LIMIT 10
        """
        
        cursor.execute(query)
        alerts = cursor.fetchall()
        
        for alert in alerts:
            if alert.get('event_time'):
                alert['event_time'] = str(alert['event_time'])
        
        cursor.close()
        conn.close()
        
        return {"status": "success", "alerts": alerts}
    except Exception as e:
        return {"status": "error", "message": str(e), "alerts": []}


def kill_long_running_queries():
    """Terminate all long-running queries in PostgreSQL."""
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT pid, 
                   EXTRACT(EPOCH FROM (NOW() - query_start))::INT as duration,
                   LEFT(query, 100) as query_preview
            FROM pg_stat_activity 
            WHERE state = 'active' 
              AND NOW() - query_start > INTERVAL '1 minute'
              AND query NOT LIKE '%pg_stat_activity%'
              AND pid != pg_backend_pid()
        """)
        
        long_queries = cursor.fetchall()
        killed = []
        
        for pid, duration, query_preview in long_queries:
            cursor.execute(f"SELECT pg_terminate_backend({pid})")
            killed.append({
                "pid": pid,
                "duration_seconds": duration,
                "query": query_preview
            })
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"status": "success", "queries_terminated": len(killed), "details": killed}
    except Exception as e:
        return {"status": "error", "message": str(e), "queries_terminated": 0, "details": []}


def run_fast_query():
    """Run the optimized fraud detection query."""
    try:
        with open(FAST_QUERY_PATH, 'r') as f:
            fast_query = f.read()
        
        query_lines = [line for line in fast_query.split('\n') 
                       if not line.strip().startswith('--')]
        clean_query = '\n'.join(query_lines)
        
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        start_time = datetime.now()
        cursor.execute(clean_query)
        results = cursor.fetchall()
        end_time = datetime.now()
        
        duration = (end_time - start_time).total_seconds()
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "rows_returned": len(results),
            "execution_time_seconds": round(duration, 2),
            "query_file": FAST_QUERY_PATH
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# API Endpoints
@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint to verify the API is running."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }


@app.post("/api/resolve-alert", response_model=ResolveAlertResponse, tags=["Alert Resolution"])
async def resolve_alert():
    """
    **Resolve PostgreSQL long running query alerts.**
    
    This endpoint performs the following actions:
    1. Checks Zabbix for active 'long running query' alerts
    2. Kills all PostgreSQL queries running longer than 1 minute
    3. Executes the optimized fast query (fraud_detection_fast.sql)
    
    **Returns:** Resolution status with details about killed queries and fast query execution.
    """
    response = {
        "timestamp": datetime.now().isoformat(),
        "action": "resolve_long_running_query_alert"
    }
    
    # Step 1: Check Zabbix for alerts
    zabbix_result = get_zabbix_alerts()
    response["zabbix_check"] = {
        "status": zabbix_result["status"],
        "active_alerts": len(zabbix_result.get("alerts", [])),
        "alerts": zabbix_result.get("alerts", [])
    }
    
    # Step 2: Kill long-running queries
    kill_result = kill_long_running_queries()
    response["kill_queries"] = kill_result
    
    # Step 3: Run the fast query
    fast_result = run_fast_query()
    response["fast_query_execution"] = fast_result
    
    # Overall status
    if fast_result.get("success"):
        response["resolution_status"] = "RESOLVED"
        response["message"] = f"Terminated {kill_result.get('queries_terminated', 0)} slow queries and ran optimized query in {fast_result.get('execution_time_seconds', 0)}s"
    else:
        response["resolution_status"] = "PARTIAL"
        response["message"] = "Queries killed but fast query execution had issues"
    
    return response


@app.get("/api/check-alerts", response_model=ZabbixCheckResponse, tags=["Zabbix"])
async def check_alerts():
    """
    **Check Zabbix for active long-running query alerts.**
    
    Queries the Zabbix MySQL database for problems matching 'long running query'.
    Only returns unresolved (active) problems.
    """
    result = get_zabbix_alerts()
    return {
        "timestamp": datetime.now().isoformat(),
        "status": result["status"],
        "active_alerts": len(result.get("alerts", [])),
        "alerts": result.get("alerts", [])
    }


@app.get("/api/status", response_model=StatusResponse, tags=["PostgreSQL"])
async def get_status():
    """
    **Check PostgreSQL for currently running queries.**
    
    Returns all active queries (excluding system queries) with their PIDs,
    duration, and a preview of the query text.
    """
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT pid, state, 
                   EXTRACT(EPOCH FROM (NOW() - query_start))::INT as duration,
                   LEFT(query, 80) as query_preview
            FROM pg_stat_activity 
            WHERE state = 'active'
              AND query NOT LIKE '%pg_stat_activity%'
        """)
        
        queries = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return {
            "timestamp": datetime.now().isoformat(),
            "active_queries": len(queries),
            "queries": [
                {"pid": q[0], "state": q[1], "duration_sec": q[2], "query": q[3]}
                for q in queries
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/kill-queries", tags=["PostgreSQL"])
async def kill_queries():
    """
    **Kill all long-running PostgreSQL queries.**
    
    Terminates queries that have been running for more than 1 minute.
    Does not run the fast query afterwards.
    """
    result = kill_long_running_queries()
    return {
        "timestamp": datetime.now().isoformat(),
        **result
    }


@app.post("/api/run-fast-query", tags=["PostgreSQL"])
async def run_optimized_query():
    """
    **Run the optimized fraud detection query.**
    
    Executes the fast version of the query from:
    `/home/galaxy/DB_setup/testing_queries/fraud_detection_fast.sql`
    
    Expected execution time: ~2-3 seconds
    """
    result = run_fast_query()
    return {
        "timestamp": datetime.now().isoformat(),
        **result
    }

@app.get("/api/autofix-preview", response_model=List[AutofixAction], tags=["Autofix"])
async def get_autofix_preview():
    """
    **Preview the automated fix actions.**
    
    Returns the sequence of steps that will be executed during an autofix event.
    """
    return [
        {
          "step": 1,
          "title": "Identify Long Queries",
          "description": "Detect queries running longer than threshold",
          "risk_level": "LOW_RISK",
          "icon": "search"
        },
        {
          "step": 2,
          "title": "Terminate Queries",
          "description": "Safely terminate identified long-running queries",
          "risk_level": "MEDIUM_RISK",
          "icon": "stop"
        },
        {
          "step": 3,
          "title": "Replace with Optimized Query",
          "description": "Run optimized query in place of long-running queries",
          "risk_level": "MEDIUM_RISK",
          "icon": "zap" 
        }
    ]



if __name__ == '__main__':
    print("=" * 60)
    print("Zabbix Alert Resolution API (FastAPI)")
    print("=" * 60)
    print(f"Started: {datetime.now().isoformat()}")
    print("-" * 60)
    print("Docs:     http://10.10.90.92:5050/docs")
    print("ReDoc:    http://10.10.90.92:5050/redoc")
    print("-" * 60)
    uvicorn.run(app, host='0.0.0.0', port=5050)
