
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sys
import json
import types
import time as _time

# Import the REAL psycopg2 first (for the /compare endpoint)
try:
    import psycopg2 as real_psycopg2
    DB_AVAILABLE = True
except ImportError:
    real_psycopg2 = None
    DB_AVAILABLE = False

# Now mock psycopg2 for the optimization pipeline (avoids timeout hangs)
mock = types.ModuleType('psycopg2')
mock.Error = Exception
sys.modules['psycopg2'] = mock

sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')
from agent.orchestrator import AgentOrchestrator
from output.formatter import OutputFormatter
from config import DB_CONFIG

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

print("Initializing Agent Orchestrator... (this takes ~16s but only happens ONCE)")
orchestrator = AgentOrchestrator(use_llm=True, use_explain=False)
formatter = OutputFormatter()
print("Agent Ready!")

class QueryRequest(BaseModel):
    query: str

class CompareRequest(BaseModel):
    original_query: str
    optimized_query: str

@app.post("/analyze")
def analyze(req: QueryRequest):
    try:
        result = orchestrator.optimize(req.query)
        output = formatter.format_as_json(result)
        return output
    except Exception as e:
        return {"error": str(e)}

@app.post("/compare")
def compare(req: CompareRequest):
    """Run both queries on the real DB with timing and compare results."""
    if not DB_AVAILABLE or real_psycopg2 is None:
        return {"error": "psycopg2 is not installed. Cannot run live comparison."}

    SAMPLE_LIMIT = 10
    TIMEOUT_SEC = 30

    def _run_query(conn, query, label):
        """Execute a query, return timing and sample rows."""
        cur = conn.cursor()
        try:
            cur.execute(f"SET statement_timeout = '{TIMEOUT_SEC}s'")

            # 1) Get execution plan timing via EXPLAIN ANALYZE
            explain_query = f"EXPLAIN (ANALYZE, TIMING, FORMAT JSON) {query}"
            start = _time.time()
            cur.execute(explain_query)
            elapsed = round(_time.time() - start, 3)
            plan_rows = cur.fetchall()
            plan = plan_rows[0][0] if plan_rows else []
            planning_time = plan[0].get("Planning Time", 0) if plan else 0
            execution_time = plan[0].get("Execution Time", 0) if plan else 0

            # 2) Get sample rows for equivalence check
            sample_query = query.rstrip().rstrip(';')
            cur.execute(f"SELECT * FROM ({sample_query}) _sample LIMIT {SAMPLE_LIMIT}")
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = cur.fetchall()
            # Convert to serializable list of dicts
            sample = [dict(zip(columns, [str(v) for v in row])) for row in rows]

            cur.close()
            return {
                "label": label,
                "wall_time_sec": elapsed,
                "planning_time_ms": round(planning_time, 2),
                "execution_time_ms": round(execution_time, 2),
                "row_count": len(sample),
                "columns": columns,
                "sample_rows": sample,
                "error": None
            }
        except Exception as e:
            cur.close()
            return {
                "label": label,
                "wall_time_sec": 0,
                "planning_time_ms": 0,
                "execution_time_ms": 0,
                "row_count": 0,
                "columns": [],
                "sample_rows": [],
                "error": str(e)
            }

    try:
        conn = real_psycopg2.connect(**DB_CONFIG)
        conn.set_session(readonly=True, autocommit=True)

        original_result = _run_query(conn, req.original_query, "Original Query")
        optimized_result = _run_query(conn, req.optimized_query, "Optimized Query")

        conn.close()

        # Compare sample results
        orig_set = set(tuple(sorted(r.items())) for r in original_result["sample_rows"])
        opt_set = set(tuple(sorted(r.items())) for r in optimized_result["sample_rows"])
        results_match = orig_set == opt_set

        # Calculate speedup
        orig_exec = original_result["execution_time_ms"]
        opt_exec = optimized_result["execution_time_ms"]
        if opt_exec > 0 and orig_exec > 0:
            speedup = round(orig_exec / opt_exec, 1)
            speedup_label = f"{speedup}x faster"
        elif orig_exec > 0 and opt_exec == 0:
            speedup_label = "Instant"
        else:
            speedup_label = "N/A"

        return {
            "original": original_result,
            "optimized": optimized_result,
            "results_match": results_match,
            "speedup": speedup_label,
            "error": None
        }

    except Exception as e:
        return {"error": f"Database connection failed: {str(e)}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5051)

