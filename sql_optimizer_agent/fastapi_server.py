
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

# Removed test mock to ensure equivalence validation runs against real DB.
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')
from agent.orchestrator import AgentOrchestrator
from output.formatter import OutputFormatter
from validator.equivalence import EquivalenceValidator
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

import io
import logging

@app.post("/analyze")
def analyze(req: QueryRequest):
    log_buffer = io.StringIO()
    original_stdout = sys.stdout
    sys.stdout = log_buffer

    # Capture internal logger warnings (e.g. from ReflectionAgent)
    logger = logging.getLogger()
    old_level = logger.level
    logger.setLevel(logging.INFO)
    
    log_handler = logging.StreamHandler(log_buffer)
    formatter_log = logging.Formatter('%(levelname)s: %(message)s')
    log_handler.setFormatter(formatter_log)
    logger.addHandler(log_handler)

    try:
        result = orchestrator.optimize(req.query)
        output = formatter.format_as_json(result)
        output["agent_logs"] = log_buffer.getvalue()
        output["original_query"] = req.query
        output["line_by_line_report"] = orchestrator.get_line_by_line_report(result)
        return output
    except Exception as e:
        return {"error": str(e)}
    finally:
        sys.stdout = original_stdout
        logger.removeHandler(log_handler)
        logger.setLevel(old_level)

@app.post("/compare")
def compare(req: CompareRequest):
    """Run both queries on the real DB with timing and compare results."""
    if not DB_AVAILABLE or real_psycopg2 is None:
        return {"error": "psycopg2 is not installed. Cannot run live comparison."}

    SAMPLE_LIMIT = 10
    TIMEOUT_SEC = 120

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

            # 2) Extract actual total row count from EXPLAIN plan
            actual_rows = 0
            try:
                top_plan = plan[0].get("Plan", {}) if plan else {}
                actual_rows = top_plan.get("Actual Rows", 0)
            except Exception:
                pass

            # 3) Get sample rows for output display
            sample_query = query.rstrip().rstrip(';')
            columns = []
            sample = []

            # Strip existing LIMIT clause to replace with our own small limit
            import re as _re
            sample_q_no_limit = _re.sub(r'\s+LIMIT\s+\d+\s*$', '', sample_query, flags=_re.IGNORECASE)

            # Try multiple strategies to get sample rows
            fetch_strategies = [
                f"{sample_q_no_limit} LIMIT {SAMPLE_LIMIT}",                     # Direct LIMIT (works for CTEs)
                f"SELECT * FROM ({sample_q_no_limit}) _s LIMIT {SAMPLE_LIMIT}",  # Subquery wrap (simple queries)
                sample_query,                                                    # Run as-is (already has LIMIT)
            ]
            sample_error = None
            for fetch_sql in fetch_strategies:
                try:
                    cur.execute(f"SET statement_timeout = '{TIMEOUT_SEC}s'")
                    cur.execute(fetch_sql)
                    columns = [desc[0] for desc in cur.description] if cur.description else []
                    rows = cur.fetchall()
                    sample = [dict(zip(columns, [str(v) for v in row])) for row in rows]
                    if sample:
                        sample_error = None
                        break  # Got rows, stop trying
                except Exception as ex:
                    conn.rollback()
                    sample_error = str(ex)
                    continue

            cur.close()
            return {
                "label": label,
                "wall_time_sec": elapsed,
                "planning_time_ms": round(planning_time, 2),
                "execution_time_ms": round(execution_time, 2),
                "row_count": actual_rows if actual_rows > 0 else len(sample),
                "columns": columns,
                "sample_rows": sample,
                "error": sample_error
            }
        except Exception as e:
            err_msg = str(e).lower()
            if "timeout" in err_msg or "canceling statement" in err_msg:
                try:
                    # Rollback the aborted transaction caused by the timeout before executing anything else
                    conn.rollback()
                    # Fallback to get estimated cost using EXPLAIN
                    fallback_query = f"EXPLAIN (FORMAT JSON) {query}"
                    cur.execute(fallback_query)
                    plan_rows = cur.fetchall()
                    plan = plan_rows[0][0] if plan_rows else []
                    total_cost = plan[0].get("Plan", {}).get("Total Cost", 0) if plan else 0
                    
                    cur.close()
                    return {
                        "label": f"{label} (Timeout - Est. Cost)",
                        "wall_time_sec": TIMEOUT_SEC,
                        "planning_time_ms": 0,
                        "execution_time_ms": total_cost, # Sends cost as time to visually use the UI bar
                        "row_count": 0,
                        "columns": ["message"],
                        "sample_rows": [{"message": f"Query timed out (> {TIMEOUT_SEC}s). Displaying Estimated Planner Cost."}],
                        "error": None
                    }
                except Exception as ex:
                    pass

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
            if opt_exec < orig_exec:
                speedup = round(orig_exec / opt_exec, 1)
                speedup_label = f"{speedup}x faster"
            else:
                slowdown = round(opt_exec / orig_exec, 1)
                speedup_label = f"{slowdown}x slower (Regression)"
        elif orig_exec > 0 and opt_exec == 0:
            speedup_label = "Instant"
        else:
            speedup_label = "N/A"

        # 3) Run deep equivalence validator (Anchored Stratified Sampling)
        #    Wrapped in try/except so a validator timeout never kills the compare response
        v_res = {"valid": results_match, "validation_method": "Sample Row Comparison"}
        try:
            validator = EquivalenceValidator(DB_CONFIG)
            v_res = validator.validate(req.original_query, req.optimized_query)
        except Exception as ve:
            v_res = {"valid": results_match, "validation_method": "Sample Row Comparison (validator timeout)", "error": str(ve)}

        return {
            "original": original_result,
            "optimized": optimized_result,
            "results_match": v_res.get("valid", results_match),
            "validation_metadata": {
                "method": v_res.get("validation_method"),
                "sampled_keys": v_res.get("sampled_keys", []),
                "anchor_table": v_res.get("anchor_table")
            },
            "speedup": speedup_label,
            "error": None
        }

    except Exception as e:
        return {"error": f"Database connection failed: {str(e)}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5051)

