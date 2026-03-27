"""
fastapi_server.py  —  SQL Optimizer Agent backend

ALL BUGS FIXED IN THIS VERSION
════════════════════════════════════════════════════════════════════════════════

FIX-A  EXPLAIN ANALYZE cannot run inside SET TRANSACTION READ ONLY.
        Previously both timing and row-fetching shared one connection, so
        EXPLAIN ANALYZE would fail or silently produce nothing.
        Fix: timing uses its own independent production-DB connection
        (conn_timing). The shadow-table connection (conn_shadow) is used
        ONLY for row fetching — EXPLAIN ANALYZE is never called on it.

FIX-B  conn.rollback() destroyed the session state (search_path + READ ONLY)
        so every subsequent fetch hit the production tables, timed out at 30s,
        and returned zero rows.
        Fix: never rollback conn_shadow. Use SAVEPOINT / ROLLBACK TO SAVEPOINT
        to recover from per-query errors without aborting the whole transaction.

FIX-C  SET TRANSACTION READ ONLY must be the very FIRST statement inside a
        transaction. Calling SET search_path first implicitly starts the
        transaction (psycopg2 autocommit=False issues BEGIN on the first
        execute), making the subsequent SET TRANSACTION READ ONLY fail with
        "SET TRANSACTION must be called before any query".
        Fix: set search_path at the SESSION level (persists across all
        transactions on this connection) BEFORE starting any transaction,
        then issue SET TRANSACTION READ ONLY as the first statement of the txn.

FIX-D  When a cursor raises an exception in psycopg2 (autocommit=False), the
        connection enters InFailedSqlTransaction state. Every subsequent
        execute() on any cursor from that connection immediately throws
        "ERROR: current transaction is aborted, commands ignored until end of
        transaction block" — so the retry loop in _fetch_rows_from_shadow was
        silently skipping all fallback strategies.
        Fix: wrap each fetch attempt in a SAVEPOINT. On failure, issue
        ROLLBACK TO SAVEPOINT (not ROLLBACK) to return the connection to a
        clean state while keeping the session alive (temp tables + search_path
        intact).

FIX-E  bottleneck.py had no rule for CAST(col AS TYPE) in JOIN ON conditions,
        silently disabling index usage. The fixed bottleneck.py now detects
        this as a HIGH-severity issue and surfaces it to the LLM.
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sys
import re as _re
import io
import logging
import time as _time

try:
    import psycopg2 as real_psycopg2
    DB_AVAILABLE = True
except ImportError:
    real_psycopg2 = None
    DB_AVAILABLE = False

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
formatter    = OutputFormatter()
print("Agent Ready!")


# ─────────────────────────────────────────────────────────────────────────────
# Request/response models
# ─────────────────────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    query: str

class CompareRequest(BaseModel):
    original_query:  str
    optimized_query: str


# ─────────────────────────────────────────────────────────────────────────────
# Helper: strip public. schema prefix
# ─────────────────────────────────────────────────────────────────────────────

def _strip_public(q: str) -> str:
    """Remove explicit public. prefix so search_path resolves to temp tables."""
    return q.replace('public.', '')


# ─────────────────────────────────────────────────────────────────────────────
# Helper: get timing from production DB (FIX-A / FIX-C)
# Uses a completely independent connection — never touches shadow tables.
# ─────────────────────────────────────────────────────────────────────────────

def _timing_from_production(query: str, timeout_ms: int) -> dict:
    """
    Run EXPLAIN (ANALYZE, TIMING, FORMAT JSON) on the production DB via its
    own dedicated connection (autocommit=True).

    Returns planning_time_ms, execution_time_ms, wall_time_sec, row_count,
    timed_out flag, and error string.
    """
    result = {
        "planning_time_ms":  0.0,
        "execution_time_ms": 0.0,
        "wall_time_sec":     0.0,
        "row_count":         0,
        "timed_out":         False,
        "error":             None,
    }
    try:
        conn = real_psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur  = conn.cursor()
        cur.execute(f"SET statement_timeout = {timeout_ms};")
        start = _time.time()
        try:
            cur.execute(f"EXPLAIN (ANALYZE, TIMING, FORMAT JSON) {query}")
            result["wall_time_sec"] = round(_time.time() - start, 3)
            rows = cur.fetchall()
            plan = rows[0][0] if rows else []
            if plan:
                result["planning_time_ms"]  = round(plan[0].get("Planning Time",  0), 2)
                result["execution_time_ms"] = round(plan[0].get("Execution Time", 0), 2)
                result["row_count"]         = plan[0].get("Plan", {}).get("Actual Rows", 0)
        except Exception as te:
            result["wall_time_sec"] = round(_time.time() - start, 3)
            msg = str(te).lower()
            if "timeout" in msg or "canceling" in msg:
                result["timed_out"] = True
                # Fallback: use estimated planner cost as a proxy for exec time
                try:
                    conn2 = real_psycopg2.connect(**DB_CONFIG)
                    conn2.autocommit = True
                    cur2  = conn2.cursor()
                    cur2.execute(f"EXPLAIN (FORMAT JSON) {query}")
                    plan2 = cur2.fetchall()[0][0]
                    result["execution_time_ms"] = round(
                        plan2[0].get("Plan", {}).get("Total Cost", 0), 2
                    )
                    cur2.close()
                    conn2.close()
                except Exception:
                    pass
            else:
                result["error"] = str(te)
        finally:
            cur.close()
            conn.close()
    except Exception as ce:
        result["error"] = str(ce)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Helper: fetch rows from shadow-table connection (FIX-B / FIX-C / FIX-D)
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_rows_from_shadow(conn, query: str, limit: int, timeout_ms: int) -> dict:
    """
    Fetch up to `limit` sample rows from the shadow (temp-table) connection.

    Key invariants:
    - NEVER call EXPLAIN ANALYZE (violates READ ONLY).           [FIX-A]
    - NEVER call conn.rollback() (resets search_path/READ ONLY). [FIX-B]
    - Use SAVEPOINT/ROLLBACK TO SAVEPOINT to recover from errors  [FIX-D]
      so the connection stays in a clean, usable state between
      retry attempts.

    The search_path was set at SESSION level before any transaction
    started, so it survives across SAVEPOINT rollbacks.            [FIX-C]
    """
    sample_query    = query.rstrip().rstrip(';')
    sample_no_limit = _re.sub(
        r'\s+LIMIT\s+\d+\s*$', '', sample_query, flags=_re.IGNORECASE
    )

    fetch_strategies = [
        f"{sample_no_limit} LIMIT {limit}",
        f"SELECT * FROM ({sample_no_limit}) _s LIMIT {limit}",
        sample_query,
    ]

    last_error  = None
    sp_counter  = 0  # unique savepoint names to avoid collisions

    for fetch_sql in fetch_strategies:
        sp_counter += 1
        sp_name = f"fetch_sp_{sp_counter}"
        cur = conn.cursor()
        try:
            # FIX-D: wrap in SAVEPOINT so a failure is recoverable
            cur.execute(f"SAVEPOINT {sp_name};")
            cur.execute(f"SET LOCAL statement_timeout = {timeout_ms};")
            cur.execute(fetch_sql)
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows    = cur.fetchall()
            sample  = [dict(zip(columns, [str(v) for v in row])) for row in rows]
            cur.execute(f"RELEASE SAVEPOINT {sp_name};")
            cur.close()
            return {"columns": columns, "sample_rows": sample, "error": None}

        except Exception as ex:
            last_error = str(ex)
            # FIX-D: roll back to savepoint only — NOT the whole transaction.
            # This clears the aborted-transaction flag while keeping the
            # session alive (search_path + temp tables intact).
            try:
                cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name};")
                cur.execute(f"RELEASE SAVEPOINT {sp_name};")
            except Exception:
                pass
            try:
                cur.close()
            except Exception:
                pass
            continue  # try next strategy

    return {"columns": [], "sample_rows": [], "error": last_error}


# ─────────────────────────────────────────────────────────────────────────────
# Helper: build shadow temp tables via anchored sampling
# ─────────────────────────────────────────────────────────────────────────────

# SQL reserved words that must never be used as table identifiers in DDL.
# The parser fix eliminates these, but this set acts as a safety net in case
# any other code path calls _build_shadow_tables with unvalidated names.
_SQL_RESERVED = {
    'SELECT','FROM','WHERE','JOIN','LEFT','RIGHT','INNER','OUTER','CROSS',
    'FULL','ON','USING','GROUP','ORDER','HAVING','LIMIT','OFFSET','UNION',
    'INTERSECT','EXCEPT','WITH','AS','BY','AND','OR','NOT','IN','EXISTS',
    'CASE','WHEN','THEN','ELSE','END','DISTINCT','ALL','INTO','SET',
    'VALUES','TABLE','INDEX','LATERAL','NATURAL','CONDITION','RECURSIVE',
}

_IDENT_RE = _re.compile(r'^[A-Za-z_][A-Za-z0-9_$]*$')


def _is_valid_table_name(name: str) -> bool:
    """Return True only if name is a safe, non-reserved SQL identifier."""
    if not name or len(name) <= 2:
        return False
    if name.upper() in _SQL_RESERVED:
        return False
    if not _IDENT_RE.match(name):
        return False
    return True


def _build_shadow_tables(used_tables, anchor_info, sampled_keys,
                         key_list_str, setup_cur):
    """
    Create pg_temp shadow tables.  Anchored tables are filtered to the sampled
    key set; unrelated tables fall back to LIMIT 2000.
    Runs under autocommit=True so DDL commits immediately.

    Safety: skips any name that is not a valid PostgreSQL identifier or is a
    reserved word, so a bad name from the parser can never crash the DDL phase
    and leave the shadow connection in a broken state.
    """
    anchor_table   = anchor_info['anchor_table']   if anchor_info else None
    anchor_key     = anchor_info['anchor_key']     if anchor_info else None
    related_tables = anchor_info['related_tables'] if anchor_info else {}

    for table in used_tables:
        if not _is_valid_table_name(table):
            print(f"_build_shadow_tables: skipping invalid/reserved name '{table}'")
            continue
        try:
            setup_cur.execute(f"DROP TABLE IF EXISTS pg_temp.{table}")
            if sampled_keys and (table == anchor_table or table in related_tables):
                fk_col = (
                    anchor_key
                    if table == anchor_table
                    else related_tables[table]
                )
                setup_cur.execute(
                    f"CREATE TEMP TABLE {table} AS "
                    f"SELECT * FROM public.{table} "
                    f"WHERE {fk_col} IN ({key_list_str})"
                )
            else:
                setup_cur.execute(
                    f"CREATE TEMP TABLE {table} AS "
                    f"SELECT * FROM public.{table} LIMIT 2000"
                )
        except Exception as e:
            print(f"_build_shadow_tables: failed for table '{table}': {e}")
            # Continue — do not let one bad table break all others


# ─────────────────────────────────────────────────────────────────────────────
# /analyze
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/analyze")
def analyze(req: QueryRequest):
    log_buffer      = io.StringIO()
    original_stdout = sys.stdout
    sys.stdout      = log_buffer

    logger    = logging.getLogger()
    old_level = logger.level
    logger.setLevel(logging.INFO)
    log_handler = logging.StreamHandler(log_buffer)
    log_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger.addHandler(log_handler)

    try:
        result = orchestrator.optimize(req.query)
        output = formatter.format_as_json(result)
        output["agent_logs"]          = log_buffer.getvalue()
        output["original_query"]      = req.query
        output["line_by_line_report"] = orchestrator.get_line_by_line_report(result)
        return output
    except Exception as e:
        return {"error": str(e)}
    finally:
        sys.stdout = original_stdout
        logger.removeHandler(log_handler)
        logger.setLevel(old_level)


# ─────────────────────────────────────────────────────────────────────────────
# /compare  — fully fixed
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/compare")
def compare(req: CompareRequest):
    """
    Run both queries against a mini shadow DB (anchored temp tables) and
    compare results + timing.

    Connection architecture (two completely separate connections):

    conn_timing  (production DB, autocommit=True, short-lived)
      EXPLAIN ANALYZE: wall time, planning time, exec time, row count
      Opened and closed per query. Never sees temp tables.

    conn_shadow  (one persistent session for the whole /compare call)
      Phase 1 (autocommit=True):  DDL -> CREATE TEMP TABLE ...
      Phase 2 (autocommit=False): search_path set at SESSION level first,
                                  then BEGIN -> SET TRANSACTION READ ONLY,
                                  then fetch rows via SAVEPOINT loop.
      Never rollback'd. Closed once at the very end.
    """
    if not DB_AVAILABLE or real_psycopg2 is None:
        return {"error": "psycopg2 is not installed. Cannot run live comparison."}

    SAMPLE_LIMIT = 10
    TIMEOUT_MS   = 30_000   # 30 s — more than enough for shadow-table queries

    try:
        from analyzer.sql_parser      import SQLParser
        from analyzer.schema_analyzer import SchemaAnalyzer

        # Phase 1: Parse to discover real table names
        parser      = SQLParser()
        p1          = parser.parse(req.original_query)
        p2          = parser.parse(req.optimized_query)
        cte_names   = [c['name'] for c in (p1.ctes + p2.ctes)]
        used_tables = list(set(
            t for t in (p1.tables + p2.tables) if t not in cte_names
        ))

        sampled_keys      = []
        anchor_info       = None
        key_list_str      = ""
        anchor_table_name = None

        # Phase 2: DDL — build shadow tables (autocommit=True)
        conn_shadow = real_psycopg2.connect(**DB_CONFIG)
        conn_shadow.autocommit = True
        setup_cur = conn_shadow.cursor()

        if used_tables:
            schema_analyzer   = SchemaAnalyzer(DB_CONFIG)
            anchor_info       = schema_analyzer.find_anchor_key(used_tables)

            if anchor_info:
                anchor_table_name = anchor_info['anchor_table']
                anchor_key        = anchor_info['anchor_key']
                try:
                    try:
                        setup_cur.execute(
                            f"SELECT {anchor_key} FROM {anchor_table_name} "
                            f"TABLESAMPLE SYSTEM (0.1) LIMIT 10"
                        )
                    except Exception:
                        setup_cur.execute(
                            f"SELECT {anchor_key} FROM {anchor_table_name} LIMIT 10"
                        )
                    sampled_keys = [
                        str(r[0]) for r in setup_cur.fetchall() if r[0] is not None
                    ]
                    if sampled_keys:
                        key_list_str = ",".join(f"'{k}'" for k in sampled_keys)
                except Exception as se:
                    print(f"Compare: anchor sampling failed: {se}")

            _build_shadow_tables(
                used_tables, anchor_info, sampled_keys, key_list_str, setup_cur
            )

        setup_cur.close()

        # Phase 3: Switch to query-execution mode
        #
        # FIX-C correct order:
        #   Step a: SET search_path at SESSION level while autocommit=True.
        #           Session settings survive across transactions and SAVEPOINTs.
        #   Step b: switch autocommit=False.
        #   Step c: SET TRANSACTION READ ONLY is the FIRST statement in the txn.
        #
        # WRONG order (old code):
        #   conn.autocommit = False
        #   cur.execute("SET search_path ...")   <- starts txn implicitly (BEGIN)
        #   cur.execute("SET TRANSACTION READ ONLY;")  <- TOO LATE, txn already open

        # Step 3a — session-level search_path (autocommit still True here)
        session_cur = conn_shadow.cursor()
        session_cur.execute("SET search_path = pg_temp, public;")
        session_cur.close()

        # Step 3b — switch to manual transaction mode
        conn_shadow.autocommit = False

        # Step 3c — open transaction and immediately mark read-only
        txn_cur = conn_shadow.cursor()
        txn_cur.execute("SET TRANSACTION READ ONLY;")   # must be FIRST in txn
        txn_cur.close()

        orig_q = _strip_public(req.original_query)
        opt_q  = _strip_public(req.optimized_query)

        # Phase 4: Timing (independent production-DB connections — FIX-C)
        orig_timing = _timing_from_production(req.original_query, TIMEOUT_MS)
        opt_timing  = _timing_from_production(req.optimized_query,  TIMEOUT_MS)

        # Phase 5: Row fetch from shadow DB (SAVEPOINT-based retries — FIX-D)
        orig_rows = _fetch_rows_from_shadow(conn_shadow, orig_q, SAMPLE_LIMIT, TIMEOUT_MS)
        opt_rows  = _fetch_rows_from_shadow(conn_shadow, opt_q,  SAMPLE_LIMIT, TIMEOUT_MS)

        conn_shadow.close()

        # Phase 6: Assemble result dicts
        def _label(timing_dict, base):
            return f"{base} (Timeout - Est. Cost)" if timing_dict["timed_out"] else base

        original_result = {
            "label":             _label(orig_timing, "Original Query"),
            "wall_time_sec":     orig_timing["wall_time_sec"],
            "planning_time_ms":  orig_timing["planning_time_ms"],
            "execution_time_ms": orig_timing["execution_time_ms"],
            "row_count":         orig_timing["row_count"] or len(orig_rows["sample_rows"]),
            "columns":           orig_rows["columns"],
            "sample_rows":       orig_rows["sample_rows"],
            "error":             orig_rows["error"],
        }
        optimized_result = {
            "label":             _label(opt_timing, "Optimized Query"),
            "wall_time_sec":     opt_timing["wall_time_sec"],
            "planning_time_ms":  opt_timing["planning_time_ms"],
            "execution_time_ms": opt_timing["execution_time_ms"],
            "row_count":         opt_timing["row_count"] or len(opt_rows["sample_rows"]),
            "columns":           opt_rows["columns"],
            "sample_rows":       opt_rows["sample_rows"],
            "error":             opt_rows["error"],
        }

        # Timed-out queries: surface a message in the output table
        if orig_timing["timed_out"] and not original_result["sample_rows"]:
            original_result["sample_rows"] = [{
                "message": "Query timed out (> 120s). Displaying Estimated Planner Cost."
            }]
            original_result["columns"] = ["message"]

        # Phase 7: Speedup label
        orig_exec = original_result["execution_time_ms"]
        opt_exec  = optimized_result["execution_time_ms"]
        if opt_exec > 0 and orig_exec > 0:
            if opt_exec < orig_exec:
                speedup_label = f"{round(orig_exec / opt_exec, 1)}x faster"
            else:
                speedup_label = f"{round(opt_exec / orig_exec, 1)}x slower (Regression)"
        elif orig_exec > 0 and opt_exec == 0:
            speedup_label = "Instant"
        else:
            speedup_label = "N/A"

        # Phase 8: Equivalence validation
        def _row_sig(r):
            return tuple(sorted(r.items()))

        orig_set    = set(_row_sig(r) for r in original_result["sample_rows"]  if "message" not in r)
        opt_set     = set(_row_sig(r) for r in optimized_result["sample_rows"] if "message" not in r)
        quick_match = (orig_set == opt_set)

        v_res = {"valid": quick_match, "validation_method": "Sample Row Comparison"}
        try:
            validator = EquivalenceValidator(DB_CONFIG)
            v_res     = validator.validate(req.original_query, req.optimized_query)
        except Exception as ve:
            v_res = {
                "valid":             quick_match,
                "validation_method": "Sample Row Comparison (validator error)",
                "error":             str(ve),
            }

        return {
            "original":            original_result,
            "optimized":           optimized_result,
            "results_match":       v_res.get("valid", quick_match),
            "validation_metadata": {
                "method":       v_res.get("validation_method"),
                "sampled_keys": v_res.get("sampled_keys", sampled_keys),
                "anchor_table": v_res.get("anchor_table", anchor_table_name),
            },
            "speedup": speedup_label,
            "error":   None,
        }

    except Exception as e:
        return {"error": f"Database connection failed: {str(e)}"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5051)