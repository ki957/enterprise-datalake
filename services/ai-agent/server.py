"""
FastAPI backend for DataLake AI v2 — replaces the Streamlit app.py.

Runs on port 8502.  The React frontend (Vite dev server on :3001) proxies
/api/* here.  In production the React build is served as static files.

Start locally:
    cd services/ai-agent
    python server.py
"""

import asyncio
import hashlib
import json
import os
import sys
import time as _time
from contextlib import asynccontextmanager
from typing import Any

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# ── OpenTelemetry initialization (before any other imports) ───────────────────
_OTEL_ENABLED = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "") != ""
_tracer = None

if _OTEL_ENABLED:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

    provider = TracerProvider()
    processor = BatchSpanProcessor(OTLPSpanExporter())
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer("ai-agent")
    HTTPXClientInstrumentor().instrument()

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST, REGISTRY


def _get_or_create_metric(metric_cls, name, doc, labelnames=(), **kwargs):
    try:
        return metric_cls(name, doc, labelnames, **kwargs)
    except ValueError:
        return REGISTRY._names_to_collectors.get(name) or REGISTRY._names_to_collectors.get(name + "_total")


# ── Prometheus metrics ─────────────────────────────────────────────────────────
METRIC_REQUESTS = _get_or_create_metric(
    Counter, "ai_agent_requests_total", "Total API requests", ["endpoint", "agent", "status"]
)
METRIC_DURATION = _get_or_create_metric(
    Histogram, "ai_agent_request_duration_seconds", "Request duration",
    ["endpoint", "agent"], buckets=[1, 5, 10, 20, 30, 45, 60, 75, 90, 120]
)
METRIC_CACHE_HITS = _get_or_create_metric(Counter, "ai_agent_cache_hits_total", "Response cache hits")
METRIC_CACHE_MISSES = _get_or_create_metric(Counter, "ai_agent_cache_misses_total", "Response cache misses")
METRIC_TIMEOUTS = _get_or_create_metric(Counter, "ai_agent_timeout_total", "Request timeouts")
METRIC_ERRORS = _get_or_create_metric(Counter, "ai_agent_errors_total", "Request errors", ["endpoint"])
METRIC_INPUT_TOKENS = _get_or_create_metric(
    Counter, "ai_agent_input_tokens_total", "Total input tokens consumed", ["agent"]
)
METRIC_OUTPUT_TOKENS = _get_or_create_metric(
    Counter, "ai_agent_output_tokens_total", "Total output tokens consumed", ["agent"]
)
METRIC_COST_USD = _get_or_create_metric(Counter, "ai_agent_cost_usd_total", "Total cost in USD", ["agent"])
METRIC_ACTIVE_SESSIONS = _get_or_create_metric(Gauge, "ai_agent_active_sessions", "Number of active sessions")
METRIC_CACHE_SIZE = _get_or_create_metric(Gauge, "ai_agent_cache_size", "Number of entries in response cache")

sys.path.insert(0, os.path.dirname(__file__))

# ── Lazy graph init ────────────────────────────────────────────────────────────
_graph = None


def get_graph():
    global _graph
    if _graph is None:
        from graph.pipeline_graph import create_supervisor_graph
        from rag.knowledge_base import maybe_reload_knowledge_base
        try:
            maybe_reload_knowledge_base()
        except Exception:
            pass
        _graph = create_supervisor_graph()
    return _graph


def _prewarm():
    """Pre-create all singletons that add cold-start latency on first request."""
    # 1. LLM singleton (validates Groq API key — a network call)
    try:
        from agents.base import get_llm
        get_llm()
    except Exception:
        pass
    # 2. Graph compilation
    get_graph()
    # 3. ChromaDB client
    try:
        from rag.knowledge_base import get_collection
        get_collection()
    except Exception:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.get_running_loop().run_in_executor(None, _prewarm)
    yield


# ── Response cache — skips LLM for repeated identical questions ───────────────
_CACHE_TTL   = 300   # seconds (5 min — safe for analytics that refresh daily)
_resp_cache: dict[str, tuple[dict, float]] = {}

# Optional Redis-backed cache (falls back to in-memory dict if Redis unavailable)
_redis_cache = None
try:
    from cache.redis_cache import RedisCache
    _redis_cache = RedisCache(ttl=_CACHE_TTL)
except ImportError:
    pass


def _cache_key(message: str, agent: str) -> str:
    return hashlib.md5(f"{agent}:{message.strip().lower()}".encode()).hexdigest()  # noqa: S324


def _get_cached(key: str) -> dict | None:
    # Try Redis first
    if _redis_cache is not None:
        result = _redis_cache.get(key)
        if result is not None:
            METRIC_CACHE_HITS.inc()
            return result
    # Fallback to in-memory
    entry = _resp_cache.get(key)
    if entry and (_time.time() - entry[1]) < _CACHE_TTL:
        METRIC_CACHE_HITS.inc()
        return entry[0]
    _resp_cache.pop(key, None)
    METRIC_CACHE_MISSES.inc()
    return None


def _set_cached(key: str, result: dict) -> None:
    _resp_cache[key] = (result, _time.time())
    if _redis_cache is not None:
        _redis_cache.set(key, result)


# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(title="DataLake AI API", lifespan=lifespan)

if _OTEL_ENABLED:
    FastAPIInstrumentor.instrument_app(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Constants ──────────────────────────────────────────────────────────────────
ROUTING_HINTS = {
    "airbyte":       "airbyte connection: ",
    "ingestion":     "minio sync files: ",
    "quality":       "data quality test rows: ",
    "orchestration": "airflow dag pipeline: ",
    "performance":   "slow query performance: ",
    "schema":        "schema describe columns: ",
    "insight":       "",
    # These 4 agents had no hint — explicit UI selection fell through to auto-routing
    # and lost to orchestration/quality/schema on keyword scoring.
    "self_healing":  "self heal auto fix auto repair: ",
    "anomaly":       "anomaly detection detect unusual pattern run history: ",
    "nl_dbt":        "generate model create dbt model a dbt model new dbt model: ",
    "contract":      "data contract: ",
}

AGENT_STATUS = {
    "insight":       "Querying ClickHouse analytics layer…",
    "quality":       "Running data quality checks…",
    "ingestion":     "Checking MinIO / source ingestion…",
    "orchestration": "Inspecting Airflow DAGs…",
    "performance":   "Analysing slow queries & storage…",
    "schema":        "Fetching schema & dbt models…",
    "airbyte":       "Querying Airbyte connections…",
    "self_healing":  "Running self-healing diagnostics…",
    "anomaly":       "Detecting anomalies & trends…",
    "nl_dbt":        "Generating dbt model…",
    "contract":      "Building data contracts…",
}

SERVICE_PORTS = {
    "airflow":    8080,
    "clickhouse": 8123,
    "grafana":    3000,
    "superset":   8088,
    "airbyte":    8000,
    "minio":      9001,
    "dbt":        8082,
    "keycloak":   8180,
    "vault":      8200,
    "prometheus": 9090,
    "spark":      8081,
}


# ── SSE streaming helper ───────────────────────────────────────────────────────
_STREAM_CHUNK = 25    # characters per SSE event
_STREAM_DELAY = 0.006 # seconds between chunks  →  25 chars / 6ms ≈ 4 KB/s

async def chunk_stream(text: str):
    """Stream response in 25-char chunks at 6ms cadence.
    300-word response (~1 500 chars): 60 chunks × 6 ms = 360 ms total.
    Previous word-per-word at 18 ms: ~300 × 18 ms = 5 400 ms — 15× slower.
    """
    for i in range(0, len(text), _STREAM_CHUNK):
        piece = text[i : i + _STREAM_CHUNK]
        yield f"data: {json.dumps({'type': 'token', 'content': piece})}\n\n"
        await asyncio.sleep(_STREAM_DELAY)
    yield f"data: {json.dumps({'type': 'done'})}\n\n"


# ── /api/chat ──────────────────────────────────────────────────────────────────
_INVOKE_TIMEOUT = 75.0  # seconds — hard cap before returning a clean error

class ChatRequest(BaseModel):
    message:    str
    session_id: str
    agent:      str = "auto"
    history:    list = []


@app.post("/api/chat")
async def chat(req: ChatRequest):
    from graph.pipeline_graph import _route, AgentState

    if _OTEL_ENABLED and _tracer:
        with _tracer.start_as_current_span("chat_request") as span:
            span.set_attribute("session_id", req.session_id)
            span.set_attribute("agent", req.agent)
            return await _handle_chat(req)
    return await _handle_chat(req)


async def _handle_chat(req):
    from graph.pipeline_graph import _route, AgentState

    message = req.message
    if req.agent != "auto":
        message = ROUTING_HINTS.get(req.agent, "") + message

    preview: AgentState = {
        "message": req.message, "agent": "", "response": "",
        "history": [], "session_id": req.session_id, "lc_messages": [],
    }
    predicted = _route(preview) if req.agent == "auto" else req.agent

    # Check response cache for identical repeated questions
    ck = _cache_key(req.message, predicted)
    cached = _get_cached(ck)

    async def generate():
        req_start = _time.time()
        yield f"data: {json.dumps({'type': 'status', 'agent': predicted, 'message': AGENT_STATUS.get(predicted, 'Thinking…')})}\n\n"

        # ── Cache hit — stream immediately, skip LLM entirely ──────────────
        if cached:
            yield f"data: {json.dumps({'type': 'agent', 'agent': cached['agent'], 'trace': cached['trace']})}\n\n"
            async for chunk in chunk_stream(cached["response"]):
                yield chunk
            latency = _time.time() - req_start
            METRIC_DURATION.labels(endpoint="/api/chat", agent=predicted).observe(latency)
            METRIC_REQUESTS.labels(endpoint="/api/chat", agent=predicted, status="cache_hit").inc()
            return

        # ── Cache miss — invoke the graph ───────────────────────────────────
        _req_start = _time.time()
        loop = asyncio.get_running_loop()
        try:
            result = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: get_graph().invoke({
                        "message":     message,
                        "agent":       "",
                        "response":    "",
                        "history":     req.history[-6:],
                        "session_id":  req.session_id,
                        "lc_messages": [],
                    }),
                ),
                timeout=_INVOKE_TIMEOUT,
            )

            agent_used = result.get("agent", "insight")
            response   = result.get("response", "No response.")

            # Extract tool trace
            tool_trace = []
            total_input_tokens  = 0
            total_output_tokens = 0
            for msg in result.get("lc_messages", []):
                if hasattr(msg, "tool_calls") and msg.tool_calls:
                    for tc in msg.tool_calls:
                        tool_trace.append({
                            "tool":  tc.get("name", ""),
                            "input": str(tc.get("args", ""))[:200],
                        })
                elif hasattr(msg, "name") and msg.name:
                    if tool_trace:
                        tool_trace[-1]["output"] = str(msg.content)[:300]
                # Accumulate token usage from LangChain AIMessage.usage_metadata
                if hasattr(msg, "usage_metadata") and msg.usage_metadata:
                    um = msg.usage_metadata
                    total_input_tokens  += um.get("input_tokens",  0)
                    total_output_tokens += um.get("output_tokens", 0)

            # Cache before streaming
            _set_cached(ck, {"agent": agent_used, "response": response, "trace": tool_trace})

            yield f"data: {json.dumps({'type': 'agent', 'agent': agent_used, 'trace': tool_trace})}\n\n"

            async for chunk in chunk_stream(response):
                yield chunk

            # Persist memory + cost in background — don't block the stream
            latency_ms = int((_time.time() - _req_start) * 1000)
            loop.run_in_executor(None, _persist_memory,
                                  req.session_id, req.message, response, agent_used)
            if total_input_tokens or total_output_tokens:
                loop.run_in_executor(None, _log_cost,
                    req.session_id, agent_used,
                    total_input_tokens, total_output_tokens, latency_ms)

            # Record metrics
            latency = _time.time() - req_start
            METRIC_DURATION.labels(endpoint="/api/chat", agent=agent_used).observe(latency)
            METRIC_REQUESTS.labels(endpoint="/api/chat", agent=agent_used, status="ok").inc()
            METRIC_INPUT_TOKENS.labels(agent=agent_used).inc(total_input_tokens)
            METRIC_OUTPUT_TOKENS.labels(agent=agent_used).inc(total_output_tokens)
            # Groq pricing: $0.11/M input, $0.34/M output
            cost = (total_input_tokens / 1_000_000 * 0.11) + (total_output_tokens / 1_000_000 * 0.34)
            METRIC_COST_USD.labels(agent=agent_used).inc(cost)

        except asyncio.TimeoutError:
            METRIC_TIMEOUTS.inc()
            METRIC_REQUESTS.labels(endpoint="/api/chat", agent=predicted, status="timeout").inc()
            yield f"data: {json.dumps({'type': 'agent', 'agent': predicted, 'trace': []})}\n\n"
            timeout_msg = (
                "## Request Timed Out\n\n"
                f"The agent took longer than **{int(_INVOKE_TIMEOUT)} seconds** to respond.\n\n"
                "**Try:**\n\n"
                "1. Ask a simpler / more specific question\n"
                "2. Name the table directly — e.g. _\"query gold.fct_orders for monthly revenue\"_\n"
                "3. Check that ClickHouse is running: `http://localhost:8123`\n\n"
                "> **Bottom line:** Break the question into smaller parts and retry."
            )
            async for chunk in chunk_stream(timeout_msg):
                yield chunk

        except Exception as e:
            METRIC_ERRORS.labels(endpoint="/api/chat").inc()
            METRIC_REQUESTS.labels(endpoint="/api/chat", agent=predicted, status="error").inc()
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)[:200]})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


def _persist_memory(session_id: str, user_msg: str, response: str, agent: str) -> None:
    try:
        from memory.postgres_memory import save_message
        save_message(session_id, "user",      user_msg)
        save_message(session_id, "assistant", response, agent=agent)
    except Exception:
        pass


def _log_cost(session_id: str, agent: str,
              input_tokens: int, output_tokens: int, latency_ms: int) -> None:
    try:
        from memory.cost_tracker import log_call
        log_call(session_id, agent, input_tokens, output_tokens, latency_ms)
    except Exception:
        pass


# ── /api/history ───────────────────────────────────────────────────────────────
@app.get("/api/history")
async def get_history(session_id: str, limit: int = 100):
    try:
        from memory.postgres_memory import get_history as _get
        rows = _get(session_id, limit=limit)
        return {"messages": [
            {"role": r["role"], "content": r["content"], "agent": r.get("agent", "")}
            for r in rows
        ]}
    except Exception:
        return {"messages": []}


@app.get("/api/history/search")
async def search_history(session_id: str, q: str):
    try:
        from memory.postgres_memory import _conn, _ensure_table
        from psycopg2.extras import RealDictCursor
        _ensure_table()
        with _conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT role, content, agent, created_at "
                    "FROM ai_agent_memory "
                    "WHERE session_id = %s AND content ILIKE %s "
                    "ORDER BY created_at DESC LIMIT 20",
                    (session_id, f"%{q}%"),
                )
                rows = cur.fetchall()
        return {"results": [dict(r) for r in rows]}
    except Exception:
        return {"results": []}


@app.delete("/api/session")
async def clear_session(session_id: str):
    try:
        from memory.postgres_memory import clear_session as _clear
        deleted = _clear(session_id)
        return {"deleted": deleted}
    except Exception:
        return {"deleted": 0}


# ── /api/rag/reload ────────────────────────────────────────────────────────────
# Called by Airflow DAGs after schema changes (schema_evolution, streaming_enrichment).
# Re-seeds ChromaDB from all schemas + clears the 5-min response cache.

@app.post("/api/rag/reload")
async def rag_reload():
    """Force an immediate ChromaDB re-seed + clear response cache.
    Safe to call at any time. Typically triggered by Airflow after schema changes."""
    loop = asyncio.get_running_loop()
    try:
        from rag.knowledge_base import force_reload
        result = await loop.run_in_executor(None, force_reload)
        _resp_cache.clear()  # stale cached responses about old schema must go
        return {"status": "ok", "reloaded": result}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# ── /api/health ────────────────────────────────────────────────────────────────
_health_cache: dict = {"data": {}, "ts": 0.0}
_health_lock: asyncio.Lock | None = None  # created lazily inside the event loop


async def _ping(name: str, port: int) -> tuple[str, str]:
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection("localhost", port), timeout=1.0
        )
        writer.close()
        await writer.wait_closed()
        return name, "up"
    except Exception:
        return name, "down"


@app.get("/api/health")
async def health():
    global _health_lock
    if _health_lock is None:
        _health_lock = asyncio.Lock()
    if _time.time() - _health_cache["ts"] > 300:
        async with _health_lock:
            if _time.time() - _health_cache["ts"] > 300:  # re-check inside lock
                results = await asyncio.gather(*[
                    _ping(name, port) for name, port in SERVICE_PORTS.items()
                ])
                _health_cache["data"] = dict(results)
                _health_cache["ts"] = _time.time()
    return {"services": _health_cache["data"], "cached_at": _health_cache["ts"]}


# ── /metrics — Prometheus-compatible metrics endpoint ─────────────────────────
@app.get("/metrics")
async def metrics():
    METRIC_CACHE_SIZE.set(len(_resp_cache))
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ── /api/costs ─────────────────────────────────────────────────────────────────

@app.get("/api/costs/summary")
async def costs_summary(days: int = 30):
    """Overall + per-agent token/cost summary for the last N days."""
    loop = asyncio.get_running_loop()
    try:
        from memory.cost_tracker import get_summary
        data = await loop.run_in_executor(None, lambda: get_summary(days))
        return data
    except Exception as e:
        return {"error": str(e), "totals": {}, "by_agent": [], "days": days}


@app.get("/api/costs/daily")
async def costs_daily(days: int = 14):
    """Daily spend by date + agent (for the time-series chart)."""
    loop = asyncio.get_running_loop()
    try:
        from memory.cost_tracker import get_daily_costs
        rows = await loop.run_in_executor(None, lambda: get_daily_costs(days))
        # coerce Decimal / date → JSON-safe
        clean = []
        for r in rows:
            clean.append({
                k: (v.isoformat() if hasattr(v, "isoformat") else
                    float(v)       if hasattr(v, "__float__")  else v)
                for k, v in r.items()
            })
        return {"rows": clean, "days": days}
    except Exception as e:
        return {"error": str(e), "rows": [], "days": days}


@app.get("/api/costs/recent")
async def costs_recent(limit: int = 50):
    """Most recent N individual calls."""
    loop = asyncio.get_running_loop()
    try:
        from memory.cost_tracker import get_recent_calls
        rows = await loop.run_in_executor(None, lambda: get_recent_calls(limit))
        return {"rows": rows}
    except Exception as e:
        return {"error": str(e), "rows": []}


# ── Static React build (production) ───────────────────────────────────────────
_static = os.path.join(os.path.dirname(__file__), "..", "ai-agent-v2", "frontend", "dist")
if os.path.isdir(_static):
    app.mount("/", StaticFiles(directory=_static, html=True), name="frontend")


if __name__ == "__main__":
    uvicorn.run(
        "server:app",
        host="0.0.0.0",   # bind all interfaces — required for same-network mobile access
        port=8502,
        reload=True,
        reload_dirs=[os.path.dirname(__file__)],
    )
