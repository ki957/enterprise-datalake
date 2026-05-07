# AGENTS.md

Compact instructions for future OpenCode sessions. Every line answers: "Would an agent likely miss this without help?"

## Quick Start Commands

```bash
make base            # MUST run first - creates Docker networks + volumes
make all              # Full stack in dependency order
make ui                # FastAPI :8502 + Vite :3001 (active UI)
```

## Test Commands (Non-Obvious)

```bash
# Single test
pytest tests/test_saas_pipeline_logic.py::test_ch_insert_serialisation -v

# Integration (spins up isolated ClickHouse :18123 + PostgreSQL :15432)
make test-integration

# AI Agent smoke test (4 questions, scored 0-100, PASS ≥ 70)
cd services/ai-agent && python test_agents.py
```

## Build Order Matters

```
make base  → make security → make storage → make transform → make serving
         → make governance → make observability → make visualization
         → make sources → make streaming → make all
```

## dbt Conventions (Repo-Specific)

- **Bronze layer**: NOT in dbt - created by `setup_clickhouse_bronze.py` (S3 views over MinIO)
- **Models path → Schema mapping** (controlled by `dbt_project.yml` + `macros/generate_schema_name.sql`):
  - `models/silver/` → `staging` schema (MergeTree)
  - `models/gold/dimensions/` → `gold` schema (ReplacingMergeTree, SCD Type 2)
  - `models/gold/facts/` → `gold` schema (MergeTree, append-only)
  - `models/staging/` → `staging` schema (inline `config(schema='staging')`)
  - `models/marts/` → `gold` schema (inline `config(schema='gold')`, agent-generated)
- **dbt compile target**: Always pass `--target-path /tmp/dbt_target` (root-owned `target/` inside Docker mounts)
- **dbt run**: `cd transformation/dbt/datalake_transforms && dbt run --select <model> --no-version-check --target-path /tmp/dbt_target`

## AI Agent (11 Specialist Agents)

- **Entry point**: `services/ai-agent/server.py` (FastAPI at :8502)
- **Agent routing**: `services/ai-agent/graph/pipeline_graph.py::_route()` (keyword-scored, tie-break by `_AGENT_PRIORITY`)
- **LLM constraint**: `meta-llama/llama-4-scout-17b-16e-instruct`, `streaming=False` (Groq), ≤4 tools/agent
- **Agent-generated dbt models**: Only write to `models/marts/` (never silver/gold/staging) via `tools/dbt_write_tools.py`

### Groq Tool-Calling Rules (break if violated)

- **All optional params must be `str`** — Groq rejects `int`/`bool` defaults: `limit: int = 30` → LLM passes `"30"` → 400 error. Always use `str` and convert internally.
- **Zero-arg tools break schema generation** — every `@tool` must have at least one optional `str` param.
- **`create_chart` takes `labels: str, values: str`** — LLM passes comma-separated strings, not arrays. `_parse_list()` in `chart_tools.py` handles the conversion.
- **STEP 0 must be a compact one-liner** — verbose "Do NOT call any tools" blocks in STEP 0 bleed into technical paths and cause `Failed to call a function`. Pattern used by all agents: `ANALOGY QUESTIONS (contain "..."): answer with metaphor. TECHNICAL QUESTIONS: call tools.`
- **Response format scoping** — mark structured footers as "for technical requests only — NOT for analogy questions" or the footer appears on creative answers.
- **Mandatory tool calls** — write "ALWAYS call X" not "call X if condition". Conditional phrasing causes the step to be skipped.

## ClickHouse Quirks

- **Port**: Host = `localhost:9002` (native), Inside Docker = `clickhouse:9000`
- **No LAG/LEAD**: Use self-join on `addMonths(dt, 1)` for period-over-period
- **No concat() with Decimal**: Cast first: `toString(round(amount, 2))`
- **Date truncation**: `toStartOfMonth()` not `DATE_TRUNC`, filter: `is_current = 1` not `= true`

## Airflow DAGs

- **Loaded from host repo mount** - file edits take effect without container restart
- **11 active DAGs** in `orchestration/airflow/dags/` (see `CLAUDE.md` for schedule table)
- **API auth**: `AIRFLOW__API__AUTH_BACKENDS` must include `basic_auth`

## Streaming (Kafka + Debezium)

- **KRaft mode** (no ZooKeeper), runs via `make streaming`
- **Debezium**: MySQL CDC → Kafka topic `dbz.shopflow.*` → `streaming_enrichment` DAG (Groq AI enrichment every 5 min)

## Environment & Setup

- **Secrets**: Copy `.env.example` → `.env`, never commit real credentials
- **dbt profiles**: `~/.dbt/profiles.yml` must have `datalake_transforms` target (see `CLAUDE.md` for exact config)
- **Pre-commit**: `pip install pre-commit && pre-commit install && pre-commit run --all-files`

## Key Constraints

- **No cloud services** - everything runs locally via Docker Compose + host processes
- **Graphify outputs**: In `graphify-out/` (1324 Obsidian notes, interactive `graph.html`)
- **VS Code workspace**: Open `enterprise-datalake.code-workspace` in `.vscode/` for 9 organized folder groups
