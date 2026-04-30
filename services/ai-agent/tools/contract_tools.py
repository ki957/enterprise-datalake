"""
Tools for the AI data contract agent.

The contract engine works in 3 steps:
  1. profile_table_stats (in profiling_tools.py) — collects statistics
  2. Agent (LLM) generates Great Expectations JSON from those statistics
  3. write_expectations (here) — persists the generated JSON to disk

Expectations are written to:
  governance/great_expectations/expectations/{schema}_{table}.json

Every write is logged to the audit store. Existing files are backed up
with a .bak extension before overwriting so diffs can be reviewed.
"""

import json
import os
import shutil
from pathlib import Path
from langchain_core.tools import tool

from memory.audit_store import log_action

_GE_EXPECTATIONS_DIR = Path(
    os.getenv(
        "GE_EXPECTATIONS_DIR",
        "/home/kishore/enterprise-datalake/governance/great_expectations/expectations",
    )
)

# Minimal valid GE expectation suite template
_SUITE_TEMPLATE = {
    "expectation_suite_name": "",
    "ge_cloud_id": None,
    "expectations": [],
    "data_asset_type": None,
    "meta": {
        "great_expectations_version": "0.18.0",
        "generated_by": "ai_contract_agent",
    },
}


@tool
def list_expectation_suites(filter_prefix: str = "") -> str:
    """List all existing Great Expectations suites.
    Shows the suite name and how many expectations each has.
    filter_prefix: optional prefix to narrow results (e.g. 'gold_' or '')."""
    try:
        pattern = f"{filter_prefix}*.json" if filter_prefix else "*.json"
        files = sorted(_GE_EXPECTATIONS_DIR.glob(pattern))
        if not files:
            return "No expectation suites found yet."
        lines = ["**Existing expectation suites:**\n"]
        for f in files:
            try:
                data  = json.loads(f.read_text())
                count = len(data.get("expectations", []))
                lines.append(f"- `{f.stem}` — {count} expectations")
            except Exception:
                lines.append(f"- `{f.stem}` — (unreadable)")
        return "\n".join(lines)
    except Exception as e:
        return f"Error listing suites: {e}"


@tool
def write_expectations(table: str, schema: str, expectations_json: str) -> str:
    """Write AI-generated Great Expectations to disk for a table.
    Backs up any existing suite before overwriting.

    table: table name (e.g. 'fct_orders')
    schema: schema name (e.g. 'gold')
    expectations_json: JSON array of expectation objects, e.g.:
      [
        {"expectation_type": "expect_column_values_to_not_be_null",
         "kwargs": {"column": "order_id"}},
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "revenue", "min_value": 0}}
      ]"""
    suite_name = f"{schema}_{table}"
    out_path   = _GE_EXPECTATIONS_DIR / f"{suite_name}.json"
    target     = {"suite": suite_name, "path": str(out_path)}

    # Parse + validate the expectations JSON the LLM generated
    try:
        expectations = json.loads(expectations_json)
        if not isinstance(expectations, list):
            return "Error: expectations_json must be a JSON array of expectation objects."
    except json.JSONDecodeError as e:
        return f"Error: expectations_json is not valid JSON — {e}"

    # Basic structure validation
    for i, exp in enumerate(expectations):
        if "expectation_type" not in exp or "kwargs" not in exp:
            return (
                f"Error: expectation #{i} is missing 'expectation_type' or 'kwargs'. "
                "Each expectation must have both keys."
            )

    # Backup existing suite before overwriting
    if out_path.exists():
        backup = out_path.with_suffix(".json.bak")
        shutil.copy2(out_path, backup)

    # Build full suite document
    suite = dict(_SUITE_TEMPLATE)
    suite["expectation_suite_name"] = suite_name
    suite["expectations"] = [
        {
            "expectation_type": e["expectation_type"],
            "kwargs":           e["kwargs"],
            "meta":             e.get("meta", {}),
        }
        for e in expectations
    ]

    try:
        _GE_EXPECTATIONS_DIR.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(suite, indent=2))
        log_action(
            "write_expectations",
            target,
            "success",
            reasoning=f"{len(expectations)} expectations written for {schema}.{table}",
        )
        backed_up = " (previous version backed up to `.json.bak`)" if out_path.with_suffix(".json.bak").exists() else ""
        return (
            f"✅ Expectation suite **`{suite_name}`** written{backed_up}.\n"
            f"Path: `{out_path}`\n"
            f"Expectations: {len(expectations)}\n\n"
            f"Run with: `python governance/great_expectations/run_checkpoint.py`"
        )
    except Exception as e:
        log_action("write_expectations", target, "error", error=str(e))
        return f"Error writing suite: {e}"
