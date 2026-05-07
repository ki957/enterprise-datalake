"""
AI data contract agent — auto-generates Great Expectations from ClickHouse stats (#1).

Replaces hand-written GE rules with LLM-generated expectations that adapt
to the actual data distributions. The agent:
  1. Profiles the table (null rates, cardinality, percentiles)
  2. Decides what expectations make sense for each column
  3. Writes a versioned GE suite to governance/great_expectations/expectations/

The LLM does the reasoning about which expectations to generate.
The tools handle the profiling and file I/O deterministically.

Token discipline: system prompt is ~220 tokens. The profile output is the
main token cost — capped at 20 columns in profile_table_stats.
"""

from langgraph.prebuilt import create_react_agent

from agents.base import get_llm
from tools.profiling_tools import profile_table_stats
from tools.contract_tools import list_expectation_suites, write_expectations
from tools.clickhouse_tools import query_clickhouse


_SYSTEM_PROMPT = """You are a data contract agent. You generate Great Expectations rules from real data statistics.

ANALOGY QUESTIONS (contain "explain like", "analogy", "metaphor", "imagine", "like a", "as if",
"story", "pretend", "handshake", "market", "supplier", "buyer", "legal", "clauses", "kid", "teach me",
"what is a data contract", "what are data contracts"): answer with a vivid creative analogy
about data quality guarantees — training knowledge is enough, no tools needed.

TECHNICAL QUESTIONS (contain a table name like "fct_orders", "gold.", "staging.", "create",
"generate", "profile", "write expectations", "build contract"):
WORKFLOW — follow IN ORDER:
1. Call list_expectation_suites(filter_prefix="") to see what already exists.
2. Call profile_table_stats(table, schema) to get column statistics.
   - Gold tables: profile_table_stats('fct_orders', 'gold')
   - Staging tables: profile_table_stats('stg_orders', 'staging')
3. Build expectations from the profile:
   - null_rate = 0% → expect_column_values_to_not_be_null
   - Numeric columns → expect_column_values_to_be_between (p5 as min, p95*1.5 as max)
   - String columns cardinality < 20 → expect_column_values_to_be_in_set
   - All tables → expect_table_row_count_to_be_between (0.5x to 2x row count)
   - PK columns (id, *_key, *_id) → expect_column_values_to_be_unique
4. Call write_expectations(table, schema, expectations_json) with a compact JSON string.
   REQUIRED — do not skip. Do not echo the JSON in your text response.
5. After write_expectations succeeds, output:
   **Table profiled:** schema.table
   **Expectations generated:** N
   **Key rules:** bullet list, one line each
   **Coverage gaps:** columns skipped and why"""


def create_contract_agent():
    """Return a compiled LangGraph ReAct agent for AI data contract generation."""
    llm   = get_llm()
    tools = [
        profile_table_stats,
        list_expectation_suites,
        write_expectations,
        query_clickhouse,
    ]
    return create_react_agent(llm, tools, prompt=_SYSTEM_PROMPT)
