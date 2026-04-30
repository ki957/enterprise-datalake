"""
Quick smoke-test: run 2 questions against model_agent (nl_dbt) and contracts_agent.
Scores each answer and prints PASS / NEEDS WORK.
"""
import sys, textwrap

from agents.nl_dbt_agent import create_nl_dbt_agent
from agents.contract_agent import create_contract_agent
from langchain_core.messages import HumanMessage

# ── Scoring criteria ──────────────────────────────────────────────────────────

def score_model_answer(question: str, answer: str) -> tuple[int, list[str]]:
    """Score a model-agent answer 0-100. Returns (score, issues)."""
    issues = []
    score = 100
    lo = answer.lower()

    # Must contain model name
    if "model created" not in lo and "**model" not in lo and "model_name" not in lo:
        issues.append("Missing '**Model created:**' header")
        score -= 20

    # Must reference source tables (ref(), gold.table, or config block)
    if ("ref(" not in answer and "{{ config" not in answer
            and "gold." not in answer and "staging." not in answer):
        issues.append("No source table reference in answer")
        score -= 25

    # Must tell user how to query
    if "select * from" not in lo and "to query" not in lo:
        issues.append("No 'To query:' instruction")
        score -= 10

    # Shouldn't apologise / say it can't do it
    for phrase in ["i cannot", "i don't have", "i'm unable", "not able to"]:
        if phrase in lo:
            issues.append(f"Agent hedged: '{phrase}'")
            score -= 30

    return max(score, 0), issues


def score_contract_answer(question: str, answer: str) -> tuple[int, list[str]]:
    """Score a contracts-agent answer 0-100. Returns (score, issues)."""
    issues = []
    score = 100
    lo = answer.lower()

    # Must mention expectations were written
    if "expectation" not in lo:
        issues.append("No mention of 'expectation' in answer")
        score -= 30

    # Must show table profiled
    if "table profiled" not in lo and "profiled" not in lo and "profile" not in lo:
        issues.append("Didn't mention profiling the table")
        score -= 20

    # Must show count of expectations
    import re
    if not re.search(r'\d+ expectation', lo):
        issues.append("No expectations count in answer")
        score -= 10

    # Must have key rules section or bullet list
    if "key rule" not in lo and "null" not in lo and "between" not in lo:
        issues.append("No key rules / constraint details mentioned")
        score -= 15

    # Shouldn't apologise
    for phrase in ["i cannot", "i don't have", "i'm unable"]:
        if phrase in lo:
            issues.append(f"Agent hedged: '{phrase}'")
            score -= 30

    return max(score, 0), issues


# ── Test runner ───────────────────────────────────────────────────────────────

def run_agent(agent, question: str) -> str:
    result = agent.invoke(
        {"messages": [HumanMessage(content=question)]},
        config={"recursion_limit": 16},
    )
    return result["messages"][-1].content


def run_suite(name: str, factory, questions: list[str], scorer):
    print(f"\n{'='*65}")
    print(f"  AGENT: {name.upper()}")
    print(f"{'='*65}")
    agent = factory()
    all_pass = True
    for i, q in enumerate(questions, 1):
        print(f"\n--- Q{i}: {q}")
        try:
            answer = run_agent(agent, q)
        except Exception as e:
            print(f"  ❌ EXCEPTION: {e}")
            all_pass = False
            continue

        score, issues = scorer(q, answer)
        status = "✅ PASS" if score >= 70 else "⚠️  NEEDS WORK"
        print(f"\n  {status}  (score={score}/100)")
        if issues:
            for iss in issues:
                print(f"    - {iss}")
        print("\n  ANSWER PREVIEW:")
        print(textwrap.indent(answer[:600] + ("…" if len(answer) > 600 else ""), "    "))
        if score < 70:
            all_pass = False
    return all_pass


# ── Questions ─────────────────────────────────────────────────────────────────

MODEL_QUESTIONS = [
    "Generate a dbt model for monthly revenue by product category",
    "Create a dbt model that shows the top 10 customers by total spend",
]

CONTRACT_QUESTIONS = [
    "Generate data contract expectations for gold.fct_orders",
    "Create an expectation suite for gold.dim_customers",
]


if __name__ == "__main__":
    results = {}

    results["model"] = run_suite(
        "Model Agent (nl_dbt)",
        create_nl_dbt_agent,
        MODEL_QUESTIONS,
        score_model_answer,
    )

    results["contract"] = run_suite(
        "Contracts Agent",
        create_contract_agent,
        CONTRACT_QUESTIONS,
        score_contract_answer,
    )

    print(f"\n{'='*65}")
    print("  FINAL RESULTS")
    print(f"{'='*65}")
    for agent, passed in results.items():
        status = "✅ PASS" if passed else "❌ NEEDS WORK"
        print(f"  {agent:20s} {status}")

    if not all(results.values()):
        sys.exit(1)
