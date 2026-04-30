import glob
import os
import subprocess
from langchain_core.tools import tool

DBT_DIR = os.getenv("DBT_PROJECT_DIR", "/dbt/datalake_transforms")


def _run_dbt(args: list[str]) -> str:
    try:
        result = subprocess.run(
            ["dbt"] + args + ["--project-dir", DBT_DIR],
            capture_output=True,
            text=True,
            timeout=300,
        )
        output = result.stdout + result.stderr
        # Return last meaningful lines
        lines = [
            l for l in output.split("\n")
            if any(kw in l for kw in ["PASS", "FAIL", "ERROR", "Completed", "Warning", "Running"])
        ]
        return "\n".join(lines[-15:]) or output[-600:]
    except subprocess.TimeoutExpired:
        return "dbt command timed out after 5 minutes."
    except FileNotFoundError:
        return "dbt not found in PATH. Is dbt installed in the container?"
    except Exception as e:
        return f"dbt command failed: {str(e)}"


@tool
def run_dbt_models(select: str = "") -> str:
    """Run dbt transformation models. Use select= to target a layer:
    'silver' for ShopFlow staging, 'gold' for dimensions+facts,
    'staging' for SaaS staging, 'marts' for SaaS gold.
    Leave empty to run all models."""
    args = ["run"]
    if select:
        args += ["--select", select]
    return _run_dbt(args)


@tool
def run_dbt_tests() -> str:
    """Run all dbt schema and data quality tests.
    Returns pass/fail summary and details of any failures."""
    return _run_dbt(["test"])


@tool
def get_dbt_model_sql(model_name: str) -> str:
    """Read the SQL source for a specific dbt model.
    Use to understand or debug transformation logic."""
    import re
    # Reject names with path traversal or shell metacharacters
    if not re.match(r'^[a-zA-Z0-9_]+$', model_name):
        return f"Invalid model name '{model_name}'. Only alphanumeric characters and underscores are allowed."
    try:
        pattern = f"{DBT_DIR}/models/**/{model_name}.sql"
        files = glob.glob(pattern, recursive=True)
        if not files:
            return f"Model '{model_name}' not found under {DBT_DIR}/models/"
        if len(files) > 1:
            names = [os.path.relpath(f, DBT_DIR) for f in files]
            return f"Ambiguous: multiple models named '{model_name}': {names}. Specify a unique name."
        with open(files[0]) as f:
            return f"SQL for {model_name} ({os.path.relpath(files[0], DBT_DIR)}):\n\n{f.read()}"
    except Exception as e:
        return f"Error reading model: {str(e)}"
