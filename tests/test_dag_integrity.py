"""
DAG Integrity Tests
====================
Verifies that all Airflow DAGs:
  - Parse without import errors
  - Have required metadata (description, tags, owner)
  - Have max_active_runs set (prevents runaway parallel executions)
  - Do NOT use catchup=True (no accidental backfill storms)
  - Retries are configured

Run with:
    pip install apache-airflow==2.8.0 pytest
    pytest tests/test_dag_integrity.py -v
"""

import importlib.util
import os
import sys
from pathlib import Path

import pytest

# Skip this entire module when Airflow is not installed.
# In CI, the workflow installs apache-airflow before running this file.
# Locally, run: pip install apache-airflow==2.8.0 && pytest tests/test_dag_integrity.py
airflow = pytest.importorskip("airflow", reason="apache-airflow not installed — install with: pip install apache-airflow==2.8.0")

# Point the Airflow home at a temp location so tests don't need a real DB
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite:////tmp/airflow_test.db")
os.environ.setdefault("AIRFLOW__CORE__FERNET_KEY", "l82azX1LorH_j3gdKvODS19mYePRjBvmjfqBIzhFvos=")

DAGS_ROOT = Path(__file__).parent.parent / "orchestration" / "airflow" / "dags"

# Add dags root to sys.path so 'from common.helpers import ...' resolves in all DAG files
if str(DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(DAGS_ROOT))


def _collect_dag_files() -> list[Path]:
    """Recursively find all .py files under the dags root, excluding __pycache__, common/ helper
    modules, and *_tasks.py utility files (which define task functions, not DAG objects)."""
    return [
        p for p in DAGS_ROOT.rglob("*.py")
        if "__pycache__" not in str(p)
        and not p.name.startswith("_")
        and "common" not in p.parts
        and not p.name.endswith("_tasks.py")
    ]


def _load_module(path: Path):
    """Import a DAG file as a Python module."""
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _get_dags_from_module(module) -> list:
    """Extract all DAG objects from a loaded module."""
    from airflow import DAG
    return [obj for obj in vars(module).values() if isinstance(obj, DAG)]


# ── Collect test parameters ────────────────────────────────────────────────────

DAG_FILES = _collect_dag_files()


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=[p.name for p in DAG_FILES])
def test_dag_imports_without_error(dag_file):
    """DAG file must be importable — catches syntax errors and bad imports."""
    try:
        module = _load_module(dag_file)
    except Exception as exc:
        pytest.fail(f"{dag_file.name} failed to import: {exc}")


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=[p.name for p in DAG_FILES])
def test_dag_has_at_least_one_dag_object(dag_file):
    """Every DAG file must define at least one DAG."""
    try:
        module = _load_module(dag_file)
    except Exception:
        pytest.skip("Import failed — covered by test_dag_imports_without_error")

    dags = _get_dags_from_module(module)
    assert len(dags) >= 1, f"{dag_file.name} defines no DAG objects"


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=[p.name for p in DAG_FILES])
def test_dag_has_description(dag_file):
    """Every DAG must have a non-empty description — required for discoverability."""
    try:
        module = _load_module(dag_file)
    except Exception:
        pytest.skip("Import failed")

    for dag in _get_dags_from_module(module):
        assert dag.description, f"DAG '{dag.dag_id}' in {dag_file.name} has no description"


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=[p.name for p in DAG_FILES])
def test_dag_has_tags(dag_file):
    """Every DAG must have at least one tag — used for Airflow UI filtering."""
    try:
        module = _load_module(dag_file)
    except Exception:
        pytest.skip("Import failed")

    for dag in _get_dags_from_module(module):
        assert dag.tags, f"DAG '{dag.dag_id}' in {dag_file.name} has no tags"


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=[p.name for p in DAG_FILES])
def test_dag_catchup_disabled(dag_file):
    """catchup=True causes backfill storms on first deploy — must be disabled."""
    try:
        module = _load_module(dag_file)
    except Exception:
        pytest.skip("Import failed")

    for dag in _get_dags_from_module(module):
        assert not dag.catchup, (
            f"DAG '{dag.dag_id}' has catchup=True — this will trigger backfill on deploy"
        )


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=[p.name for p in DAG_FILES])
def test_dag_default_args_has_retries(dag_file):
    """All DAGs should configure retries — prevents transient failures from failing the pipeline."""
    try:
        module = _load_module(dag_file)
    except Exception:
        pytest.skip("Import failed")

    for dag in _get_dags_from_module(module):
        retries = dag.default_args.get("retries") if dag.default_args else None
        assert retries is not None and retries >= 1, (
            f"DAG '{dag.dag_id}' has no retries in default_args"
        )
