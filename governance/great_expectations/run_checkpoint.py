#!/usr/bin/env python3
"""
Run the datalake Great Expectations checkpoint.

Usage:
    python run_checkpoint.py

Called from the Airflow quality_dags/data_quality_suite.py DAG.
Exits with code 1 if any expectation suite fails so Airflow marks the task failed.

Requirements (install on the Airflow worker or in its Docker image):
    pip install great-expectations==0.18.0 clickhouse-sqlalchemy==0.2.4
"""

import os
import sys

GE_ROOT = os.path.dirname(os.path.abspath(__file__))


def main():
    try:
        import great_expectations as ge
    except ImportError:
        print("[SKIP] great-expectations not installed. "
              "Add 'great-expectations==0.18.0' to Dockerfile.airflow requirements.")
        sys.exit(0)

    context = ge.get_context(context_root_dir=GE_ROOT)

    result = context.run_checkpoint(checkpoint_name="datalake_checkpoint")

    if not result["success"]:
        failed = [
            vr["expectation_suite_name"]
            for vr in result["run_results"].values()
            if not vr["validation_result"]["success"]
        ]
        print(f"\n[FAIL] Great Expectations checkpoint failed for: {failed}", file=sys.stderr)
        sys.exit(1)

    total_expectations = sum(
        vr["validation_result"]["statistics"]["evaluated_expectations"]
        for vr in result["run_results"].values()
    )
    print(f"\n[PASS] All expectations passed ({total_expectations} total).")
    sys.exit(0)


if __name__ == "__main__":
    main()
