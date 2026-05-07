"""
ClickHouse query load test — validates query performance under concurrent load.

Run: python tests/load/test_clickhouse_load.py
Requires: pip install clickhouse-connect
"""

import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import clickhouse_connect

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_DEFAULT_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_DEFAULT_PASSWORD")

QUERIES = [
    ("SELECT count() FROM bronze.src_mysql_orders", "order count"),
    ("SELECT count() FROM bronze.src_mysql_customers", "customer count"),
    ("SELECT segment, count() FROM bronze.src_mysql_customers GROUP BY segment", "segment breakdown"),
    ("SELECT status, count() FROM bronze.src_mysql_orders GROUP BY status", "order status breakdown"),
    ("SELECT toStartOfMonth(order_date) AS month, sum(amount) AS revenue FROM bronze.src_mysql_orders GROUP BY month ORDER BY month", "monthly revenue"),
    ("SELECT customer_id, sum(amount) AS total FROM bronze.src_mysql_orders GROUP BY customer_id ORDER BY total DESC LIMIT 10", "top 10 customers"),
]


def run_query(client, sql, label, results):
    start = time.time()
    try:
        client.command(sql)
        elapsed = time.time() - start
        results.append({"label": label, "status": "ok", "duration": elapsed})
    except Exception as e:
        elapsed = time.time() - start
        results.append({"label": label, "status": "error", "duration": elapsed, "error": str(e)})


def main(concurrency=10, iterations=5):
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD,
    )

    print(f"ClickHouse Load Test: {concurrency} concurrent users, {iterations} iterations each")
    print(f"Target: {CH_HOST}:{CH_PORT}")
    print()

    all_results = []

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        for i in range(concurrency):
            thread_client = clickhouse_connect.get_client(
                host=CH_HOST, port=CH_PORT,
                username=CH_USER, password=CH_PASSWORD,
            )
            for j in range(iterations):
                sql, label = random.choice(QUERIES)
                futures.append(
                    executor.submit(run_query, thread_client, sql, label, all_results)
                )

        for f in as_completed(futures):
            f.result()

    # Summary
    ok = [r for r in all_results if r["status"] == "ok"]
    errors = [r for r in all_results if r["status"] == "error"]
    durations = [r["duration"] for r in ok]

    print(f"Total queries: {len(all_results)}")
    print(f"Successful:    {len(ok)}")
    print(f"Errors:        {len(errors)}")
    if durations:
        print(f"Min latency:   {min(durations)*1000:.0f}ms")
        print(f"Max latency:   {max(durations)*1000:.0f}ms")
        print(f"Median:        {sorted(durations)[len(durations)//2]*1000:.0f}ms")
        print(f"95th pctl:     {sorted(durations)[int(len(durations)*0.95)]*1000:.0f}ms")
        print(f"Throughput:    {len(ok)/sum(durations):.1f} queries/sec")


if __name__ == "__main__":
    import random
    import sys
    concurrency = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    iterations = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    main(concurrency, iterations)
