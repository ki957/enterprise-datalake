"""
Load tests for the Enterprise Data Lake AI Agent API.

Uses locust for concurrent user simulation.
Run: locust -f tests/load/test_ai_agent_load.py --headless -u 10 -r 2 --run-time 30s

Requires: pip install locust
"""

import os
import random
import time
from locust import HttpUser, task, between

AGENTS = ["auto", "insight", "schema", "quality", "orchestration", "performance",
          "airbyte", "ingestion", "self_healing", "anomaly", "nl_dbt", "contract"]

SAMPLE_QUERIES = [
    "What is the total revenue by month?",
    "Show me the top 10 customers by order count",
    "How many active subscriptions do we have?",
    "What is the daily active user trend?",
    "List all failed DAG runs from last week",
    "Show me the data quality status for gold tables",
    "What is the average order value by segment?",
    "How many products are in each category?",
    "Show me customer churn rate by month",
    "What are the slowest queries in ClickHouse?",
]


class AIAgentUser(HttpUser):
    wait_time = between(1, 3)
    host = os.getenv("AI_AGENT_HOST", "http://localhost:8502")

    def on_start(self):
        self.session_id = f"load-test-{int(time.time())}-{random.randint(1000, 9999)}"

    @task(3)
    def chat_query(self):
        agent = random.choice(AGENTS)
        message = random.choice(SAMPLE_QUERIES)
        self.client.post(
            "/api/chat",
            json={
                "message": message,
                "session_id": self.session_id,
                "agent": agent,
                "history": [],
            },
            name="/api/chat",
        )

    @task(2)
    def health_check(self):
        self.client.get("/api/health", name="/api/health")

    @task(1)
    def get_costs(self):
        self.client.get("/api/costs/summary?days=7", name="/api/costs/summary")

    @task(1)
    def get_metrics(self):
        self.client.get("/metrics", name="/metrics")
