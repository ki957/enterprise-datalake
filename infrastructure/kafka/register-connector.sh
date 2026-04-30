#!/usr/bin/env bash
# Idempotent Debezium connector registration.
# Uses PUT (upsert semantics) so it's safe to run multiple times.
# Run with: make setup-debezium

set -euo pipefail

CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
CONNECTOR_NAME="shopflow-mysql-connector"
CONFIG_FILE="$(dirname "$0")/debezium-connector-mysql.json"

# Inline the MySQL password from .env rather than using file-based secret
# (Kafka Connect file secrets require a mounted volume — simpler to inline for dev)
MYSQL_PASS="${MYSQL_ROOT_PASSWORD:-MySQL@2024}"

# Build config with password substituted
CONFIG=$(jq --arg pw "$MYSQL_PASS" \
  '.config["database.password"] = $pw' \
  "$CONFIG_FILE")

echo "Waiting for Kafka Connect to be ready..."
for i in $(seq 1 20); do
  if curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
    echo "Kafka Connect is ready."
    break
  fi
  echo "  attempt $i/20 — waiting 5s..."
  sleep 5
done

echo "Registering connector: ${CONNECTOR_NAME}"
HTTP_CODE=$(curl -s -o /tmp/connect_response.txt -w "%{http_code}" \
  -X PUT \
  -H "Content-Type: application/json" \
  --data "${CONFIG}" \
  "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config")

cat /tmp/connect_response.txt
echo ""

if [[ "$HTTP_CODE" -ge 200 && "$HTTP_CODE" -lt 300 ]]; then
  echo "✅ Connector registered (HTTP $HTTP_CODE)"
else
  echo "❌ Connector registration failed (HTTP $HTTP_CODE)"
  exit 1
fi
