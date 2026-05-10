#!/bin/sh
# Waits for Elasticsearch to be ready then creates the scooter-alerts index with geo_point mapping.
set -e

ES_URL="http://elasticsearch:9200"
ES_USER="elastic"
ES_PASS="${ELASTIC_PASSWORD:-admin12345}"

echo "[es-setup] Waiting for Elasticsearch..."
until curl -s -u "$ES_USER:$ES_PASS" "$ES_URL/_cluster/health" | grep -q '"status":"green"\|"status":"yellow"'; do
  sleep 5
done
echo "[es-setup] Elasticsearch is ready."

# Create scooter-alerts index
curl -s -u "$ES_USER:$ES_PASS" -X PUT "$ES_URL/scooter-alerts" \
  -H "Content-Type: application/json" \
  -d @/setup/mapping_alerts.json
echo ""
echo "[es-setup] scooter-alerts index created."

# Create scooter-telemetry index (for optional full ingestion)
curl -s -u "$ES_USER:$ES_PASS" -X PUT "$ES_URL/scooter-telemetry" \
  -H "Content-Type: application/json" \
  -d @/setup/mapping_telemetry.json
echo ""
echo "[es-setup] scooter-telemetry index created."

# Set kibana_system password so Kibana can connect
curl -s -u "$ES_USER:$ES_PASS" -X POST "$ES_URL/_security/user/kibana_system/_password" \
  -H "Content-Type: application/json" \
  -d "{\"password\":\"$ES_PASS\"}"
echo ""
echo "[es-setup] kibana_system password set."

echo "[es-setup] Done."
