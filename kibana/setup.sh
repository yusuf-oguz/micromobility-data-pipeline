#!/bin/sh
# Kibana başlayana kadar bekle, sonra dashboard'u import et.

KIBANA_URL="http://kibana:5601"
ELASTIC_USER="elastic"
ELASTIC_PASS="${ELASTIC_PASSWORD:-admin12345}"

echo "[kibana-setup] Kibana bekleniyor..."
until curl -sf -u "$ELASTIC_USER:$ELASTIC_PASS" "$KIBANA_URL/api/status" \
  | grep -q '"level":"available"'; do
  sleep 10
done
echo "[kibana-setup] Kibana hazir."

echo "[kibana-setup] Dashboard import ediliyor..."
curl -sf -X POST \
  -u "$ELASTIC_USER:$ELASTIC_PASS" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  --form file=@/kibana/dashboard.ndjson \
  "$KIBANA_URL/api/saved_objects/_import?overwrite=true"
echo ""
echo "[kibana-setup] Tamamlandi."
