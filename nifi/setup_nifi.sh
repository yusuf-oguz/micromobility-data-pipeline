#!/bin/sh
# NiFi otomatik kurulum scripti.
# NiFi hazır olana kadar bekler, token alır, template import eder,
# canvas'a yerleştirir ve tüm processor'ları başlatır.
set -e

NIFI_URL="http://nifi:8080/nifi-api"
NIFI_USER="admin"
NIFI_PASS="adminadminadmin"
TEMPLATE_FILE="/templates/scooter_flow.xml"

echo "[nifi-setup] NiFi bekleniyor..."
until curl -sf "$NIFI_URL/flow/status" > /dev/null 2>&1; do
  sleep 10
done
echo "[nifi-setup] NiFi hazir."

# Token al
TOKEN=$(curl -sf \
  -X POST \
  -d "username=$NIFI_USER&password=$NIFI_PASS" \
  "$NIFI_URL/access/token")

if [ -z "$TOKEN" ]; then
  # Single-user auth kapalıysa (HTTP mod) token gerekmeyebilir
  AUTH_HEADER=""
  echo "[nifi-setup] Token alinamadi, anonim mod deneniyor."
else
  AUTH_HEADER="Authorization: Bearer $TOKEN"
  echo "[nifi-setup] Token alindi."
fi

call() {
  if [ -n "$AUTH_HEADER" ]; then
    curl -sf -H "$AUTH_HEADER" "$@"
  else
    curl -sf "$@"
  fi
}

# Root process group ID'sini al
ROOT_PG_ID=$(call "$NIFI_URL/flow/process-groups/root" \
  | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
echo "[nifi-setup] Root PG ID: $ROOT_PG_ID"

# Template zaten var mı kontrol et
EXISTING=$(call "$NIFI_URL/flow/templates" \
  | grep -c 'Scooter Pipeline' || true)

if [ "$EXISTING" -gt "0" ]; then
  echo "[nifi-setup] Template zaten mevcut, atlanıyor."
else
  # Template'i upload et
  if [ -n "$AUTH_HEADER" ]; then
    UPLOAD_RESP=$(curl -sf \
      -H "$AUTH_HEADER" \
      -X POST \
      -F "template=@$TEMPLATE_FILE" \
      "$NIFI_URL/process-groups/$ROOT_PG_ID/templates/upload")
  else
    UPLOAD_RESP=$(curl -sf \
      -X POST \
      -F "template=@$TEMPLATE_FILE" \
      "$NIFI_URL/process-groups/$ROOT_PG_ID/templates/upload")
  fi

  TEMPLATE_ID=$(echo "$UPLOAD_RESP" \
    | grep -o '<id>[^<]*</id>' | head -1 | sed 's/<[^>]*>//g')
  echo "[nifi-setup] Template yuklendi, ID: $TEMPLATE_ID"

  # Template'i canvas'a yerleştir
  call \
    -X POST \
    -H "Content-Type: application/json" \
    -d "{\"templateId\":\"$TEMPLATE_ID\",\"originX\":100,\"originY\":100}" \
    "$NIFI_URL/process-groups/$ROOT_PG_ID/template-instance" > /dev/null
  echo "[nifi-setup] Template instantiate edildi."
fi

# Tüm processor'ları başlat
call \
  -X PUT \
  -H "Content-Type: application/json" \
  -d "{\"id\":\"$ROOT_PG_ID\",\"state\":\"RUNNING\"}" \
  "$NIFI_URL/flow/process-groups/$ROOT_PG_ID" > /dev/null

echo "[nifi-setup] Tum processor'lar baslatildi."
echo "[nifi-setup] Tamamlandi."
