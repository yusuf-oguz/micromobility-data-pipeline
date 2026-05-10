#!/bin/sh
# NiFi otomatik kurulum: REST API ile processor'ları oluşturur.
# Template import yerine direkt API çağrıları kullanılır (NiFi 1.25.0 uyumlu).

NIFI_URL="http://nifi:8080/nifi-api"

echo "[nifi-setup] NiFi bekleniyor..."
until curl -sf "$NIFI_URL/flow/status" > /dev/null 2>&1; do
  sleep 10
done
echo "[nifi-setup] NiFi hazir."

# Root PG ID
ROOT_PG_ID=$(curl -sf "$NIFI_URL/flow/process-groups/root" \
  | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
echo "[nifi-setup] Root PG ID: $ROOT_PG_ID"

# Zaten processor var mı?
PROC_LIST=$(curl -sf "$NIFI_URL/process-groups/$ROOT_PG_ID/processors")
PROC_COUNT=$(echo "$PROC_LIST" | grep -o '"type":"org.apache.nifi' | wc -l)
echo "[nifi-setup] Mevcut processor sayisi: $PROC_COUNT"

if [ "$PROC_COUNT" -gt "0" ]; then
  echo "[nifi-setup] Flow zaten kurulu, processor'lar baslatiliyor..."
else
  echo "[nifi-setup] Processor'lar olusturuluyor..."

  # 1. TailFile
  TAIL_ID=$(curl -sf -X POST -H "Content-Type: application/json" \
    -d '{
      "revision":{"version":0},
      "component":{
        "type":"org.apache.nifi.processors.standard.TailFile",
        "name":"TailFile - scooter_stream.jsonl",
        "position":{"x":100,"y":200},
        "config":{
          "schedulingPeriod":"1 sec",
          "schedulingStrategy":"TIMER_DRIVEN",
          "properties":{
            "File to Tail":"/data/scooter_stream.jsonl",
            "tail-mode":"Single file",
            "Rolling Filename Pattern":"scooter_stream.jsonl.*",
            "Initial Start Position":"Beginning of File"
          }
        }
      }
    }' "$NIFI_URL/process-groups/$ROOT_PG_ID/processors" \
    | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
  echo "[nifi-setup] TailFile ID: $TAIL_ID"

  # 2. SplitText
  SPLIT_ID=$(curl -sf -X POST -H "Content-Type: application/json" \
    -d '{
      "revision":{"version":0},
      "component":{
        "type":"org.apache.nifi.processors.standard.SplitText",
        "name":"SplitText - one record per FlowFile",
        "position":{"x":400,"y":200},
        "config":{
          "schedulingStrategy":"TIMER_DRIVEN",
          "autoTerminatedRelationships":["original","failure"],
          "properties":{
            "Line Split Count":"1",
            "Remove Trailing Newlines":"true",
            "Header Line Count":"0"
          }
        }
      }
    }' "$NIFI_URL/process-groups/$ROOT_PG_ID/processors" \
    | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
  echo "[nifi-setup] SplitText ID: $SPLIT_ID"

  # 3. EvaluateJsonPath
  EVAL_ID=$(curl -sf -X POST -H "Content-Type: application/json" \
    -d '{
      "revision":{"version":0},
      "component":{
        "type":"org.apache.nifi.processors.standard.EvaluateJsonPath",
        "name":"EvaluateJsonPath - extract routing attrs",
        "position":{"x":700,"y":200},
        "config":{
          "schedulingStrategy":"TIMER_DRIVEN",
          "autoTerminatedRelationships":["unmatched","failure"],
          "properties":{
            "Destination":"flowfile-attribute",
            "Return Type":"auto-detect",
            "Path Not Found Behavior":"warn",
            "anomaly_type":"$.anomaly_type",
            "fault_code":"$.fault_code",
            "event_id":"$.event_id"
          }
        }
      }
    }' "$NIFI_URL/process-groups/$ROOT_PG_ID/processors" \
    | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
  echo "[nifi-setup] EvaluateJsonPath ID: $EVAL_ID"

  # 4. RouteOnAttribute
  ROUTE_ID=$(curl -sf -X POST -H "Content-Type: application/json" \
    -d '{
      "revision":{"version":0},
      "component":{
        "type":"org.apache.nifi.processors.standard.RouteOnAttribute",
        "name":"RouteOnAttribute - elastic vs postgres",
        "position":{"x":1000,"y":200},
        "config":{
          "schedulingStrategy":"TIMER_DRIVEN",
          "autoTerminatedRelationships":["unmatched"],
          "properties":{
            "Routing Strategy":"Route to Property name",
            "to_elastic":"${anomaly_type:isEmpty():not()}"
          }
        }
      }
    }' "$NIFI_URL/process-groups/$ROOT_PG_ID/processors" \
    | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
  echo "[nifi-setup] RouteOnAttribute ID: $ROUTE_ID"

  # 5. InvokeHTTP → Elasticsearch
  ES_ID=$(curl -sf -X POST -H "Content-Type: application/json" \
    -d '{
      "revision":{"version":0},
      "component":{
        "type":"org.apache.nifi.processors.standard.InvokeHTTP",
        "name":"InvokeHTTP - POST to Elasticsearch",
        "position":{"x":1300,"y":0},
        "config":{
          "schedulingStrategy":"TIMER_DRIVEN",
          "autoTerminatedRelationships":["Response","Failure","No Retry","Original","Retry"],
          "properties":{
            "HTTP Method":"POST",
            "Remote URL":"http://elasticsearch:9200/scooter-alerts/_doc",
            "Content-Type":"application/json",
            "Basic Authentication Username":"elastic",
            "Basic Authentication Password":"admin12345",
            "Connection Timeout":"5 secs",
            "Read Timeout":"15 secs"
          }
        }
      }
    }' "$NIFI_URL/process-groups/$ROOT_PG_ID/processors" \
    | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
  echo "[nifi-setup] InvokeHTTP (ES) ID: $ES_ID"

  # Bağlantıları kur
  echo "[nifi-setup] Baglantılar kuruluyor..."

  # TailFile success → SplitText
  curl -sf -X POST -H "Content-Type: application/json" \
    -d "{\"revision\":{\"version\":0},\"component\":{\"source\":{\"id\":\"$TAIL_ID\",\"groupId\":\"$ROOT_PG_ID\",\"type\":\"PROCESSOR\"},\"destination\":{\"id\":\"$SPLIT_ID\",\"groupId\":\"$ROOT_PG_ID\",\"type\":\"PROCESSOR\"},\"selectedRelationships\":[\"success\"],\"backPressureObjectThreshold\":\"10000\",\"backPressureDataSizeThreshold\":\"1 GB\"}}" \
    "$NIFI_URL/process-groups/$ROOT_PG_ID/connections" > /dev/null
  echo "[nifi-setup] TailFile -> SplitText baglandi."

  # SplitText splits → EvaluateJsonPath
  curl -sf -X POST -H "Content-Type: application/json" \
    -d "{\"revision\":{\"version\":0},\"component\":{\"source\":{\"id\":\"$SPLIT_ID\",\"groupId\":\"$ROOT_PG_ID\",\"type\":\"PROCESSOR\"},\"destination\":{\"id\":\"$EVAL_ID\",\"groupId\":\"$ROOT_PG_ID\",\"type\":\"PROCESSOR\"},\"selectedRelationships\":[\"splits\"],\"backPressureObjectThreshold\":\"10000\",\"backPressureDataSizeThreshold\":\"1 GB\"}}" \
    "$NIFI_URL/process-groups/$ROOT_PG_ID/connections" > /dev/null
  echo "[nifi-setup] SplitText -> EvaluateJsonPath baglandi."

  # EvaluateJsonPath matched → RouteOnAttribute
  curl -sf -X POST -H "Content-Type: application/json" \
    -d "{\"revision\":{\"version\":0},\"component\":{\"source\":{\"id\":\"$EVAL_ID\",\"groupId\":\"$ROOT_PG_ID\",\"type\":\"PROCESSOR\"},\"destination\":{\"id\":\"$ROUTE_ID\",\"groupId\":\"$ROOT_PG_ID\",\"type\":\"PROCESSOR\"},\"selectedRelationships\":[\"matched\"],\"backPressureObjectThreshold\":\"10000\",\"backPressureDataSizeThreshold\":\"1 GB\"}}" \
    "$NIFI_URL/process-groups/$ROOT_PG_ID/connections" > /dev/null
  echo "[nifi-setup] EvaluateJsonPath -> RouteOnAttribute baglandi."

  # RouteOnAttribute to_elastic → InvokeHTTP
  curl -sf -X POST -H "Content-Type: application/json" \
    -d "{\"revision\":{\"version\":0},\"component\":{\"source\":{\"id\":\"$ROUTE_ID\",\"groupId\":\"$ROOT_PG_ID\",\"type\":\"PROCESSOR\"},\"destination\":{\"id\":\"$ES_ID\",\"groupId\":\"$ROOT_PG_ID\",\"type\":\"PROCESSOR\"},\"selectedRelationships\":[\"to_elastic\"],\"backPressureObjectThreshold\":\"10000\",\"backPressureDataSizeThreshold\":\"1 GB\"}}" \
    "$NIFI_URL/process-groups/$ROOT_PG_ID/connections" > /dev/null
  echo "[nifi-setup] RouteOnAttribute -> InvokeHTTP (ES) baglandi."

  echo "[nifi-setup] Tum processor'lar ve baglantılar olusturuldu."
fi

# Tüm flow'u başlat
echo "[nifi-setup] Flow baslatiliyor..."
curl -sf -X PUT -H "Content-Type: application/json" \
  -d "{\"id\":\"$ROOT_PG_ID\",\"state\":\"RUNNING\"}" \
  "$NIFI_URL/flow/process-groups/$ROOT_PG_ID" > /dev/null

echo "[nifi-setup] Tamamlandi."
