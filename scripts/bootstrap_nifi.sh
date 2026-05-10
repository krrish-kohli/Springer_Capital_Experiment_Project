#!/usr/bin/env bash
# Provision NiFi ListenHTTP -> InvokeHTTP -> ClickHouse bronze.events (Medallion demo).
# Run on the host after: docker compose up -d
# Requires: curl, jq, python3
set -euo pipefail

NIFI_BASE="${NIFI_BASE:-https://localhost:8443}"
NIFI_USER="${NIFI_USER:-admin}"
NIFI_PASS="${NIFI_PASS:-adminadminadmin}"
CLICKHOUSE_HTTP_URL="${CLICKHOUSE_HTTP_URL:-http://clickhouse:8123}"

LISTEN_NAME="${NIFI_LISTEN_PROCESSOR_NAME:-Medallion_ListenHTTP}"
INVOKE_NAME="${NIFI_INVOKE_PROCESSOR_NAME:-Medallion_InvokeHTTP_CH}"
LISTEN_PORT="${NIFI_LISTEN_PORT:-8081}"
BASE_PATH="${NIFI_BASE_PATH:-contentListener}"

for cmd in curl jq python3; do
  command -v "$cmd" >/dev/null || {
    echo "error: missing required command: $cmd" >&2
    exit 1
  }
done

REMOTE_URL="${CLICKHOUSE_HTTP_URL%/}/?query=$(python3 <<'PY'
import urllib.parse

q = (
    "INSERT INTO bronze.events (event_id, event_ts, event_name, user_id, session_id, "
    "properties, ingest_ts, ingest_source, _raw) FORMAT JSONEachRow"
)
print(urllib.parse.quote(q, safe=""), end="")
PY
)"

nifi_token() {
  curl -sk -X POST "${NIFI_BASE%/}/nifi-api/access/token" \
    -d "username=${NIFI_USER}&password=${NIFI_PASS}" \
    -H "Content-Type: application/x-www-form-urlencoded"
}

api_get() {
  local path=$1
  curl -sk -H "Authorization: Bearer ${TOKEN}" "${NIFI_BASE%/}${path}"
}

api_post_json() {
  local path=$1
  local body=$2
  curl -sk -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" \
    -X POST "${NIFI_BASE%/}${path}" -d "$body"
}

api_put_json() {
  local path=$1
  local body=$2
  curl -sk -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" \
    -X PUT "${NIFI_BASE%/}${path}" -d "$body"
}

api_delete() {
  local path=$1
  curl -sk -H "Authorization: Bearer ${TOKEN}" -X DELETE "${NIFI_BASE%/}${path}"
}

wait_for_nifi() {
  local i t
  for i in $(seq 1 90); do
    t="$(nifi_token)"
    if [[ ${#t} -gt 200 ]]; then
      return 0
    fi
    sleep 2
  done
  echo "error: NiFi did not become ready for API access within timeout" >&2
  exit 1
}

stop_processor() {
  local pid=$1
  local doc ver body
  doc=$(api_get "/nifi-api/processors/${pid}")
  ver=$(echo "$doc" | jq '.revision.version')
  body=$(jq -n --argjson v "$ver" '{revision:{version:$v}, state:"STOPPED", disconnectedNodeAcknowledged:false}')
  api_put_json "/nifi-api/processors/${pid}/run-status" "$body" >/dev/null
}

delete_processor() {
  local pid=$1
  local ver
  ver=$(api_get "/nifi-api/processors/${pid}" | jq '.revision.version')
  api_delete "/nifi-api/processors/${pid}?version=${ver}&disconnectedNodeAcknowledged=false" >/dev/null
}

delete_connections_touching() {
  local root=$1
  local pid=$2
  local list cid ver
  list=$(api_get "/nifi-api/process-groups/${root}/connections")
  echo "$list" | jq -c --arg pid "$pid" '.connections[]? | select(.component.source.id == $pid or .component.destination.id == $pid)' |
    while read -r row; do
      [[ -z "$row" ]] && continue
      cid=$(echo "$row" | jq -r '.id')
      ver=$(echo "$row" | jq '.revision.version')
      api_delete "/nifi-api/connections/${cid}?version=${ver}&disconnectedNodeAcknowledged=false" >/dev/null || true
    done
}

find_processor_id_by_name() {
  local root=$1
  local name=$2
  api_get "/nifi-api/process-groups/${root}/processors" |
    jq -r --arg n "$name" '.processors[]? | select(.component.name == $n) | .id' | head -1
}

# Only one ListenHTTP may bind LISTEN_PORT; remove other ListenHTTP processors using the same port.
cleanup_conflicting_listen_http() {
  local root=$1
  local port=$2
  local keep=$3
  local ids
  ids=$(api_get "/nifi-api/process-groups/${root}/processors" |
    jq -r --arg port "$port" --arg keep "$keep" '
      .processors[]?
      | select(.component.type == "org.apache.nifi.processors.standard.ListenHTTP")
      | select(.component.config.properties["Listening Port"] == $port)
      | select(.component.name != $keep)
      | .id')
  local pid
  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    echo "Removing conflicting ListenHTTP processor ${pid} (port ${port})."
    stop_processor "$pid" || true
    delete_connections_touching "$root" "$pid"
    delete_processor "$pid"
  done <<<"$ids"
}

echo "Waiting for NiFi API (${NIFI_BASE})..."
wait_for_nifi
TOKEN="$(nifi_token)"
echo "NiFi API token acquired."

ROOT_ID=$(api_get "/nifi-api/process-groups/root" | jq -r '.id')

LISTEN_ID=$(find_processor_id_by_name "$ROOT_ID" "$LISTEN_NAME")
INVOKE_ID=$(find_processor_id_by_name "$ROOT_ID" "$INVOKE_NAME")

if [[ -n "$LISTEN_ID" && -z "$INVOKE_ID" ]]; then
  echo "Removing partial ${LISTEN_NAME} (no matching invoke processor)."
  stop_processor "$LISTEN_ID" || true
  delete_connections_touching "$ROOT_ID" "$LISTEN_ID"
  delete_processor "$LISTEN_ID"
  LISTEN_ID=""
fi
if [[ -z "$LISTEN_ID" && -n "$INVOKE_ID" ]]; then
  echo "Removing partial ${INVOKE_NAME} (no matching listen processor)."
  stop_processor "$INVOKE_ID" || true
  delete_connections_touching "$ROOT_ID" "$INVOKE_ID"
  delete_processor "$INVOKE_ID"
  INVOKE_ID=""
fi

if [[ -n "$LISTEN_ID" && -n "$INVOKE_ID" ]]; then
  echo "Medallion NiFi processors already present; ensuring RUNNING."
  stop_processor "$LISTEN_ID" || true
  stop_processor "$INVOKE_ID" || true
  # Refresh revisions after stop
  RV=$(api_get "/nifi-api/process-groups/root" | jq '.revision.version')
  HAS_CONN=$(api_get "/nifi-api/process-groups/${ROOT_ID}/connections" |
    jq --arg a "$LISTEN_ID" --arg b "$INVOKE_ID" '[.connections[]? | select(.component.source.id == $a and .component.destination.id == $b and (.component.selectedRelationships[]? == "success"))] | length')
  if [[ "$HAS_CONN" -eq 0 ]]; then
    echo "Creating missing connection ${LISTEN_NAME} -> ${INVOKE_NAME}."
    RV=$(api_get "/nifi-api/process-groups/root" | jq '.revision.version')
    body=$(jq -n \
      --argjson rv "$RV" \
      --arg root "$ROOT_ID" \
      --arg src "$LISTEN_ID" \
      --arg dst "$INVOKE_ID" \
      '{revision:{version:$rv}, component:{source:{id:$src, type:"PROCESSOR", groupId:$root}, destination:{id:$dst, type:"PROCESSOR", groupId:$root}, selectedRelationships:["success"]}}')
    api_post_json "/nifi-api/process-groups/${ROOT_ID}/connections" "$body" >/dev/null
  fi
  LV=$(api_get "/nifi-api/processors/${LISTEN_ID}" | jq '.revision.version')
  IV=$(api_get "/nifi-api/processors/${INVOKE_ID}" | jq '.revision.version')
  api_put_json "/nifi-api/processors/${LISTEN_ID}/run-status" "$(jq -n --argjson v "$LV" '{revision:{version:$v}, state:"RUNNING", disconnectedNodeAcknowledged:false}')" >/dev/null
  api_put_json "/nifi-api/processors/${INVOKE_ID}/run-status" "$(jq -n --argjson v "$IV" '{revision:{version:$v}, state:"RUNNING", disconnectedNodeAcknowledged:false}')" >/dev/null
  echo "NiFi medallion flow is RUNNING."
  exit 0
fi

echo "Creating ${LISTEN_NAME} and ${INVOKE_NAME}..."
cleanup_conflicting_listen_http "$ROOT_ID" "$LISTEN_PORT" "$LISTEN_NAME"

RV=$(api_get "/nifi-api/process-groups/root" | jq '.revision.version')
listen_body=$(jq -n \
  --argjson rv "$RV" \
  --arg name "$LISTEN_NAME" \
  --arg port "$LISTEN_PORT" \
  --arg base "$BASE_PATH" \
  '{
    revision:{version:$rv},
    component:{
      type:"org.apache.nifi.processors.standard.ListenHTTP",
      bundle:{group:"org.apache.nifi", artifact:"nifi-standard-nar", version:"1.26.0"},
      name:$name,
      position:{x:100, y:100},
      config:{
        concurrentlySchedulableTaskCount:"1",
        autoTerminatedRelationships:[],
        properties:{
          "Listening Port":$port,
          "Base Path":$base
        }
      }
    }
  }')
LISTEN_RESP=$(api_post_json "/nifi-api/process-groups/${ROOT_ID}/processors" "$listen_body")
LISTEN_ID=$(echo "$LISTEN_RESP" | jq -r '.id')
if [[ -z "$LISTEN_ID" || "$LISTEN_ID" == "null" ]]; then
  echo "error: failed to create ListenHTTP: $LISTEN_RESP" >&2
  exit 1
fi

RV=$(api_get "/nifi-api/process-groups/root" | jq '.revision.version')
invoke_body=$(jq -n \
  --argjson rv "$RV" \
  --arg name "$INVOKE_NAME" \
  --arg url "$REMOTE_URL" \
  '{
    revision:{version:$rv},
    component:{
      type:"org.apache.nifi.processors.standard.InvokeHTTP",
      bundle:{group:"org.apache.nifi", artifact:"nifi-standard-nar", version:"1.26.0"},
      name:$name,
      position:{x:400, y:100},
      config:{
        concurrentlySchedulableTaskCount:"1",
        autoTerminatedRelationships:["Original","Retry","No Retry","Failure","Response"],
        properties:{
          "HTTP Method":"POST",
          "Remote URL":$url,
          "Content-Type":"application/json",
          "Attributes to Send":"^$",
          "send-message-body":"true"
        }
      }
    }
  }')
INVOKE_RESP=$(api_post_json "/nifi-api/process-groups/${ROOT_ID}/processors" "$invoke_body")
INVOKE_ID=$(echo "$INVOKE_RESP" | jq -r '.id')
if [[ -z "$INVOKE_ID" || "$INVOKE_ID" == "null" ]]; then
  echo "error: failed to create InvokeHTTP: $INVOKE_RESP" >&2
  exit 1
fi

RV=$(api_get "/nifi-api/process-groups/root" | jq '.revision.version')
conn_body=$(jq -n \
  --argjson rv "$RV" \
  --arg root "$ROOT_ID" \
  --arg src "$LISTEN_ID" \
  --arg dst "$INVOKE_ID" \
  '{revision:{version:$rv}, component:{source:{id:$src, type:"PROCESSOR", groupId:$root}, destination:{id:$dst, type:"PROCESSOR", groupId:$root}, selectedRelationships:["success"]}}')
api_post_json "/nifi-api/process-groups/${ROOT_ID}/connections" "$conn_body" >/dev/null

LV=$(api_get "/nifi-api/processors/${LISTEN_ID}" | jq '.revision.version')
IV=$(api_get "/nifi-api/processors/${INVOKE_ID}" | jq '.revision.version')
api_put_json "/nifi-api/processors/${LISTEN_ID}/run-status" "$(jq -n --argjson v "$LV" '{revision:{version:$v}, state:"RUNNING", disconnectedNodeAcknowledged:false}')" >/dev/null
api_put_json "/nifi-api/processors/${INVOKE_ID}/run-status" "$(jq -n --argjson v "$IV" '{revision:{version:$v}, state:"RUNNING", disconnectedNodeAcknowledged:false}')" >/dev/null

echo "NiFi medallion flow created and started (${LISTEN_NAME} -> ${INVOKE_NAME} -> ClickHouse)."
