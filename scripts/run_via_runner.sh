#!/usr/bin/env bash
set -euo pipefail

SCOPE_JSON='{"domains":["customer","sales"],"tables":["customers","orders"]}'

curl -sS -X POST "http://localhost:8080/runs" \
  -H "Content-Type: application/json" \
  -d "{\"scope\": $SCOPE_JSON}"

echo

