#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="${1:-config/tables.yml}"

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Config not found: $CONFIG_PATH" >&2
  exit 1
fi

python3 -c "import hashlib; from pathlib import Path; p=Path('$CONFIG_PATH'); print(hashlib.sha256(p.read_bytes()).hexdigest())"

