"""Simulated web/app events: send to NiFi ListenHTTP or directly to ClickHouse HTTP."""

from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import httpx

EVENT_NAMES = ("page_view", "product_view", "add_to_cart", "purchase")
PRODUCT_IDS = ("SKU-1", "SKU-2", "SKU-3", "SKU-4")


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def build_event() -> dict:
    name = random.choice(EVENT_NAMES)
    user_id = f"u{random.randint(1, 500)}"
    session_id = f"s{random.randint(1, 2000)}"
    props: dict = {"page": random.choice(("/", "/products", "/cart", "/checkout"))}
    if name in ("product_view", "add_to_cart", "purchase"):
        props["product_id"] = random.choice(PRODUCT_IDS)
    if name == "purchase":
        props["revenue"] = round(random.uniform(5.0, 250.0), 2)
    return {
        "event_id": str(uuid.uuid4()),
        "event_ts": _now_iso(),
        "event_name": name,
        "user_id": user_id,
        "session_id": session_id,
        "properties": json.dumps(props),
        "ingest_ts": _now_iso(),
        "ingest_source": os.environ.get("INGEST_SOURCE", "event_sim"),
        "_raw": None,
    }


def send_to_clickhouse(client: httpx.Client, base: str, row: dict) -> None:
    q = (
        "INSERT INTO bronze.events "
        "(event_id, event_ts, event_name, user_id, session_id, properties, ingest_ts, ingest_source, _raw) "
        "FORMAT JSONEachRow"
    )
    payload = {
        "event_id": row["event_id"],
        "event_ts": row["event_ts"],
        "event_name": row["event_name"],
        "user_id": row["user_id"],
        "session_id": row["session_id"],
        "properties": row["properties"],
        "ingest_ts": row["ingest_ts"],
        "ingest_source": row["ingest_source"],
        "_raw": row["_raw"],
    }
    r = client.post(
        f"{base.rstrip('/')}/",
        params={"query": q},
        content=json.dumps(payload) + "\n",
        timeout=30.0,
    )
    r.raise_for_status()


def send_to_nifi(client: httpx.Client, url: str, body: dict) -> None:
    r = client.post(url, json=body, timeout=30.0)
    r.raise_for_status()


def main() -> None:
    target = os.environ.get("EVENT_TARGET", "nifi").lower()
    interval = float(os.environ.get("EVENT_INTERVAL_SEC", "3"))
    ch_url = os.environ.get("CLICKHOUSE_URL", "http://clickhouse:8123")
    nifi_url = os.environ.get("NIFI_INGEST_URL", "http://nifi:8081/contentListener")

    with httpx.Client() as client:
        while True:
            row = build_event()
            try:
                if target == "clickhouse":
                    send_to_clickhouse(client, ch_url, row)
                else:
                    send_to_nifi(
                        client,
                        nifi_url,
                        {
                            "event_id": row["event_id"],
                            "event_ts": row["event_ts"],
                            "event_name": row["event_name"],
                            "user_id": row["user_id"],
                            "session_id": row["session_id"],
                            "properties": row["properties"],
                        },
                    )
            except Exception as exc:  # noqa: BLE001
                print(f"send failed: {exc!r}")
            time.sleep(interval)


if __name__ == "__main__":
    main()
