from __future__ import annotations

import hashlib
import json
import os
import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import clickhouse_connect
import yaml
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


class Scope(BaseModel):
    domains: list[str] | None = None
    tables: list[str] | None = None


class RunRequest(BaseModel):
    run_id: str | None = None
    run_ts: str | None = None  # "YYYY-MM-DD HH:MM:SS"
    scope: Scope | None = None


class RunResponse(BaseModel):
    run_id: str
    status: Literal["success", "failed", "error"]
    failed_rules: int
    warn_rules: int
    exception_rows: int


@dataclass(frozen=True)
class RunnerConfig:
    repo_root: Path
    config_path: Path
    dbt_project_dir: Path
    dbt_profiles_dir: Path


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def _runner_config() -> RunnerConfig:
    repo_root = Path(_env("REPO_ROOT", "/repo")).resolve()
    config_path = Path(_env("TABLES_CONFIG_PATH", str(repo_root / "config" / "tables.yml"))).resolve()
    dbt_project_dir = Path(_env("DBT_PROJECT_DIR", str(repo_root / "dbt" / "silver_validation"))).resolve()
    dbt_profiles_dir = Path(_env("DBT_PROFILES_DIR", "/tmp/dbt_profiles")).resolve()
    return RunnerConfig(
        repo_root=repo_root,
        config_path=config_path,
        dbt_project_dir=dbt_project_dir,
        dbt_profiles_dir=dbt_profiles_dir,
    )


def _ensure_profiles(profiles_dir: Path) -> None:
    profiles_dir.mkdir(parents=True, exist_ok=True)
    profiles_yml = profiles_dir / "profiles.yml"
    # Keep this minimal and env-driven; no secrets committed.
    profiles_yml.write_text(
        "\n".join(
            [
                "silver_validation:",
                "  target: dev",
                "  outputs:",
                "    dev:",
                "      type: clickhouse",
                "      schema: default",
                "      host: \"{{ env_var('CLICKHOUSE_HOST', 'clickhouse') }}\"",
                "      port: 8123",
                "      user: \"{{ env_var('CLICKHOUSE_USER', 'default') }}\"",
                "      password: \"{{ env_var('CLICKHOUSE_PASSWORD', '') }}\"",
                "      secure: false",
                "      verify: false",
                "      database: \"{{ env_var('CLICKHOUSE_DATABASE', 'default') }}\"",
                "",
            ]
        )
    )


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _now_ts_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _parse_tables_config(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict) or "domains" not in data:
        raise ValueError("Invalid tables.yml (missing 'domains').")
    return data


def _filter_config(config: dict[str, Any], scope: Scope | None) -> dict[str, Any]:
    if scope is None or (not scope.domains and not scope.tables):
        return config

    want_domains = set(scope.domains or [])
    want_tables = set(scope.tables or [])

    out = {k: v for k, v in config.items() if k != "domains"}
    out_domains: list[dict[str, Any]] = []

    for dom in config.get("domains", []) or []:
        dom_name = dom.get("name")
        if want_domains and dom_name not in want_domains:
            continue
        tables = []
        for t in dom.get("tables", []) or []:
            t_name = t.get("name")
            if want_tables and t_name not in want_tables:
                continue
            tables.append(t)
        if tables:
            out_domains.append({**dom, "tables": tables})

    out["domains"] = out_domains
    return out


def _ch_client():
    return clickhouse_connect.get_client(
        host=_env("CLICKHOUSE_HOST", "clickhouse"),
        port=int(_env("CLICKHOUSE_PORT", "8123")),
        username=_env("CLICKHOUSE_USER", "default"),
        password=_env("CLICKHOUSE_PASSWORD", ""),
        database=_env("CLICKHOUSE_DATABASE", "default"),
    )


def _insert_run_start(ch, run_id: str, run_ts: str, config_hash: str, scope: dict[str, Any]) -> None:
    ch.command(
        """
        INSERT INTO audit.audit_run_summary
        (run_id, run_ts, run_end_ts, status, trigger_type, scope, dbt_job_id, dbt_invocation_id, total_rules, failed_rules, warn_rules, exception_rows, error_message, config_hash)
        VALUES
        ({run_id:UUID}, toDateTime({run_ts:String}), NULL, 'running', 'nifi', {scope:String}, NULL, NULL, 0, 0, 0, 0, NULL, {config_hash:String})
        """,
        parameters={
            "run_id": run_id,
            "run_ts": run_ts,
            "scope": json.dumps(scope, separators=(",", ":"), sort_keys=True),
            "config_hash": config_hash,
        },
    )


def _finalize_run(ch, run_id: str, status: str, error_message: str | None) -> RunResponse:
    def _scalar(q: str) -> int:
        res = ch.query(q, parameters={"run_id": run_id})
        if not res.result_rows:
            return 0
        return int(res.result_rows[0][0])

    # Pull counts from audit tables written by dbt models.
    failed_rules = _scalar("SELECT countIf(status IN ('fail','error')) FROM audit.audit_results WHERE run_id = {run_id:UUID}")
    warn_rules = _scalar("SELECT countIf(status = 'warn') FROM audit.audit_results WHERE run_id = {run_id:UUID}")
    exception_rows = _scalar("SELECT count() FROM audit.audit_exceptions WHERE run_id = {run_id:UUID}")
    total_rules = _scalar("SELECT count() FROM audit.audit_results WHERE run_id = {run_id:UUID}")

    ch.command(
        """
        ALTER TABLE audit.audit_run_summary
        UPDATE
          run_end_ts = now(),
          status = {status:String},
          total_rules = {total_rules:UInt32},
          failed_rules = {failed_rules:UInt32},
          warn_rules = {warn_rules:UInt32},
          exception_rows = {exception_rows:UInt64},
          error_message = {error_message:Nullable(String)}
        WHERE run_id = {run_id:UUID}
        """,
        parameters={
            "run_id": run_id,
            "status": status,
            "total_rules": total_rules,
            "failed_rules": failed_rules,
            "warn_rules": warn_rules,
            "exception_rows": exception_rows,
            "error_message": error_message,
        },
    )

    return RunResponse(
        run_id=run_id,
        status="success" if status == "success" else ("failed" if status == "failed" else "error"),
        failed_rules=failed_rules,
        warn_rules=warn_rules,
        exception_rows=exception_rows,
    )


def _run_dbt(dbt_project_dir: Path, profiles_dir: Path, vars_dict: dict[str, Any]) -> tuple[int, str]:
    env = os.environ.copy()
    env.setdefault("CLICKHOUSE_HOST", _env("CLICKHOUSE_HOST", "clickhouse"))
    env.setdefault("CLICKHOUSE_PORT", _env("CLICKHOUSE_PORT", "8123"))
    env.setdefault("CLICKHOUSE_USER", _env("CLICKHOUSE_USER", "default"))
    env.setdefault("CLICKHOUSE_PASSWORD", _env("CLICKHOUSE_PASSWORD", ""))
    env.setdefault("CLICKHOUSE_DATABASE", _env("CLICKHOUSE_DATABASE", "default"))

    vars_json = json.dumps(vars_dict, separators=(",", ":"), sort_keys=True)

    target_path = "/tmp/dbt_target"
    log_path = "/tmp/dbt_logs"

    # Step 1: materialize audit outputs (even if tests will fail)
    run_cmd = [
        "dbt",
        "run",
        "--profiles-dir",
        str(profiles_dir),
        "--project-dir",
        str(dbt_project_dir),
        "--target-path",
        target_path,
        "--log-path",
        log_path,
        "--select",
        "staging+ validation+ marts+",
        "--vars",
        vars_json,
    ]
    run_proc = subprocess.run(run_cmd, capture_output=True, text=True, env=env)
    if run_proc.returncode != 0:
        return run_proc.returncode, run_proc.stdout + "\n" + run_proc.stderr

    # Step 2: tests (may fail; runner should still finalize run summary)
    test_cmd = [
        "dbt",
        "test",
        "--profiles-dir",
        str(profiles_dir),
        "--project-dir",
        str(dbt_project_dir),
        "--target-path",
        target_path,
        "--log-path",
        log_path,
        "--vars",
        vars_json,
    ]
    test_proc = subprocess.run(test_cmd, capture_output=True, text=True, env=env)
    return test_proc.returncode, test_proc.stdout + "\n" + test_proc.stderr


app = FastAPI(title="silver-validation-runner", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/runs", response_model=RunResponse)
def run(req: RunRequest) -> RunResponse:
    cfg = _runner_config()
    if not cfg.config_path.exists():
        raise HTTPException(status_code=500, detail=f"Missing config: {cfg.config_path}")
    if not cfg.dbt_project_dir.exists():
        raise HTTPException(status_code=500, detail=f"Missing dbt project: {cfg.dbt_project_dir}")

    run_id = req.run_id or str(uuid.uuid4())
    try:
        uuid.UUID(run_id)
    except Exception:
        raise HTTPException(status_code=400, detail="run_id must be a UUID string")

    run_ts = req.run_ts or _now_ts_str()
    try:
        datetime.strptime(run_ts, "%Y-%m-%d %H:%M:%S")
    except Exception:
        raise HTTPException(status_code=400, detail="run_ts must be 'YYYY-MM-DD HH:MM:SS'")

    full_cfg = _parse_tables_config(cfg.config_path)
    filtered_cfg = _filter_config(full_cfg, req.scope)
    config_hash = _sha256_file(cfg.config_path)

    tables_config_json = json.dumps(filtered_cfg, separators=(",", ":"), sort_keys=True)

    _ensure_profiles(cfg.dbt_profiles_dir)

    ch = _ch_client()
    _insert_run_start(ch, run_id=run_id, run_ts=run_ts, config_hash=config_hash, scope={"scope": (req.scope.model_dump() if req.scope else None)})

    vars_dict = {
        "run_id": run_id,
        "run_ts": run_ts,
        "config_hash": config_hash,
        "tables_config_json": tables_config_json,
    }

    exit_code, output = _run_dbt(cfg.dbt_project_dir, cfg.dbt_profiles_dir, vars_dict)

    if exit_code == 0:
        return _finalize_run(ch, run_id, status="success", error_message=None)

    # dbt test failures are expected in this MVP; record as failed (not error).
    status = "failed"
    err_msg = "dbt tests failed (see runner logs)"
    if exit_code not in (1, 2):
        status = "error"
        err_msg = f"dbt returned exit code {exit_code}"

    # Keep dbt output in runner logs; don’t store full logs in ClickHouse for MVP.
    # If you want, a future improvement is to store a trimmed error snippet.
    print(output)

    return _finalize_run(ch, run_id, status=status, error_message=err_msg)

