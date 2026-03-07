from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
MEMORY_FILE = DATA_DIR / "project_memory.json"
STORE_VERSION = 2


@dataclass
class ProjectMemory:
    project_name: str
    aws_profile_name: str = ""
    account_id: str = ""
    cur_skipped: bool = False
    athena_database: str = ""
    athena_table: str = ""
    athena_workgroup: str = "primary"
    athena_output_s3: str = ""
    athena_profile_name: str = ""
    athena_region: str = "us-east-1"
    created_at: str = ""
    updated_at: str = ""
    last_used_at: str = ""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_store() -> dict[str, Any]:
    return {"version": STORE_VERSION, "projects": {}}


def _normalize_project(raw: dict[str, Any], name_hint: str) -> ProjectMemory:
    now = _now_iso()
    project_name = (raw.get("project_name") or name_hint or "").strip()
    if not project_name:
        raise ValueError("project_name is required.")

    created_at = (raw.get("created_at") or now).strip()
    updated_at = (raw.get("updated_at") or now).strip()
    last_used_at = (raw.get("last_used_at") or updated_at or now).strip()

    return ProjectMemory(
        project_name=project_name,
        aws_profile_name=(raw.get("aws_profile_name") or "").strip(),
        account_id=(raw.get("account_id") or "").strip(),
        cur_skipped=bool(raw.get("cur_skipped", False)),
        athena_database=(raw.get("athena_database") or "").strip(),
        athena_table=(raw.get("athena_table") or "").strip(),
        athena_workgroup=(raw.get("athena_workgroup") or "primary").strip() or "primary",
        athena_output_s3=(raw.get("athena_output_s3") or "").strip(),
        athena_profile_name=(raw.get("athena_profile_name") or "").strip(),
        athena_region=(raw.get("athena_region") or "us-east-1").strip() or "us-east-1",
        created_at=created_at,
        updated_at=updated_at,
        last_used_at=last_used_at,
    )


def _normalize_store(raw: dict[str, Any]) -> dict[str, Any]:
    projects_raw = raw.get("projects")
    if not isinstance(projects_raw, dict):
        projects_raw = {}

    normalized_projects: dict[str, dict[str, Any]] = {}
    for key, value in projects_raw.items():
        if not isinstance(value, dict):
            continue
        try:
            project = _normalize_project(value, str(key))
        except Exception:
            continue
        normalized_projects[project.project_name] = asdict(project)

    return {"version": STORE_VERSION, "projects": normalized_projects}


def _write_store(store: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    normalized = _normalize_store(store)

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".tmp",
        delete=False,
        dir=str(DATA_DIR),
        encoding="utf-8",
    ) as tmp:
        json.dump(normalized, tmp, indent=2)
        tmp.write("\n")
        tmp_path = tmp.name

    os.replace(tmp_path, MEMORY_FILE)


def load_memory_store() -> dict[str, Any]:
    if not MEMORY_FILE.exists():
        return _default_store()

    try:
        content = json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return _default_store()

    if not isinstance(content, dict):
        return _default_store()
    return _normalize_store(content)


def list_projects() -> list[str]:
    store = load_memory_store()
    projects = store.get("projects", {})
    return sorted(projects.keys(), key=lambda name: name.lower())


def get_project(name: str) -> ProjectMemory | None:
    target = (name or "").strip()
    if not target:
        return None

    store = load_memory_store()
    projects = store.get("projects", {})
    if not isinstance(projects, dict):
        return None

    if target in projects:
        return _normalize_project(projects[target], target)

    lower_target = target.lower()
    for project_name, payload in projects.items():
        if project_name.lower() == lower_target and isinstance(payload, dict):
            return _normalize_project(payload, project_name)
    return None


def upsert_project(memory: ProjectMemory) -> None:
    now = _now_iso()
    project_name = (memory.project_name or "").strip()
    if not project_name:
        raise ValueError("project_name is required.")

    store = load_memory_store()
    projects = store.get("projects", {})
    if not isinstance(projects, dict):
        projects = {}

    existing = get_project(project_name)
    created_at = existing.created_at if existing else (memory.created_at or now)

    record = ProjectMemory(
        project_name=project_name,
        aws_profile_name=(memory.aws_profile_name or "").strip(),
        account_id=(memory.account_id or "").strip(),
        cur_skipped=bool(memory.cur_skipped),
        athena_database=(memory.athena_database or "").strip(),
        athena_table=(memory.athena_table or "").strip(),
        athena_workgroup=(memory.athena_workgroup or "primary").strip() or "primary",
        athena_output_s3=(memory.athena_output_s3 or "").strip(),
        athena_profile_name=(memory.athena_profile_name or "").strip(),
        athena_region=(memory.athena_region or "us-east-1").strip() or "us-east-1",
        created_at=created_at,
        updated_at=now,
        last_used_at=memory.last_used_at or now,
    )
    projects[project_name] = asdict(record)
    _write_store({"version": STORE_VERSION, "projects": projects})


def touch_project(name: str) -> None:
    project = get_project(name)
    if not project:
        return

    now = _now_iso()
    project.updated_at = now
    project.last_used_at = now
    upsert_project(project)
