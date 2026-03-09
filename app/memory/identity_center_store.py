from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
MEMORY_FILE = DATA_DIR / "identity_center_profiles.json"
STORE_VERSION = 1


@dataclass
class IdentityCenterProfileMemory:
    profile_name: str
    sso_start_url: str = ""
    sso_region: str = ""
    preferred_role_name: str = ""
    default_account_id: str = ""
    default_region: str = ""
    created_at: str = ""
    updated_at: str = ""
    last_used_at: str = ""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_store() -> dict[str, Any]:
    return {"version": STORE_VERSION, "profiles": {}}


def _normalize_profile(raw: dict[str, Any], name_hint: str) -> IdentityCenterProfileMemory:
    now = _now_iso()
    profile_name = (raw.get("profile_name") or name_hint or "").strip()
    if not profile_name:
        raise ValueError("profile_name is required.")

    created_at = (raw.get("created_at") or now).strip()
    updated_at = (raw.get("updated_at") or now).strip()
    last_used_at = (raw.get("last_used_at") or updated_at or now).strip()
    return IdentityCenterProfileMemory(
        profile_name=profile_name,
        sso_start_url=(raw.get("sso_start_url") or "").strip(),
        sso_region=(raw.get("sso_region") or "").strip(),
        preferred_role_name=(raw.get("preferred_role_name") or "").strip(),
        default_account_id=(raw.get("default_account_id") or "").strip(),
        default_region=(raw.get("default_region") or "").strip(),
        created_at=created_at,
        updated_at=updated_at,
        last_used_at=last_used_at,
    )


def _normalize_store(raw: dict[str, Any]) -> dict[str, Any]:
    profiles_raw = raw.get("profiles")
    if not isinstance(profiles_raw, dict):
        profiles_raw = {}
    normalized_profiles: dict[str, dict[str, Any]] = {}
    for key, value in profiles_raw.items():
        if not isinstance(value, dict):
            continue
        try:
            normalized = _normalize_profile(value, str(key))
        except Exception:
            continue
        normalized_profiles[normalized.profile_name] = asdict(normalized)
    return {"version": STORE_VERSION, "profiles": normalized_profiles}


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


def load_identity_center_store() -> dict[str, Any]:
    if not MEMORY_FILE.exists():
        return _default_store()
    try:
        content = json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return _default_store()
    if not isinstance(content, dict):
        return _default_store()
    return _normalize_store(content)


def get_profile_memory(profile_name: str) -> IdentityCenterProfileMemory | None:
    target = (profile_name or "").strip()
    if not target:
        return None
    store = load_identity_center_store()
    profiles = store.get("profiles", {})
    if not isinstance(profiles, dict):
        return None

    if target in profiles:
        return _normalize_profile(profiles[target], target)
    target_lower = target.lower()
    for name, payload in profiles.items():
        if name.lower() == target_lower and isinstance(payload, dict):
            return _normalize_profile(payload, name)
    return None


def upsert_profile_memory(memory: IdentityCenterProfileMemory) -> None:
    profile_name = (memory.profile_name or "").strip()
    if not profile_name:
        raise ValueError("profile_name is required.")
    now = _now_iso()

    store = load_identity_center_store()
    profiles = store.get("profiles", {})
    if not isinstance(profiles, dict):
        profiles = {}

    existing = get_profile_memory(profile_name)
    created_at = existing.created_at if existing else (memory.created_at or now)
    normalized = IdentityCenterProfileMemory(
        profile_name=profile_name,
        sso_start_url=(memory.sso_start_url or "").strip(),
        sso_region=(memory.sso_region or "").strip(),
        preferred_role_name=(memory.preferred_role_name or "").strip(),
        default_account_id=(memory.default_account_id or "").strip(),
        default_region=(memory.default_region or "").strip(),
        created_at=created_at,
        updated_at=now,
        last_used_at=memory.last_used_at or now,
    )
    profiles[profile_name] = asdict(normalized)
    _write_store({"version": STORE_VERSION, "profiles": profiles})


def touch_profile_memory(profile_name: str) -> None:
    existing = get_profile_memory(profile_name)
    if not existing:
        return
    now = _now_iso()
    existing.updated_at = now
    existing.last_used_at = now
    upsert_profile_memory(existing)
