from __future__ import annotations

import configparser
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal
from urllib.parse import urlsplit, urlunsplit

import boto3


DEFAULT_SSO_REGISTRATION_SCOPES = "sso:account:access"


@dataclass
class IdentityCenterBootstrapInput:
    profile_name: str
    sso_start_url: str
    sso_region: str
    preferred_role_name: str = ""
    default_account_id: str = ""
    default_region: str = ""


@dataclass
class IdentityCenterBootstrapResult:
    profile_name: str
    profile_section: str
    sso_session_name: str
    config_path: str


@dataclass
class SsoTokenStatus:
    status: Literal["valid", "missing", "expired"]
    login_command: str
    expires_at: str | None = None


@dataclass
class AccessibleAccount:
    account_id: str
    account_name: str
    role_name: str | None
    roles: list[str]


def _profile_section_name(profile_name: str) -> str:
    return "default" if profile_name == "default" else f"profile {profile_name}"


def _aws_config_path() -> Path:
    return Path.home() / ".aws" / "config"


def _aws_sso_cache_dir() -> Path:
    return Path.home() / ".aws" / "sso" / "cache"


def _session_name_for_profile(profile_name: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9-]+", "-", profile_name.strip().lower())
    clean = clean.strip("-")
    if not clean:
        clean = "profile"
    return f"costops-{clean[:48]}"


def _normalize_sso_start_url(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    try:
        parsed = urlsplit(value)
    except Exception:
        return value
    if not parsed.scheme or not parsed.netloc:
        return value

    # IAM Identity Center start URL canonical form does not need query/fragment.
    path = parsed.path or ""
    if path == "/":
        path = ""
    else:
        path = path.rstrip("/")

    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path, "", ""))


def _parse_aws_timestamp(raw: str) -> datetime:
    value = (raw or "").strip()
    if value.endswith("UTC"):
        value = value[:-3] + "Z"
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _load_config(config_path: Path | None = None) -> tuple[configparser.RawConfigParser, Path]:
    path = config_path or _aws_config_path()
    parser = configparser.RawConfigParser()
    if path.exists():
        parser.read(path, encoding="utf-8")
    return parser, path


def _write_config_atomic(parser: configparser.RawConfigParser, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".tmp",
        delete=False,
        dir=str(path.parent),
        encoding="utf-8",
    ) as tmp:
        parser.write(tmp)
        tmp_path = tmp.name
    os.replace(tmp_path, path)


def bootstrap_identity_center_profile(
    bootstrap: IdentityCenterBootstrapInput,
    config_path: Path | None = None,
) -> IdentityCenterBootstrapResult:
    profile_name = (bootstrap.profile_name or "").strip()
    if not profile_name:
        raise ValueError("profile_name is required.")
    sso_start_url = _normalize_sso_start_url(bootstrap.sso_start_url or "")
    if not sso_start_url:
        raise ValueError("sso_start_url is required.")
    sso_region = (bootstrap.sso_region or "").strip()
    if not sso_region:
        raise ValueError("sso_region is required.")

    parser, path = _load_config(config_path=config_path)
    profile_section = _profile_section_name(profile_name)
    session_name = _session_name_for_profile(profile_name)
    session_section = f"sso-session {session_name}"

    if not parser.has_section(profile_section) and profile_section != "default":
        parser.add_section(profile_section)
    if not parser.has_section(session_section):
        parser.add_section(session_section)

    parser.set(profile_section, "sso_session", session_name)
    default_region = (bootstrap.default_region or "").strip()
    if default_region:
        parser.set(profile_section, "region", default_region)
    preferred_role_name = (bootstrap.preferred_role_name or "").strip()
    if preferred_role_name:
        parser.set(profile_section, "sso_role_name", preferred_role_name)
    elif parser.has_option(profile_section, "sso_role_name"):
        parser.remove_option(profile_section, "sso_role_name")

    default_account_id = (bootstrap.default_account_id or "").strip()
    if default_account_id:
        parser.set(profile_section, "sso_account_id", default_account_id)
    elif parser.has_option(profile_section, "sso_account_id"):
        parser.remove_option(profile_section, "sso_account_id")

    parser.set(session_section, "sso_start_url", sso_start_url)
    parser.set(session_section, "sso_region", sso_region)
    parser.set(session_section, "sso_registration_scopes", DEFAULT_SSO_REGISTRATION_SCOPES)

    _write_config_atomic(parser=parser, path=path)
    return IdentityCenterBootstrapResult(
        profile_name=profile_name,
        profile_section=profile_section,
        sso_session_name=session_name,
        config_path=str(path),
    )


def profile_uses_sso(profile_name: str, config_path: Path | None = None) -> bool:
    parser, _ = _load_config(config_path=config_path)
    section = _profile_section_name(profile_name)
    if not parser.has_section(section):
        return False
    return parser.has_option(section, "sso_session") or parser.has_option(section, "sso_start_url")


def _resolve_profile_sso_values(profile_name: str, config_path: Path | None = None) -> dict[str, str]:
    parser, _ = _load_config(config_path=config_path)
    profile_section = _profile_section_name(profile_name)
    if not parser.has_section(profile_section):
        raise RuntimeError(f"AWS profile '{profile_name}' not found in shared config.")

    profile_values = dict(parser.items(profile_section))
    session_name = (profile_values.get("sso_session") or "").strip()
    sso_start_url = (profile_values.get("sso_start_url") or "").strip()
    sso_region = (profile_values.get("sso_region") or "").strip()

    if session_name:
        session_section = f"sso-session {session_name}"
        if not parser.has_section(session_section):
            raise RuntimeError(
                f"Profile '{profile_name}' references missing section '{session_section}' in ~/.aws/config."
            )
        session_values = dict(parser.items(session_section))
        sso_start_url = sso_start_url or (session_values.get("sso_start_url") or "").strip()
        sso_region = sso_region or (session_values.get("sso_region") or "").strip()

    sso_start_url = _normalize_sso_start_url(sso_start_url)
    if not sso_start_url:
        raise RuntimeError(f"Profile '{profile_name}' has no IAM Identity Center start URL configured.")
    if not sso_region:
        raise RuntimeError(f"Profile '{profile_name}' has no IAM Identity Center region configured.")

    return {
        "sso_start_url": sso_start_url,
        "sso_region": sso_region,
        "sso_session_name": session_name,
        "sso_account_id": (profile_values.get("sso_account_id") or "").strip(),
        "sso_role_name": (profile_values.get("sso_role_name") or "").strip(),
        "region": (profile_values.get("region") or "").strip(),
    }


def get_profile_sso_values(profile_name: str, config_path: Path | None = None) -> dict[str, str]:
    return _resolve_profile_sso_values(profile_name=profile_name, config_path=config_path)


def check_sso_token_status(
    profile_name: str,
    config_path: Path | None = None,
    cache_dir: Path | None = None,
) -> SsoTokenStatus:
    login_command = f"aws sso login --profile {profile_name}"
    sso_values = _resolve_profile_sso_values(profile_name=profile_name, config_path=config_path)
    start_url = _normalize_sso_start_url(sso_values["sso_start_url"])

    token_cache_dir = cache_dir or _aws_sso_cache_dir()
    if not token_cache_dir.exists():
        return SsoTokenStatus(status="missing", login_command=login_command)

    now = datetime.now(timezone.utc)
    matched_expirations: list[datetime] = []
    for token_file in token_cache_dir.glob("*.json"):
        try:
            payload = json.loads(token_file.read_text(encoding="utf-8"))
        except Exception:
            continue

        access_token = payload.get("accessToken")
        token_start_url = _normalize_sso_start_url(str(payload.get("startUrl") or ""))
        expires_at = payload.get("expiresAt")
        if not access_token or not expires_at:
            continue
        if token_start_url != start_url:
            continue

        try:
            expiry = _parse_aws_timestamp(str(expires_at))
        except Exception:
            continue
        matched_expirations.append(expiry)

    if not matched_expirations:
        return SsoTokenStatus(status="missing", login_command=login_command)

    latest = sorted(matched_expirations, reverse=True)[0]
    if latest > now:
        return SsoTokenStatus(status="valid", login_command=login_command, expires_at=latest.isoformat())
    return SsoTokenStatus(status="expired", login_command=login_command, expires_at=latest.isoformat())


def load_valid_sso_access_token(
    profile_name: str,
    config_path: Path | None = None,
    cache_dir: Path | None = None,
) -> tuple[str, str]:
    status = check_sso_token_status(profile_name=profile_name, config_path=config_path, cache_dir=cache_dir)
    if status.status != "valid":
        raise RuntimeError(f"IAM Identity Center login required. Run `{status.login_command}` and retry.")

    token_cache_dir = cache_dir or _aws_sso_cache_dir()
    sso_values = _resolve_profile_sso_values(profile_name=profile_name, config_path=config_path)
    start_url = _normalize_sso_start_url(sso_values["sso_start_url"])
    sso_region = sso_values["sso_region"]

    now = datetime.now(timezone.utc)
    candidates: list[tuple[datetime, str]] = []
    for token_file in token_cache_dir.glob("*.json"):
        try:
            payload = json.loads(token_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        token = payload.get("accessToken")
        token_start_url = _normalize_sso_start_url(str(payload.get("startUrl") or ""))
        expires_at = payload.get("expiresAt")
        if not token or not expires_at:
            continue
        if token_start_url != start_url:
            continue
        try:
            expiry = _parse_aws_timestamp(str(expires_at))
        except Exception:
            continue
        if expiry > now:
            candidates.append((expiry, str(token)))

    if not candidates:
        raise RuntimeError(f"IAM Identity Center login required. Run `aws sso login --profile {profile_name}` and retry.")
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1], sso_region


def _sso_client(region_name: str, client_factory: Callable[..., Any] | None = None):
    if client_factory:
        return client_factory("sso", region_name=region_name)
    return boto3.client("sso", region_name=region_name)


def list_accessible_accounts(access_token: str, sso_region: str, client_factory: Callable[..., Any] | None = None) -> list[dict]:
    client = _sso_client(region_name=sso_region, client_factory=client_factory)
    accounts: list[dict] = []
    next_token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"accessToken": access_token}
        if next_token:
            kwargs["nextToken"] = next_token
        response = client.list_accounts(**kwargs)
        accounts.extend(response.get("accountList", []) or [])
        next_token = response.get("nextToken")
        if not next_token:
            break
    return accounts


def list_account_roles(
    access_token: str,
    sso_region: str,
    account_id: str,
    client_factory: Callable[..., Any] | None = None,
) -> list[str]:
    client = _sso_client(region_name=sso_region, client_factory=client_factory)
    roles: list[str] = []
    next_token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"accessToken": access_token, "accountId": account_id}
        if next_token:
            kwargs["nextToken"] = next_token
        response = client.list_account_roles(**kwargs)
        for item in response.get("roleList", []) or []:
            role_name = (item.get("roleName") or "").strip()
            if role_name:
                roles.append(role_name)
        next_token = response.get("nextToken")
        if not next_token:
            break
    return roles


def choose_role(roles: list[str], preferred_role_name: str | None) -> str | None:
    if not roles:
        return None
    preferred = (preferred_role_name or "").strip()
    if preferred and preferred in roles:
        return preferred
    return roles[0]


def enumerate_accessible_account_roles(
    profile_name: str,
    preferred_role_name: str | None = None,
    config_path: Path | None = None,
    cache_dir: Path | None = None,
    client_factory: Callable[..., Any] | None = None,
) -> list[AccessibleAccount]:
    token, sso_region = load_valid_sso_access_token(profile_name, config_path=config_path, cache_dir=cache_dir)
    accounts = list_accessible_accounts(token, sso_region=sso_region, client_factory=client_factory)
    discovered: list[AccessibleAccount] = []
    for account in accounts:
        account_id = (account.get("accountId") or "").strip()
        if not account_id:
            continue
        account_name = (account.get("accountName") or account_id).strip() or account_id
        roles = list_account_roles(
            access_token=token,
            sso_region=sso_region,
            account_id=account_id,
            client_factory=client_factory,
        )
        discovered.append(
            AccessibleAccount(
                account_id=account_id,
                account_name=account_name,
                role_name=choose_role(roles, preferred_role_name=preferred_role_name),
                roles=roles,
            )
        )
    return discovered


def get_role_credentials(
    access_token: str,
    sso_region: str,
    account_id: str,
    role_name: str,
    client_factory: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    client = _sso_client(region_name=sso_region, client_factory=client_factory)
    response = client.get_role_credentials(
        accessToken=access_token,
        accountId=account_id,
        roleName=role_name,
    )
    return response.get("roleCredentials") or {}


def make_session_from_role_credentials(role_credentials: dict[str, Any], region_name: str | None = None):
    return boto3.Session(
        aws_access_key_id=role_credentials["accessKeyId"],
        aws_secret_access_key=role_credentials["secretAccessKey"],
        aws_session_token=role_credentials["sessionToken"],
        region_name=region_name or None,
    )
