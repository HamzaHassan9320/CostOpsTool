from __future__ import annotations

import configparser
import json
import shutil
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.auth import identity_center as mod


def _write_config(path: Path, sections: dict[str, dict[str, str]]) -> None:
    parser = configparser.RawConfigParser()
    for section, values in sections.items():
        if section != "default":
            parser.add_section(section)
        for key, value in values.items():
            parser.set(section, key, value)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        parser.write(f)


def _case_dir(name: str) -> Path:
    root = Path("tmp_test_artifacts") / f"{name}_{uuid.uuid4().hex[:8]}"
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _token_payload(start_url: str, expires: datetime) -> dict:
    return {
        "startUrl": start_url,
        "region": "eu-west-1",
        "accessToken": "token-abc",
        "expiresAt": expires.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def test_bootstrap_identity_center_profile_creates_sections():
    config_path = _case_dir("idc_bootstrap_create") / "aws" / "config"
    result = mod.bootstrap_identity_center_profile(
        mod.IdentityCenterBootstrapInput(
            profile_name="dev-sso",
            sso_start_url="https://d-example.awsapps.com/start",
            sso_region="eu-west-1",
            preferred_role_name="ReadOnly",
            default_account_id="123456789012",
            default_region="eu-west-1",
        ),
        config_path=config_path,
    )

    parser = configparser.RawConfigParser()
    parser.read(config_path, encoding="utf-8")
    assert result.profile_section == "profile dev-sso"
    assert parser.has_section("profile dev-sso")
    assert parser.has_section("sso-session costops-dev-sso")
    assert parser.get("profile dev-sso", "sso_session") == "costops-dev-sso"
    assert parser.get("profile dev-sso", "sso_role_name") == "ReadOnly"
    assert parser.get("profile dev-sso", "sso_account_id") == "123456789012"
    assert parser.get("sso-session costops-dev-sso", "sso_start_url") == "https://d-example.awsapps.com/start"


def test_bootstrap_identity_center_profile_normalizes_start_url():
    config_path = _case_dir("idc_bootstrap_normalize_start_url") / "aws" / "config"
    mod.bootstrap_identity_center_profile(
        mod.IdentityCenterBootstrapInput(
            profile_name="dev-sso",
            sso_start_url="https://SC-MASTER.awsapps.com/start/#/?tab=accounts",
            sso_region="eu-west-1",
        ),
        config_path=config_path,
    )

    parser = configparser.RawConfigParser()
    parser.read(config_path, encoding="utf-8")
    assert parser.get("sso-session costops-dev-sso", "sso_start_url") == "https://sc-master.awsapps.com/start"


def test_bootstrap_identity_center_profile_preserves_unrelated_config():
    config_path = _case_dir("idc_bootstrap_preserve") / "aws" / "config"
    _write_config(
        config_path,
        {
            "profile untouched": {"region": "us-east-1", "output": "json"},
            "profile dev-sso": {"region": "us-east-1", "custom_key": "keep"},
        },
    )

    mod.bootstrap_identity_center_profile(
        mod.IdentityCenterBootstrapInput(
            profile_name="dev-sso",
            sso_start_url="https://d-example.awsapps.com/start",
            sso_region="eu-west-1",
            preferred_role_name="",
            default_account_id="",
            default_region="eu-west-1",
        ),
        config_path=config_path,
    )

    parser = configparser.RawConfigParser()
    parser.read(config_path, encoding="utf-8")
    assert parser.get("profile untouched", "output") == "json"
    assert parser.get("profile dev-sso", "custom_key") == "keep"
    assert not parser.has_option("profile dev-sso", "sso_role_name")
    assert not parser.has_option("profile dev-sso", "sso_account_id")


def test_check_sso_token_status_missing_expired_and_valid():
    case_dir = _case_dir("idc_token_status")
    config_path = case_dir / "aws" / "config"
    cache_dir = case_dir / "aws" / "sso" / "cache"
    start_url = "https://d-example.awsapps.com/start"
    _write_config(
        config_path,
        {
            "profile dev-sso": {"sso_session": "costops-dev-sso"},
            "sso-session costops-dev-sso": {"sso_start_url": start_url, "sso_region": "eu-west-1"},
        },
    )

    missing = mod.check_sso_token_status("dev-sso", config_path=config_path, cache_dir=cache_dir)
    assert missing.status == "missing"

    cache_dir.mkdir(parents=True, exist_ok=True)
    expired_payload = _token_payload(start_url=start_url, expires=datetime.now(timezone.utc) - timedelta(minutes=5))
    (cache_dir / "expired.json").write_text(json.dumps(expired_payload), encoding="utf-8")
    expired = mod.check_sso_token_status("dev-sso", config_path=config_path, cache_dir=cache_dir)
    assert expired.status == "expired"

    valid_payload = _token_payload(start_url=start_url, expires=datetime.now(timezone.utc) + timedelta(minutes=30))
    (cache_dir / "valid.json").write_text(json.dumps(valid_payload), encoding="utf-8")
    valid = mod.check_sso_token_status("dev-sso", config_path=config_path, cache_dir=cache_dir)
    assert valid.status == "valid"
    assert valid.expires_at is not None


def test_check_sso_token_status_normalizes_start_url_before_matching():
    case_dir = _case_dir("idc_token_status_normalized_match")
    config_path = case_dir / "aws" / "config"
    cache_dir = case_dir / "aws" / "sso" / "cache"
    _write_config(
        config_path,
        {
            "profile dev-sso": {"sso_session": "costops-dev-sso"},
            "sso-session costops-dev-sso": {
                "sso_start_url": "https://sc-master.awsapps.com/start/#/?tab=accounts",
                "sso_region": "eu-west-1",
            },
        },
    )
    cache_dir.mkdir(parents=True, exist_ok=True)
    payload = _token_payload(
        start_url="https://sc-master.awsapps.com/start",
        expires=datetime.now(timezone.utc) + timedelta(minutes=30),
    )
    (cache_dir / "valid.json").write_text(json.dumps(payload), encoding="utf-8")

    status = mod.check_sso_token_status("dev-sso", config_path=config_path, cache_dir=cache_dir)
    assert status.status == "valid"


def test_enumerate_accessible_account_roles_uses_preferred_role_and_fallback(monkeypatch):
    class FakeSsoClient:
        def list_accounts(self, **kwargs):
            if kwargs.get("nextToken") == "2":
                return {"accountList": [{"accountId": "222222222222", "accountName": "Prod"}]}
            return {
                "accountList": [{"accountId": "111111111111", "accountName": "Dev"}],
                "nextToken": "2",
            }

        def list_account_roles(self, **kwargs):
            if kwargs["accountId"] == "111111111111":
                return {"roleList": [{"roleName": "ReadOnly"}, {"roleName": "Admin"}]}
            return {"roleList": [{"roleName": "Support"}]}

    monkeypatch.setattr(mod, "load_valid_sso_access_token", lambda *args, **kwargs: ("token", "eu-west-1"))

    discovered = mod.enumerate_accessible_account_roles(
        profile_name="dev-sso",
        preferred_role_name="Admin",
        client_factory=lambda service_name, region_name: FakeSsoClient(),
    )

    assert len(discovered) == 2
    assert discovered[0].account_id == "111111111111"
    assert discovered[0].role_name == "Admin"
    assert discovered[1].account_id == "222222222222"
    assert discovered[1].role_name == "Support"
