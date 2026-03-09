import shutil
import uuid
from pathlib import Path

from app.memory import identity_center_store as mod
from app.memory.identity_center_store import IdentityCenterProfileMemory


def _case_dir(name: str) -> Path:
    root = Path("tmp_test_artifacts") / f"{name}_{uuid.uuid4().hex[:8]}"
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)
    return root


def test_identity_center_store_upsert_and_get(monkeypatch):
    data_dir = _case_dir("idc_store_upsert") / "data"
    memory_file = data_dir / "identity_center_profiles.json"

    monkeypatch.setattr(mod, "DATA_DIR", data_dir)
    monkeypatch.setattr(mod, "MEMORY_FILE", memory_file)

    mod.upsert_profile_memory(
        IdentityCenterProfileMemory(
            profile_name="dev-sso",
            sso_start_url="https://d-example.awsapps.com/start",
            sso_region="eu-west-1",
            preferred_role_name="ReadOnly",
            default_account_id="123456789012",
            default_region="eu-west-1",
        )
    )

    loaded = mod.get_profile_memory("dev-sso")
    assert loaded is not None
    assert loaded.sso_region == "eu-west-1"
    assert loaded.preferred_role_name == "ReadOnly"
    assert loaded.default_account_id == "123456789012"
