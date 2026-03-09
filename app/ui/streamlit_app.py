from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import streamlit as st

# Ensure `app.*` imports work even when Streamlit sets CWD/script path differently.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.agent.multi_account import AccountExecutionTarget, run_scan_for_targets
from app.auth.identity_center import (
    IdentityCenterBootstrapInput,
    bootstrap_identity_center_profile,
    check_sso_token_status,
    choose_role,
    enumerate_accessible_account_roles,
    get_profile_sso_values,
    get_role_credentials,
    list_account_roles,
    load_valid_sso_access_token,
    make_session_from_role_credentials,
    profile_uses_sso,
)
from app.auth.session_factory import make_boto3_session
from app.auth.validation import validate_profile
from app.core.types import RunContext
from app.llm.router import route
from app.memory.identity_center_store import (
    IdentityCenterProfileMemory,
    get_profile_memory,
    upsert_profile_memory,
)
from app.memory.store import ProjectMemory, get_project, list_projects, upsert_project
from app.outputs.excel_writer import write_excel
from app.outputs.report_builder import recommendations_to_rows

# IMPORTANT: ensure plugin registers
import app.services.nat.plugin  # noqa: F401

ACTION_ID = "optimization.run_scan"

STAGE_AWAIT_INTENT = "await_intent"
STAGE_AWAIT_PROFILE = "await_profile"
STAGE_VALIDATING_PROFILE = "validating_profile"
STAGE_AWAIT_IDC_FIELDS = "await_idc_fields"
STAGE_AWAIT_SSO_LOGIN = "await_sso_login"
STAGE_AWAIT_PROJECT_SELECTION = "await_project_selection"
STAGE_AWAIT_ACCOUNT_SELECTION = "await_account_selection"
STAGE_AWAIT_CUR_FIELDS = "await_cur_fields"
STAGE_RUNNING_SCAN = "running_scan"
STAGE_READY_WITH_RESULTS = "ready_with_results"

IDC_FIELDS = [
    "sso_start_url",
    "sso_region",
    "preferred_role_name",
    "default_account_id",
    "default_region",
]
IDC_FIELD_PROMPTS = {
    "sso_start_url": "Enter IAM Identity Center start URL (for example https://d-xxxxxx.awsapps.com/start).",
    "sso_region": "Enter IAM Identity Center region (for example eu-west-1).",
    "preferred_role_name": (
        "Enter preferred role name/permission set for account scans (optional). "
        "Press Enter to skip."
    ),
    "default_account_id": "Enter default AWS account ID for scope=current (optional). Press Enter to skip.",
    "default_region": "Enter default AWS region for assumed-role sessions (optional, default: eu-west-1).",
}

CUR_FIELDS_ALL = [
    "athena_database",
    "athena_table",
    "athena_workgroup",
    "athena_output_s3",
    "athena_profile_name",
    "athena_account_id",
    "athena_region",
]
CUR_FIELDS_REQUIRED = [
    "athena_database",
    "athena_table",
    "athena_workgroup",
    "athena_output_s3",
    "athena_profile_name",
]
CUR_FIELD_PROMPTS = {
    "athena_database": "Enter Athena CUR database name.",
    "athena_table": "Enter Athena CUR table name.",
    "athena_workgroup": "Enter Athena workgroup (default: primary).",
    "athena_output_s3": "Enter Athena output S3 path (s3://bucket/prefix).",
    "athena_profile_name": (
        "Enter CUR query profile name for the account that has Athena/CUR "
        "(often management/payer)."
    ),
    "athena_account_id": (
        "Enter CUR source AWS account ID (12 digits, account that Athena/CUR profile will assume into). "
        "Optional for single-account runs."
    ),
    "athena_region": "Enter CUR Athena region (default: us-east-1).",
}

STEP_LABELS = {
    "resolve_context": "Resolve context",
    "discover_nat_gateways": "Discover NAT gateways",
    "collect_nat_activity": "Collect NAT activity",
    "identify_idle_nat": "Identify idle NAT",
    "query_nat_cur_net_amortized_by_ids": "Query NAT CUR",
    "build_nat_recommendations": "Build recommendations",
}

SUPPORTED_SERVICES = {"nat"}


def _default_chat_context() -> dict[str, Any]:
    return {
        "last_validated_profile": None,
        "last_account_id": None,
        "last_project_name": None,
        "last_scope": "current",
        "last_target_account_id": None,
        "last_run_request": None,
        "last_run_status": None,
        "last_run_error": None,
        "last_error_type": None,
        "last_requested_service": None,
    }


def _chat_context() -> dict[str, Any]:
    current = st.session_state.get("chat_context")
    if not isinstance(current, dict):
        current = _default_chat_context()
        st.session_state["chat_context"] = current
    for key, value in _default_chat_context().items():
        current.setdefault(key, value)
    return current


def _classify_run_error(error_text: str) -> str:
    text = (error_text or "").strip().lower()
    if not text:
        return "unknown"
    if "cur preflight failed" in text:
        return "cur_preflight"
    if any(
        token in text
        for token in {"error when retrieving token from sso", "token has expired", "sso", "identity center login"}
    ):
        return "sso_expired"
    if any(token in text for token in {"athena query timed out", "timed out while waiting for completion"}):
        return "athena_timeout"
    if any(token in text for token in {"unauthorizedoperation", "not authorized", "accessdenied", "explicit deny"}):
        return "permission_denied"
    return "unknown"


def _snapshot_run_request() -> dict[str, Any]:
    return {
        "action": ACTION_ID,
        "profile": st.session_state.get("profile", ""),
        "account_id": st.session_state.get("account_id"),
        "account_name": st.session_state.get("account_name"),
        "role_name": st.session_state.get("role_name"),
        "account_scope": st.session_state.get("account_scope", "current"),
        "target_account_id": st.session_state.get("target_account_id"),
        "selected_targets": st.session_state.get("selected_targets") or [],
        "project_name": st.session_state.get("project_name", ""),
        "athena_database": st.session_state.get("athena_database", ""),
        "athena_table": st.session_state.get("athena_table", ""),
        "athena_workgroup": st.session_state.get("athena_workgroup", "primary"),
        "athena_output_s3": st.session_state.get("athena_output_s3", ""),
        "athena_profile_name": st.session_state.get("athena_profile_name", ""),
        "athena_account_id": st.session_state.get("athena_account_id", ""),
        "athena_region": st.session_state.get("athena_region", "us-east-1"),
        "cur_skipped": bool(st.session_state.get("cur_skipped")),
    }


def _restore_run_request(snapshot: dict[str, Any]) -> None:
    st.session_state["profile"] = str(snapshot.get("profile") or "").strip()
    st.session_state["account_id"] = snapshot.get("account_id")
    st.session_state["account_name"] = snapshot.get("account_name")
    st.session_state["role_name"] = snapshot.get("role_name")
    st.session_state["account_scope"] = str(snapshot.get("account_scope") or "current")
    st.session_state["target_account_id"] = str(snapshot.get("target_account_id") or "").strip() or None
    st.session_state["selected_targets"] = list(snapshot.get("selected_targets") or [])
    st.session_state["project_name"] = str(snapshot.get("project_name") or "").strip()
    st.session_state["athena_database"] = str(snapshot.get("athena_database") or "").strip()
    st.session_state["athena_table"] = str(snapshot.get("athena_table") or "").strip()
    st.session_state["athena_workgroup"] = str(snapshot.get("athena_workgroup") or "primary").strip() or "primary"
    st.session_state["athena_output_s3"] = str(snapshot.get("athena_output_s3") or "").strip()
    st.session_state["athena_profile_name"] = str(snapshot.get("athena_profile_name") or "").strip()
    st.session_state["athena_account_id"] = str(snapshot.get("athena_account_id") or "").strip()
    st.session_state["athena_region"] = str(snapshot.get("athena_region") or "us-east-1").strip() or "us-east-1"
    st.session_state["cur_skipped"] = False
    st.session_state["force_cur_recollect"] = bool(snapshot.get("cur_skipped"))


def _append_message(role: str, content: str) -> None:
    st.session_state["messages"].append({"role": role, "content": content})


def _assistant(content: str) -> None:
    _append_message("assistant", content)


def _apply_theme() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"], [data-testid="collapsedControl"] { display: none !important; }
        .stApp {
            background:
                radial-gradient(circle at 12% 8%, rgba(34, 197, 164, 0.24) 0%, rgba(34, 197, 164, 0) 38%),
                radial-gradient(circle at 92% 4%, rgba(56, 189, 248, 0.20) 0%, rgba(56, 189, 248, 0) 42%),
                linear-gradient(160deg, #f6fbfa 0%, #eef6f8 55%, #ecf3f6 100%);
        }
        .title-wrap {
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 16px;
            background: rgba(255, 255, 255, 0.78);
            backdrop-filter: blur(2px);
            padding: 14px 16px;
            margin-bottom: 12px;
            box-shadow: 0 8px 20px rgba(15, 23, 42, 0.05);
        }
        .title-main {
            color: #0f172a;
            font-size: 1.42rem;
            font-weight: 700;
            letter-spacing: 0.01em;
            margin: 0;
        }
        .title-sub {
            color: #334155;
            font-size: 0.92rem;
            margin: 4px 0 0 0;
        }
        .status-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 10px;
            margin-bottom: 8px;
        }
        .status-card {
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 12px;
            background: rgba(255, 255, 255, 0.82);
            padding: 8px 10px;
        }
        .status-label {
            color: #475569;
            font-size: 0.76rem;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }
        .status-value {
            color: #0f172a;
            font-size: 0.94rem;
            font-weight: 600;
            margin-top: 2px;
            overflow-wrap: anywhere;
        }
        @media (max-width: 900px) {
            .status-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _init_state() -> None:
    defaults = {
        "messages": [],
        "stage": STAGE_AWAIT_INTENT,
        "profile": "",
        "account_id": None,
        "account_name": None,
        "role_name": None,
        "account_scope": "current",
        "target_account_id": None,
        "selected_targets": [],
        "account_options": [],
        "project_name": "",
        "project_options": [],
        "idc_profile_pending": "",
        "idc_collection_mode": "scan",
        "idc_fields_queue": [],
        "idc_input": {},
        "sso_wait_mode": None,
        "sso_wait_profile": None,
        "profile_auth_mode": "unknown",
        "athena_database": os.getenv("ATHENA_DATABASE", ""),
        "athena_table": os.getenv("ATHENA_TABLE", ""),
        "athena_workgroup": os.getenv("ATHENA_WORKGROUP", "primary"),
        "athena_output_s3": os.getenv("ATHENA_OUTPUT_S3", ""),
        "athena_profile_name": os.getenv("ATHENA_PROFILE_NAME", ""),
        "athena_account_id": os.getenv("ATHENA_ACCOUNT_ID", ""),
        "athena_region": os.getenv("ATHENA_REGION", "us-east-1"),
        "cur_skipped": False,
        "force_cur_recollect": False,
        "cur_session": None,
        "cur_fields_queue": [],
        "cur_edit_mode": False,
        "recommendations": [],
        "run_diagnostics": {},
        "run_sql": None,
        "run_warnings": [],
        "run_cur_cost_lines": [],
        "latest_report_path": None,
        "latest_report_name": None,
        "chat_context": _default_chat_context(),
        "sso_account_cache": {},
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

    if not st.session_state["messages"]:
        _assistant(
            "Ask me to analyze NAT gateway optimization for an account. Example: "
            "`analyze idle nat gateway savings for all accounts with profile my-sso-profile`.\n\n"
            "Commands: `/analyze profile=<name> scope=current|all|account:<id>`, "
            "`/project <name>`, `/athena edit`, `/rescan`, `/retry`, `/help`."
        )
    _chat_context()


def _reset_scan_outputs() -> None:
    st.session_state["recommendations"] = []
    st.session_state["run_diagnostics"] = {}
    st.session_state["run_sql"] = None
    st.session_state["run_warnings"] = []
    st.session_state["run_cur_cost_lines"] = []
    st.session_state["latest_report_path"] = None
    st.session_state["latest_report_name"] = None
    st.session_state["cur_session"] = None


def _reset_project_settings() -> None:
    st.session_state["athena_database"] = os.getenv("ATHENA_DATABASE", "")
    st.session_state["athena_table"] = os.getenv("ATHENA_TABLE", "")
    st.session_state["athena_workgroup"] = os.getenv("ATHENA_WORKGROUP", "primary")
    st.session_state["athena_output_s3"] = os.getenv("ATHENA_OUTPUT_S3", "")
    st.session_state["athena_profile_name"] = os.getenv("ATHENA_PROFILE_NAME", "")
    st.session_state["athena_account_id"] = os.getenv("ATHENA_ACCOUNT_ID", "")
    st.session_state["athena_region"] = os.getenv("ATHENA_REGION", "us-east-1")
    st.session_state["cur_skipped"] = False
    st.session_state["force_cur_recollect"] = False


def _missing_cur_fields() -> list[str]:
    missing = []
    for field in CUR_FIELDS_REQUIRED:
        if not str(st.session_state.get(field, "")).strip():
            missing.append(field)
    return missing


def _needs_cur_questions() -> bool:
    if bool(st.session_state.get("force_cur_recollect")):
        return True
    return bool(_missing_cur_fields())


def _build_context(
    account_id: str | None,
    account_name: str | None = None,
    role_name: str | None = None,
    aws_session: Any | None = None,
    cur_session: Any | None = None,
    progress_callback: Callable[[str, str, float | None], None] | None = None,
) -> RunContext:
    return RunContext(
        profile_name=st.session_state.get("profile", ""),
        account_id=account_id,
        days=30,
        regions=[],
        athena_database=st.session_state.get("athena_database", "").strip(),
        athena_table=st.session_state.get("athena_table", "").strip(),
        athena_workgroup=st.session_state.get("athena_workgroup", "").strip(),
        athena_output_s3=st.session_state.get("athena_output_s3", "").strip(),
        athena_profile_name=(st.session_state.get("athena_profile_name", "").strip() or None),
        athena_region=st.session_state.get("athena_region", "us-east-1").strip() or "us-east-1",
        requested_by=st.session_state.get("project_name") or None,
        progress_callback=progress_callback,
        aws_session=aws_session,
        cur_session=cur_session,
        account_name=account_name,
        role_name=role_name,
    )


def _persist_project_memory() -> None:
    project_name = (st.session_state.get("project_name") or "").strip()
    if not project_name:
        return

    existing = get_project(project_name)
    memory = ProjectMemory(
        project_name=project_name,
        aws_profile_name=st.session_state.get("profile", "").strip(),
        account_id=(st.session_state.get("account_id") or "").strip(),
        # Backward compatibility only: CUR is mandatory so this is always false in decision logic.
        cur_skipped=False,
        athena_database=st.session_state.get("athena_database", "").strip(),
        athena_table=st.session_state.get("athena_table", "").strip(),
        athena_workgroup=st.session_state.get("athena_workgroup", "primary").strip() or "primary",
        athena_output_s3=st.session_state.get("athena_output_s3", "").strip(),
        athena_profile_name=st.session_state.get("athena_profile_name", "").strip(),
        athena_account_id=st.session_state.get("athena_account_id", "").strip(),
        athena_region=st.session_state.get("athena_region", "us-east-1").strip() or "us-east-1",
        created_at=existing.created_at if existing else "",
        last_used_at="",
    )
    upsert_project(memory)


def _seed_identity_center_memory_from_config(profile_name: str) -> IdentityCenterProfileMemory | None:
    existing = get_profile_memory(profile_name)
    if existing and existing.sso_start_url and existing.sso_region:
        return existing

    try:
        values = get_profile_sso_values(profile_name)
    except Exception:
        return existing

    start_url = (values.get("sso_start_url") or "").strip()
    region = (values.get("sso_region") or "").strip()
    if not start_url or not region:
        return existing

    seeded = IdentityCenterProfileMemory(
        profile_name=profile_name,
        sso_start_url=start_url,
        sso_region=region,
        preferred_role_name=(values.get("sso_role_name") or "").strip(),
        default_account_id=(values.get("sso_account_id") or "").strip(),
        default_region=(values.get("region") or "").strip(),
    )
    upsert_profile_memory(seeded)
    return get_profile_memory(profile_name)


def _set_sso_wait_state(*, mode: str, profile_name: str) -> None:
    st.session_state["sso_wait_mode"] = mode
    st.session_state["sso_wait_profile"] = profile_name


def _clear_sso_wait_state() -> None:
    st.session_state["sso_wait_mode"] = None
    st.session_state["sso_wait_profile"] = None


def _await_sso_login(*, mode: str, profile_name: str, login_command: str) -> None:
    _set_sso_wait_state(mode=mode, profile_name=profile_name)
    st.session_state["stage"] = STAGE_AWAIT_SSO_LOGIN
    label = "scan profile" if mode == "scan" else "CUR profile"
    _assistant(
        f"Waiting for {label} SSO login.\n"
        f"Run: `{login_command}`\n"
        "Then send `/retry` to continue."
    )


def _start_idc_collection(
    profile_name: str,
    existing: IdentityCenterProfileMemory | None,
    mode: str = "scan",
) -> None:
    seed = {
        "sso_start_url": (existing.sso_start_url if existing else "").strip(),
        "sso_region": (existing.sso_region if existing else "").strip(),
        "preferred_role_name": (existing.preferred_role_name if existing else "").strip(),
        "default_account_id": (existing.default_account_id if existing else "").strip(),
        "default_region": ((existing.default_region if existing else "").strip() or "eu-west-1"),
    }

    queue: list[str] = []
    for required in ("sso_start_url", "sso_region"):
        if not seed[required]:
            queue.append(required)
    if existing is None:
        for optional in ("preferred_role_name", "default_account_id", "default_region"):
            if optional not in queue:
                queue.append(optional)

    st.session_state["idc_profile_pending"] = profile_name
    st.session_state["idc_collection_mode"] = mode
    st.session_state["idc_input"] = seed
    st.session_state["idc_fields_queue"] = queue

    if not queue:
        return

    st.session_state["stage"] = STAGE_AWAIT_IDC_FIELDS
    _ask_next_idc_prompt()


def _ask_next_idc_prompt() -> None:
    queue = st.session_state.get("idc_fields_queue") or []
    if not queue:
        return
    field = queue[0]
    current = str((st.session_state.get("idc_input") or {}).get(field) or "").strip()
    prompt = IDC_FIELD_PROMPTS[field]
    if current:
        prompt += f" Current: `{current}`."
    _assistant(prompt)


def _finalize_idc_collection_and_continue() -> None:
    profile_name = str(st.session_state.get("idc_profile_pending") or "").strip()
    mode = str(st.session_state.get("idc_collection_mode") or "scan").strip().lower()
    if not profile_name:
        st.session_state["stage"] = STAGE_AWAIT_PROFILE
        _assistant("Profile context was lost. Enter the profile name again.")
        return

    payload = st.session_state.get("idc_input") or {}
    memory = IdentityCenterProfileMemory(
        profile_name=profile_name,
        sso_start_url=str(payload.get("sso_start_url") or "").strip(),
        sso_region=str(payload.get("sso_region") or "").strip(),
        preferred_role_name=str(payload.get("preferred_role_name") or "").strip(),
        default_account_id=str(payload.get("default_account_id") or "").strip(),
        default_region=str(payload.get("default_region") or "").strip() or "eu-west-1",
    )
    upsert_profile_memory(memory)

    bootstrap_identity_center_profile(
        IdentityCenterBootstrapInput(
            profile_name=profile_name,
            sso_start_url=memory.sso_start_url,
            sso_region=memory.sso_region,
            preferred_role_name=memory.preferred_role_name,
            default_account_id=memory.default_account_id,
            default_region=memory.default_region,
        )
    )

    if mode == "cur":
        _assistant(f"IAM Identity Center bootstrap saved for CUR profile `{profile_name}`.")
        if st.session_state.get("profile") and st.session_state.get("project_name"):
            _assistant("Re-running CUR preflight...")
            st.session_state["stage"] = STAGE_RUNNING_SCAN
        else:
            st.session_state["stage"] = STAGE_AWAIT_INTENT
        return

    _assistant(f"IAM Identity Center bootstrap saved for profile `{profile_name}`.")
    _prepare_profile_for_analysis(
        profile_name=profile_name,
        account_scope=st.session_state.get("account_scope", "current"),
        target_account_id=st.session_state.get("target_account_id"),
    )


def _handle_idc_field_input(user_text: str) -> None:
    queue = st.session_state.get("idc_fields_queue") or []
    if not queue:
        _finalize_idc_collection_and_continue()
        return
    field = queue[0]
    value = user_text.strip()
    data = st.session_state.get("idc_input") or {}

    if field in {"sso_start_url", "sso_region"} and not value:
        _assistant("This field is required.")
        return
    if field == "sso_start_url" and value and not value.startswith("https://"):
        _assistant("Start URL should start with `https://`.")
        return
    if field == "default_account_id":
        if value and (not value.isdigit() or len(value) != 12):
            _assistant("Default account ID must be a 12-digit AWS account ID or blank.")
            return
    if field == "default_region" and not value:
        value = "eu-west-1"

    data[field] = value
    st.session_state["idc_input"] = data
    st.session_state["idc_fields_queue"] = queue[1:]

    if st.session_state["idc_fields_queue"]:
        _ask_next_idc_prompt()
        return
    _finalize_idc_collection_and_continue()


def _account_option_text(option: dict[str, Any]) -> str:
    role = option.get("role_name") or "no-role"
    return f"{option.get('account_name')} ({option.get('account_id')}) role `{role}`"


def _extract_account_id(value: str) -> str | None:
    match = re.search(r"\b([0-9]{12})\b", value or "")
    return match.group(1) if match else None


def _match_account_option(options: list[dict[str, Any]], reference: str) -> dict[str, Any] | None:
    raw = (reference or "").strip()
    if not raw:
        return None

    account_id = _extract_account_id(raw)
    if account_id:
        for option in options:
            if str(option.get("account_id") or "").strip() == account_id:
                return option

    normalized = raw.lower()
    for option in options:
        if str(option.get("account_name") or "").strip().lower() == normalized:
            return option

    partial_matches = [
        option
        for option in options
        if normalized in str(option.get("account_name") or "").strip().lower()
    ]
    if len(partial_matches) == 1:
        return partial_matches[0]
    return None


def _prompt_account_selection() -> None:
    options = st.session_state.get("account_options") or []
    if not options:
        _assistant("No selectable accounts were found for this IAM Identity Center profile.")
        st.session_state["stage"] = STAGE_AWAIT_INTENT
        return
    lines = [f"{idx}. {_account_option_text(opt)}" for idx, opt in enumerate(options, start=1)]
    _assistant("Choose an account target by number, account ID, or account name:\n\n" + "\n".join(lines))
    st.session_state["stage"] = STAGE_AWAIT_ACCOUNT_SELECTION


def _resolve_targets_for_scope(
    *,
    options: list[dict[str, Any]],
    account_scope: str,
    target_account_id: str | None,
    default_account_id: str | None,
) -> list[dict[str, Any]] | None:
    selectable = [option for option in options if option.get("role_name")]
    if not selectable:
        raise RuntimeError("No accessible accounts contain an assumable role.")

    if account_scope == "all":
        return selectable

    if account_scope == "account":
        if not target_account_id:
            st.session_state["account_options"] = selectable
            _prompt_account_selection()
            return None
        matched = _match_account_option(selectable, str(target_account_id))
        if matched is not None:
            return [matched]
        raise RuntimeError(
            f"Account `{target_account_id}` is not accessible. Use a listed account ID or account name."
        )

    preferred = (
        (target_account_id or "").strip()
        or (default_account_id or "").strip()
        or str(st.session_state.get("account_id") or "").strip()
    )
    if preferred:
        matched = _match_account_option(selectable, preferred)
        if matched is not None:
            return [matched]

    st.session_state["account_options"] = selectable
    _prompt_account_selection()
    return None


def _set_selected_targets(targets: list[dict[str, Any]]) -> None:
    st.session_state["selected_targets"] = targets
    first = targets[0] if targets else {}
    st.session_state["account_id"] = first.get("account_id")
    st.session_state["account_name"] = first.get("account_name")
    st.session_state["role_name"] = first.get("role_name")


def _sso_discovery_cache_ttl_seconds() -> int:
    raw = (os.getenv("SSO_ACCOUNT_DISCOVERY_CACHE_SECONDS", "") or "").strip()
    if not raw:
        return 300
    try:
        return max(0, int(raw))
    except Exception:
        return 300


def _cached_account_options(profile_name: str, preferred_role_name: str | None) -> list[dict[str, Any]] | None:
    cache = st.session_state.get("sso_account_cache") or {}
    key = (profile_name or "").strip().lower()
    entry = cache.get(key)
    if not isinstance(entry, dict):
        return None
    if str(entry.get("preferred_role_name") or "") != str(preferred_role_name or ""):
        return None
    if float(entry.get("expires_at_epoch", 0.0) or 0.0) < time.time():
        return None
    options = entry.get("options")
    if not isinstance(options, list):
        return None
    return list(options)


def _store_account_options_cache(profile_name: str, preferred_role_name: str | None, options: list[dict[str, Any]]) -> None:
    ttl_seconds = _sso_discovery_cache_ttl_seconds()
    if ttl_seconds <= 0:
        return
    cache = st.session_state.get("sso_account_cache")
    if not isinstance(cache, dict):
        cache = {}
    cache[(profile_name or "").strip().lower()] = {
        "preferred_role_name": str(preferred_role_name or ""),
        "expires_at_epoch": time.time() + ttl_seconds,
        "options": list(options),
    }
    st.session_state["sso_account_cache"] = cache


def _resolve_target_from_direct_account_lookup(
    *,
    profile_name: str,
    account_id: str,
    preferred_role_name: str | None,
) -> dict[str, Any] | None:
    clean = (account_id or "").strip()
    if not _is_account_id(clean):
        return None
    access_token, sso_region = load_valid_sso_access_token(profile_name)
    roles = list_account_roles(
        access_token=access_token,
        sso_region=sso_region,
        account_id=clean,
    )
    role_name = choose_role(roles, preferred_role_name=preferred_role_name)
    if not role_name:
        return None
    return {
        "account_id": clean,
        "account_name": clean,
        "role_name": role_name,
        "roles": roles,
    }


def _prepare_identity_center_profile(profile_name: str, account_scope: str, target_account_id: str | None) -> bool:
    seeded = _seed_identity_center_memory_from_config(profile_name)
    if seeded is None or not seeded.sso_start_url or not seeded.sso_region:
        _start_idc_collection(profile_name=profile_name, existing=seeded, mode="scan")
        if st.session_state.get("stage") == STAGE_AWAIT_IDC_FIELDS:
            _assistant("I need IAM Identity Center bootstrap fields before I can continue.")
        return False

    bootstrap_identity_center_profile(
        IdentityCenterBootstrapInput(
            profile_name=profile_name,
            sso_start_url=seeded.sso_start_url,
            sso_region=seeded.sso_region,
            preferred_role_name=seeded.preferred_role_name,
            default_account_id=seeded.default_account_id,
            default_region=seeded.default_region,
        )
    )

    token_status = check_sso_token_status(profile_name)
    if token_status.status != "valid":
        _await_sso_login(mode="scan", profile_name=profile_name, login_command=token_status.login_command)
        return False

    _clear_sso_wait_state()
    preferred_role_name = seeded.preferred_role_name or None
    preferred_direct_account = None
    if account_scope == "account" and _is_account_id(target_account_id):
        preferred_direct_account = str(target_account_id or "").strip()
    elif account_scope == "current":
        for candidate in (
            target_account_id,
            seeded.default_account_id,
            str(st.session_state.get("account_id") or "").strip(),
        ):
            if _is_account_id(candidate):
                preferred_direct_account = str(candidate or "").strip()
                break

    if preferred_direct_account:
        direct = _resolve_target_from_direct_account_lookup(
            profile_name=profile_name,
            account_id=preferred_direct_account,
            preferred_role_name=preferred_role_name,
        )
        if direct is not None:
            options = [direct]
            targets = _resolve_targets_for_scope(
                options=options,
                account_scope=account_scope,
                target_account_id=target_account_id,
                default_account_id=seeded.default_account_id or None,
            )
            if targets is not None:
                st.session_state["profile_auth_mode"] = "sso"
                _set_selected_targets(targets)
                return True
        elif account_scope == "account":
            raise RuntimeError(
                f"Account `{preferred_direct_account}` is not accessible. "
                "Use a listed account ID or account name."
            )

    options = _cached_account_options(profile_name, preferred_role_name)
    if options is None:
        discovered = enumerate_accessible_account_roles(
            profile_name=profile_name,
            preferred_role_name=preferred_role_name,
        )
        deduped: dict[str, dict[str, Any]] = {}
        for account in discovered:
            if account.account_id in deduped:
                continue
            deduped[account.account_id] = {
                "account_id": account.account_id,
                "account_name": account.account_name,
                "role_name": account.role_name,
                "roles": account.roles,
            }
        options = list(deduped.values())
        _store_account_options_cache(profile_name, preferred_role_name, options)

    targets = _resolve_targets_for_scope(
        options=options,
        account_scope=account_scope,
        target_account_id=target_account_id,
        default_account_id=seeded.default_account_id or None,
    )
    if targets is None:
        return False

    st.session_state["profile_auth_mode"] = "sso"
    _set_selected_targets(targets)
    return True


def _prepare_legacy_profile(profile_name: str) -> bool:
    ident = validate_profile(profile_name)
    st.session_state["profile_auth_mode"] = "legacy"
    _set_selected_targets(
        [
            {
                "account_id": ident.get("account"),
                "account_name": ident.get("account"),
                "role_name": "",
            }
        ]
    )
    return True


def _prepare_profile_for_analysis(profile_name: str, account_scope: str, target_account_id: str | None) -> None:
    clean = profile_name.strip()
    if not clean:
        st.session_state["stage"] = STAGE_AWAIT_PROFILE
        _assistant("Profile name cannot be empty. Enter an AWS profile name to continue.")
        return

    st.session_state["stage"] = STAGE_VALIDATING_PROFILE
    st.session_state["profile"] = clean
    st.session_state["account_scope"] = account_scope
    st.session_state["target_account_id"] = target_account_id

    try:
        uses_sso = profile_uses_sso(clean) or get_profile_memory(clean) is not None
        if uses_sso:
            ready = _prepare_identity_center_profile(clean, account_scope, target_account_id)
        else:
            ready = _prepare_legacy_profile(clean)
    except Exception as ex:
        st.session_state["stage"] = STAGE_AWAIT_PROFILE
        _assistant(f"Profile validation failed: {ex}")
        return

    if not ready:
        return

    _clear_sso_wait_state()
    st.session_state["project_name"] = ""
    _reset_scan_outputs()
    ctx = _chat_context()
    ctx["last_validated_profile"] = clean
    ctx["last_account_id"] = st.session_state.get("account_id")
    ctx["last_project_name"] = None
    ctx["last_scope"] = st.session_state.get("account_scope", "current")
    ctx["last_target_account_id"] = st.session_state.get("target_account_id")

    target_count = len(st.session_state.get("selected_targets") or [])
    if target_count > 1:
        _assistant(
            f"Profile validated for `{target_count}` accounts with scope `{st.session_state.get('account_scope')}`.\n"
            "Now select an existing project or provide a new project name."
        )
    else:
        _assistant(
            f"Profile validated. Account `{st.session_state.get('account_id') or 'unknown'}`.\n"
            "Now select an existing project or provide a new project name."
        )
    _prompt_project_selection()


def _handle_account_selection_input(user_text: str) -> None:
    options = st.session_state.get("account_options") or []
    if not options:
        st.session_state["stage"] = STAGE_AWAIT_INTENT
        _assistant("No account options are available. Start a new analyze request.")
        return

    value = user_text.strip()
    selected: dict[str, Any] | None = None
    if value.isdigit():
        idx = int(value)
        if 1 <= idx <= len(options):
            selected = options[idx - 1]
    if selected is None:
        selected = _match_account_option(options, value)
    if selected is None:
        _assistant("Enter a valid selection number, account ID, or account name from the list.")
        return

    st.session_state["target_account_id"] = selected.get("account_id")
    _set_selected_targets([selected])
    _assistant(f"Selected account `{selected.get('account_name')}` ({selected.get('account_id')}).")
    if not st.session_state.get("project_name"):
        _prompt_project_selection()
        return
    if _needs_cur_questions():
        _start_cur_collection(edit_mode=False)
        return
    _assistant("Running optimization scan...")
    st.session_state["stage"] = STAGE_RUNNING_SCAN


def _build_execution_targets(profile_name: str) -> list[AccountExecutionTarget]:
    selected_targets = st.session_state.get("selected_targets") or []
    if not selected_targets:
        account_id = st.session_state.get("account_id")
        return [
            AccountExecutionTarget(
                account_id=str(account_id or ""),
                account_name=str(st.session_state.get("account_name") or account_id or ""),
                role_name=str(st.session_state.get("role_name") or ""),
                aws_session=None,
            )
        ]

    auth_mode = st.session_state.get("profile_auth_mode")
    if auth_mode != "sso":
        return [
            AccountExecutionTarget(
                account_id=str(target.get("account_id") or ""),
                account_name=str(target.get("account_name") or target.get("account_id") or ""),
                role_name=str(target.get("role_name") or ""),
                aws_session=None,
            )
            for target in selected_targets
        ]

    memory = get_profile_memory(profile_name)
    default_region = (memory.default_region if memory else "").strip() or "eu-west-1"
    access_token, sso_region = load_valid_sso_access_token(profile_name)
    resolved: list[AccountExecutionTarget] = []
    for target in selected_targets:
        account_id = str(target.get("account_id") or "").strip()
        role_name = str(target.get("role_name") or "").strip()
        if not account_id or not role_name:
            continue
        role_credentials = get_role_credentials(
            access_token=access_token,
            sso_region=sso_region,
            account_id=account_id,
            role_name=role_name,
        )
        session = make_session_from_role_credentials(role_credentials, region_name=default_region)
        resolved.append(
            AccountExecutionTarget(
                account_id=account_id,
                account_name=str(target.get("account_name") or account_id),
                role_name=role_name,
                aws_session=session,
            )
        )
    return resolved


def _is_sso_error(error_text: str) -> bool:
    return _classify_run_error(error_text) == "sso_expired"


def _is_account_id(value: str | None) -> bool:
    return bool(value and value.isdigit() and len(value) == 12)


def _request_cur_account_id_collection() -> None:
    st.session_state["cur_fields_queue"] = ["athena_account_id"]
    st.session_state["cur_edit_mode"] = False
    st.session_state["stage"] = STAGE_AWAIT_CUR_FIELDS
    _assistant("I need the CUR source account ID before I can continue.")
    _ask_next_cur_prompt()


def _resolve_cur_source_account_id(cur_profile: str) -> str | None:
    explicit = str(st.session_state.get("athena_account_id") or "").strip()
    if explicit:
        if _is_account_id(explicit):
            return explicit
        raise RuntimeError("CUR preflight failed: `athena_account_id` must be a 12-digit AWS account ID.")

    seeded = _seed_identity_center_memory_from_config(cur_profile)
    default_account = (seeded.default_account_id if seeded else "").strip()
    if _is_account_id(default_account):
        st.session_state["athena_account_id"] = default_account
        return default_account

    selected_targets = st.session_state.get("selected_targets") or []
    if len(selected_targets) == 1:
        target_account = str((selected_targets[0] or {}).get("account_id") or "").strip()
        if _is_account_id(target_account):
            st.session_state["athena_account_id"] = target_account
            return target_account

    return None


def _prepare_cur_profile_preflight(_retry_after_bootstrap: bool = False) -> tuple[bool, Any | None]:
    missing = _missing_cur_fields()
    if missing:
        _start_cur_collection(edit_mode=False)
        return False, None

    cur_profile = str(st.session_state.get("athena_profile_name") or "").strip()
    if not cur_profile:
        raise RuntimeError("CUR preflight failed: `athena_profile_name` is required.")

    uses_sso = profile_uses_sso(cur_profile) or get_profile_memory(cur_profile) is not None
    if uses_sso:
        cur_account_id = _resolve_cur_source_account_id(cur_profile)
        if not cur_account_id:
            _request_cur_account_id_collection()
            return False, None

        seeded = _seed_identity_center_memory_from_config(cur_profile)
        if seeded is None or not seeded.sso_start_url or not seeded.sso_region:
            _start_idc_collection(profile_name=cur_profile, existing=seeded, mode="cur")
            if st.session_state.get("stage") == STAGE_AWAIT_IDC_FIELDS:
                _assistant("I need IAM Identity Center bootstrap fields for the CUR profile before I can continue.")
            return False, None

        bootstrap_identity_center_profile(
            IdentityCenterBootstrapInput(
                profile_name=cur_profile,
                sso_start_url=seeded.sso_start_url,
                sso_region=seeded.sso_region,
                preferred_role_name=seeded.preferred_role_name,
                default_account_id=seeded.default_account_id,
                default_region=seeded.default_region,
            )
        )
        token_status = check_sso_token_status(cur_profile)
        if token_status.status != "valid":
            _await_sso_login(mode="cur", profile_name=cur_profile, login_command=token_status.login_command)
            return False, None

        access_token, sso_region = load_valid_sso_access_token(cur_profile)
        roles = list_account_roles(
            access_token=access_token,
            sso_region=sso_region,
            account_id=cur_account_id,
        )
        selected_role = choose_role(roles, preferred_role_name=seeded.preferred_role_name or None)
        if not selected_role:
            raise RuntimeError(
                "CUR preflight failed: CUR account "
                f"`{cur_account_id}` is not accessible with an assumable role via profile `{cur_profile}`."
            )
        role_credentials = get_role_credentials(
            access_token=access_token,
            sso_region=sso_region,
            account_id=cur_account_id,
            role_name=selected_role,
        )
        default_region = (seeded.default_region or "").strip() or "eu-west-1"
        _clear_sso_wait_state()
        return True, make_session_from_role_credentials(role_credentials, region_name=default_region)

    try:
        validate_profile(cur_profile)
    except Exception as ex:
        error_text = str(ex)
        lowered = error_text.lower()
        if not uses_sso and "profile" in lowered and ("not found" in lowered or "could not be found" in lowered):
            seeded = _seed_identity_center_memory_from_config(cur_profile)
            if (
                seeded is not None
                and seeded.sso_start_url
                and seeded.sso_region
                and not _retry_after_bootstrap
            ):
                bootstrap_identity_center_profile(
                    IdentityCenterBootstrapInput(
                        profile_name=cur_profile,
                        sso_start_url=seeded.sso_start_url,
                        sso_region=seeded.sso_region,
                        preferred_role_name=seeded.preferred_role_name,
                        default_account_id=seeded.default_account_id,
                        default_region=seeded.default_region,
                    )
                )
                return _prepare_cur_profile_preflight(_retry_after_bootstrap=True)
            _start_idc_collection(profile_name=cur_profile, existing=seeded, mode="cur")
            if st.session_state.get("stage") == STAGE_AWAIT_IDC_FIELDS:
                _assistant("I need IAM Identity Center bootstrap fields for the CUR profile before I can continue.")
            return False, None
        if uses_sso and _is_sso_error(error_text):
            _await_sso_login(
                mode="cur",
                profile_name=cur_profile,
                login_command=f"aws sso login --profile {cur_profile}",
            )
            return False, None
        raise RuntimeError(f"CUR preflight failed for profile `{cur_profile}`: {ex}") from ex

    _clear_sso_wait_state()
    return True, make_boto3_session(cur_profile)


def _run_scan(progress_writer: Callable[[str], None] | None = None) -> None:
    profile_name = st.session_state.get("profile") or ""
    if not profile_name:
        _assistant("Please start with an analysis request and validate an AWS profile in chat.")
        st.session_state["stage"] = STAGE_AWAIT_PROFILE
        return

    ctx = _chat_context()
    ctx["last_run_request"] = _snapshot_run_request()
    ctx["last_run_status"] = "running"
    ctx["last_run_error"] = None
    ctx["last_error_type"] = None
    ctx["last_validated_profile"] = profile_name
    ctx["last_account_id"] = st.session_state.get("account_id")
    ctx["last_project_name"] = st.session_state.get("project_name") or None
    ctx["last_scope"] = st.session_state.get("account_scope", "current")
    ctx["last_target_account_id"] = st.session_state.get("target_account_id")

    def _progress_callback(step: str, state: str, duration: float | None) -> None:
        if progress_writer is None:
            return
        label = STEP_LABELS.get(step, step)
        if state == "start":
            progress_writer(f"Running `{label}`...")
            return
        if state == "done":
            if duration is None:
                progress_writer(f"Completed `{label}`.")
            else:
                progress_writer(f"Completed `{label}` in `{duration:.1f}s`.")

    try:
        _assistant("Scan profile ready.")
        cur_ready, cur_session = _prepare_cur_profile_preflight()
        if not cur_ready:
            ctx["last_run_status"] = "pending"
            if st.session_state.get("stage") == STAGE_RUNNING_SCAN:
                st.session_state["stage"] = STAGE_AWAIT_INTENT
                _assistant(
                    "Run paused but no input prompt could be shown. "
                    "Send `/retry` to continue, or use `/athena edit` to update CUR settings."
                )
            return
        st.session_state["cur_session"] = cur_session
        _assistant("CUR profile ready.")

        targets = _build_execution_targets(profile_name)
        if not targets:
            raise RuntimeError("No account targets are available to run this scan.")

        if progress_writer and len(targets) > 1:
            progress_writer(f"Running scan sequentially across `{len(targets)}` accounts.")

        def _build_context_for_target(target: AccountExecutionTarget) -> RunContext:
            def _callback(step: str, state: str, duration: float | None) -> None:
                if progress_writer is None:
                    return
                prefix = f"[{target.account_id}] "
                label = STEP_LABELS.get(step, step)
                if state == "start":
                    progress_writer(prefix + f"Running `{label}`...")
                elif state == "done":
                    if duration is None:
                        progress_writer(prefix + f"Completed `{label}`.")
                    else:
                        progress_writer(prefix + f"Completed `{label}` in `{duration:.1f}s`.")

            return _build_context(
                account_id=target.account_id,
                account_name=target.account_name,
                role_name=target.role_name,
                aws_session=target.aws_session,
                cur_session=cur_session,
                progress_callback=_callback if progress_writer is not None else _progress_callback,
            )

        result = run_scan_for_targets(
            action_id=ACTION_ID,
            profile_name=profile_name,
            days=30,
            output="excel",
            targets=targets,
            build_context_for_target=_build_context_for_target,
        )
    except Exception as ex:
        error_text = str(ex)
        ctx["last_run_status"] = "failed"
        ctx["last_run_error"] = error_text
        ctx["last_error_type"] = _classify_run_error(error_text)
        if error_text.startswith("CUR preflight failed"):
            _assistant(f"CUR preflight failed: {error_text.replace('CUR preflight failed: ', '', 1)}")
        elif error_text.startswith("Account scan failed") or error_text.startswith("All account scans failed"):
            _assistant(f"Run failed: {error_text}")
        else:
            _assistant(f"Scan stage failed: {error_text}")
        st.session_state["stage"] = STAGE_AWAIT_INTENT
        return

    st.session_state["recommendations"] = result.recommendations
    st.session_state["run_diagnostics"] = result.diagnostics
    st.session_state["run_sql"] = result.sql_used
    st.session_state["run_warnings"] = result.warnings
    st.session_state["run_cur_cost_lines"] = result.cur_cost_lines

    multi = (result.diagnostics or {}).get("multi_account") or {}
    success_count = int(multi.get("success_count") or 0)
    failure_count = int(multi.get("failure_count") or 0)
    target_count = int(multi.get("target_count") or len(st.session_state.get("selected_targets") or []) or 1)

    completion_note = f"across `{target_count}` account target(s)"
    if target_count > 1:
        completion_note += f" (`{success_count}` succeeded, `{failure_count}` failed)"

    _assistant(
        "Scan complete for project "
        f"`{st.session_state.get('project_name') or 'unspecified'}`. "
        f"Found {len(result.recommendations)} NAT recommendation(s) {completion_note}."
    )
    if result.warnings:
        warning_lines = "\n".join(f"- {line}" for line in result.warnings[:3])
        _assistant("Run warnings:\n" + warning_lines)
    ctx["last_run_status"] = "success"
    ctx["last_run_error"] = None
    ctx["last_error_type"] = None
    ctx["last_project_name"] = st.session_state.get("project_name") or None

    _persist_project_memory()
    st.session_state["stage"] = STAGE_READY_WITH_RESULTS


def _start_cur_collection(edit_mode: bool) -> None:
    force_recollect = bool(st.session_state.get("force_cur_recollect"))
    if edit_mode or force_recollect:
        queue = CUR_FIELDS_ALL.copy()
    else:
        queue = [field for field in CUR_FIELDS_REQUIRED if not str(st.session_state.get(field, "")).strip()]
        if queue and "athena_region" not in queue:
            queue.append("athena_region")

    st.session_state["cur_fields_queue"] = queue
    st.session_state["cur_edit_mode"] = edit_mode

    if not queue:
        _assistant("Running optimization scan...")
        st.session_state["stage"] = STAGE_RUNNING_SCAN
        return

    st.session_state["stage"] = STAGE_AWAIT_CUR_FIELDS
    _ask_next_cur_prompt()


def _ask_next_cur_prompt() -> None:
    queue = st.session_state.get("cur_fields_queue", [])
    if not queue:
        st.session_state["force_cur_recollect"] = False
        _persist_project_memory()
        _assistant("CUR config saved. Running optimization scan...")
        st.session_state["stage"] = STAGE_RUNNING_SCAN
        return

    field = queue[0]
    prompt = CUR_FIELD_PROMPTS[field]
    if st.session_state.get("cur_edit_mode"):
        current = str(st.session_state.get(field, "")).strip()
        if field == "athena_region" and not current:
            current = "us-east-1"
        prompt = f"{prompt} Current: `{current or '(empty)'}`. Type `-` to keep current."

    _assistant(prompt)


def _parse_project_selection(text: str) -> str:
    candidate = text.strip()
    if not candidate:
        return ""

    options = st.session_state.get("project_options") or []
    if candidate.isdigit():
        idx = int(candidate)
        if 1 <= idx <= len(options):
            return options[idx - 1]

    for name in options:
        if name.lower() == candidate.lower():
            return name

    return candidate


def _prompt_project_selection() -> None:
    st.session_state["project_options"] = list_projects()
    options = st.session_state["project_options"]
    if options:
        lines = [f"{idx}. `{name}`" for idx, name in enumerate(options, start=1)]
        _assistant(
            "Choose a project for this analysis. Reply with the number or type a new project name.\n\n"
            + "\n".join(lines)
        )
    else:
        _assistant("No saved projects found. Type a new project name to create memory for this account.")
    st.session_state["stage"] = STAGE_AWAIT_PROJECT_SELECTION


def _select_project(project_name: str) -> None:
    clean_name = project_name.strip()
    if not clean_name:
        _assistant("Project name cannot be empty. Reply with a project number or project name.")
        return

    existing = get_project(clean_name)
    if existing:
        st.session_state["project_name"] = existing.project_name
        st.session_state["athena_database"] = existing.athena_database
        st.session_state["athena_table"] = existing.athena_table
        st.session_state["athena_workgroup"] = existing.athena_workgroup or "primary"
        st.session_state["athena_output_s3"] = existing.athena_output_s3
        st.session_state["athena_profile_name"] = existing.athena_profile_name
        st.session_state["athena_account_id"] = existing.athena_account_id
        st.session_state["athena_region"] = existing.athena_region or "us-east-1"
        st.session_state["cur_skipped"] = False
        st.session_state["force_cur_recollect"] = bool(existing.cur_skipped)
        _assistant(f"Loaded saved project `{existing.project_name}`.")
        if existing.cur_skipped:
            _assistant("This project was saved with CUR skipped previously. CUR setup is now mandatory; please re-enter CUR fields.")
    else:
        st.session_state["project_name"] = clean_name
        _reset_project_settings()
        _assistant(f"Created new project `{clean_name}`.")

    ctx = _chat_context()
    ctx["last_project_name"] = st.session_state.get("project_name")

    _persist_project_memory()

    if not st.session_state.get("selected_targets") and st.session_state.get("profile"):
        _prepare_profile_for_analysis(
            profile_name=st.session_state.get("profile", ""),
            account_scope=st.session_state.get("account_scope", "current"),
            target_account_id=st.session_state.get("target_account_id"),
        )
        if st.session_state.get("stage") in {STAGE_AWAIT_IDC_FIELDS, STAGE_AWAIT_SSO_LOGIN, STAGE_AWAIT_ACCOUNT_SELECTION}:
            return

    if _needs_cur_questions():
        _start_cur_collection(edit_mode=False)
        return

    _assistant("Running optimization scan...")
    st.session_state["stage"] = STAGE_RUNNING_SCAN


def _validate_and_set_profile(
    profile_name: str,
    account_scope: str = "current",
    target_account_id: str | None = None,
) -> None:
    _prepare_profile_for_analysis(
        profile_name=profile_name,
        account_scope=account_scope,
        target_account_id=target_account_id,
    )


def _resume_after_sso_wait() -> None:
    wait_mode = str(st.session_state.get("sso_wait_mode") or "scan").strip().lower()
    if wait_mode == "cur":
        if not st.session_state.get("project_name"):
            _prompt_project_selection()
            return
        _assistant("Retrying CUR preflight after SSO login...")
        _reset_scan_outputs()
        st.session_state["stage"] = STAGE_RUNNING_SCAN
        return

    _prepare_profile_for_analysis(
        profile_name=st.session_state.get("profile", ""),
        account_scope=st.session_state.get("account_scope", "current"),
        target_account_id=st.session_state.get("target_account_id"),
    )


def _handle_rescan() -> None:
    if not st.session_state.get("profile"):
        st.session_state["stage"] = STAGE_AWAIT_PROFILE
        _assistant("Start with an analysis request and profile validation first.")
        return
    ctx = _chat_context()
    if st.session_state.get("stage") == STAGE_AWAIT_SSO_LOGIN:
        _resume_after_sso_wait()
        return
    scope_changed = (
        st.session_state.get("account_scope") != ctx.get("last_scope")
        or st.session_state.get("target_account_id") != ctx.get("last_target_account_id")
    )
    if scope_changed or not st.session_state.get("selected_targets"):
        _prepare_profile_for_analysis(
            profile_name=st.session_state.get("profile", ""),
            account_scope=st.session_state.get("account_scope", "current"),
            target_account_id=st.session_state.get("target_account_id"),
        )
        if st.session_state.get("stage") in {STAGE_AWAIT_IDC_FIELDS, STAGE_AWAIT_SSO_LOGIN, STAGE_AWAIT_ACCOUNT_SELECTION}:
            return
    if not st.session_state.get("project_name"):
        _prompt_project_selection()
        return
    if _needs_cur_questions():
        _start_cur_collection(edit_mode=False)
        return

    _reset_scan_outputs()
    _assistant("Running optimization scan...")
    st.session_state["stage"] = STAGE_RUNNING_SCAN


def _handle_cur_field_input(user_text: str) -> None:
    queue = st.session_state.get("cur_fields_queue", [])
    if not queue:
        _ask_next_cur_prompt()
        return

    value = user_text.strip()
    field = queue[0]
    edit_mode = bool(st.session_state.get("cur_edit_mode"))

    if edit_mode and value == "-":
        st.session_state["cur_fields_queue"] = queue[1:]
        _ask_next_cur_prompt()
        return

    if edit_mode and field in {"athena_profile_name", "athena_account_id", "athena_region"} and value == "-":
        st.session_state["cur_fields_queue"] = queue[1:]
        _ask_next_cur_prompt()
        return

    if field in {"athena_database", "athena_table"}:
        if not value:
            _assistant("This field cannot be empty.")
            return
        st.session_state[field] = value
    elif field == "athena_workgroup":
        st.session_state[field] = value or "primary"
    elif field == "athena_output_s3":
        if not value.startswith("s3://"):
            _assistant("Athena output must start with `s3://`.")
            return
        st.session_state[field] = value
    elif field == "athena_profile_name":
        if not value:
            _assistant("CUR query profile name is required.")
            return
        if value.lower() == "same":
            _assistant("Enter the explicit CUR profile name. CUR profile is mandatory and not auto-inferred.")
            return
        st.session_state[field] = value
    elif field == "athena_account_id":
        clean = value.strip()
        if not clean:
            st.session_state[field] = ""
        elif clean.isdigit() and len(clean) == 12:
            st.session_state[field] = clean
        else:
            _assistant("CUR source account ID must be a 12-digit AWS account ID.")
            return
    elif field == "athena_region":
        st.session_state[field] = value or "us-east-1"

    st.session_state["cur_fields_queue"] = queue[1:]
    _ask_next_cur_prompt()


def _help_text() -> str:
    return (
        "Available commands:\n"
        "- `/analyze profile=<aws-profile> scope=current|all|account:<id>`\n"
        "- `/project <project-name>`\n"
        "- `/athena edit`\n"
        "- `/rescan`\n"
        "- `/retry`\n"
        "- `/help`\n\n"
        "Natural chat also works, for example: "
        "`analyze idle nat gateway savings for all accounts with profile finops-sso` or `try again`."
    )


def _handle_analyze_intent(
    profile_name: str | None,
    target_service: str | None = None,
    account_scope: str = "current",
    target_account_id: str | None = None,
) -> None:
    ctx = _chat_context()
    st.session_state["account_scope"] = account_scope
    st.session_state["target_account_id"] = target_account_id

    if target_service:
        ctx["last_requested_service"] = target_service
        if target_service not in SUPPORTED_SERVICES:
            _assistant(
                f"`{target_service.upper()}` analysis is not available yet. "
                "I'll run NAT optimization with your current account context."
            )

    if profile_name:
        _validate_and_set_profile(
            profile_name=profile_name,
            account_scope=account_scope,
            target_account_id=target_account_id,
        )
        return

    if not st.session_state.get("profile"):
        remembered_profile = str(ctx.get("last_validated_profile") or "").strip()
        if remembered_profile:
            st.session_state["profile"] = remembered_profile
            st.session_state["account_id"] = ctx.get("last_account_id")
            _assistant(
                f"Using your session profile `{remembered_profile}` "
                f"for account `{st.session_state.get('account_id') or 'unknown'}`."
            )

    if not st.session_state.get("profile"):
        st.session_state["stage"] = STAGE_AWAIT_PROFILE
        _assistant("Which AWS SSO profile should I use for this analysis?")
        return

    if not st.session_state.get("selected_targets"):
        _prepare_profile_for_analysis(
            profile_name=st.session_state.get("profile", ""),
            account_scope=st.session_state.get("account_scope", "current"),
            target_account_id=st.session_state.get("target_account_id"),
        )
        if st.session_state.get("stage") in {STAGE_AWAIT_IDC_FIELDS, STAGE_AWAIT_SSO_LOGIN, STAGE_AWAIT_ACCOUNT_SELECTION}:
            return

    if not st.session_state.get("project_name"):
        remembered_project = str(ctx.get("last_project_name") or "").strip()
        if remembered_project:
            _assistant(f"Using your last project `{remembered_project}`.")
            _select_project(remembered_project)
            return

    if st.session_state.get("project_name"):
        if _needs_cur_questions():
            _start_cur_collection(edit_mode=False)
            return
        _assistant("Using current session context. Running optimization scan...")
        _reset_scan_outputs()
        st.session_state["stage"] = STAGE_RUNNING_SCAN
        return

    if st.session_state.get("profile"):
        _assistant(f"Using validated profile `{st.session_state['profile']}`. Select a project for this analysis.")
        _prompt_project_selection()
        return

    st.session_state["stage"] = STAGE_AWAIT_PROFILE
    _assistant("Which AWS SSO profile should I use for this analysis?")


def _handle_retry_intent() -> None:
    if st.session_state.get("stage") == STAGE_AWAIT_SSO_LOGIN and st.session_state.get("profile"):
        _resume_after_sso_wait()
        return

    ctx = _chat_context()
    last_status = ctx.get("last_run_status")
    last_request = ctx.get("last_run_request")

    if last_status != "failed" or not isinstance(last_request, dict):
        _assistant("There is no failed run to retry. Start a new analysis or use `/rescan`.")
        return

    _restore_run_request(last_request)
    _reset_scan_outputs()
    _assistant("Retrying the last failed analysis with the same session context.")

    if not st.session_state.get("profile"):
        st.session_state["stage"] = STAGE_AWAIT_PROFILE
        _assistant("I need an AWS profile to retry. Enter the profile name.")
        return
    if not st.session_state.get("project_name"):
        _prompt_project_selection()
        return
    if _needs_cur_questions():
        _start_cur_collection(edit_mode=False)
        return

    st.session_state["stage"] = STAGE_RUNNING_SCAN


def handle_user_input(user_text: str) -> None:
    _append_message("user", user_text)
    text = user_text.strip()
    if not text:
        _assistant("Enter a message to continue.")
        return

    stage = st.session_state.get("stage", STAGE_AWAIT_INTENT)
    intent = None
    if not text.startswith("/"):
        if stage == STAGE_AWAIT_PROFILE:
            inferred = route(text)
            if inferred.intent == "analyze":
                _handle_analyze_intent(
                    inferred.profile_name,
                    inferred.target_service,
                    inferred.account_scope,
                    inferred.target_account_id,
                )
                return
            if inferred.intent in {"help", "retry", "rescan"}:
                intent = inferred
            else:
                _validate_and_set_profile(
                    profile_name=text,
                    account_scope=st.session_state.get("account_scope", "current"),
                    target_account_id=st.session_state.get("target_account_id"),
                )
                return
        if stage == STAGE_AWAIT_IDC_FIELDS:
            _handle_idc_field_input(text)
            return
        if stage == STAGE_AWAIT_SSO_LOGIN:
            _resume_after_sso_wait()
            return
        if stage == STAGE_AWAIT_PROJECT_SELECTION:
            _select_project(_parse_project_selection(text))
            return
        if stage == STAGE_AWAIT_ACCOUNT_SELECTION:
            _handle_account_selection_input(text)
            return
        if stage == STAGE_AWAIT_CUR_FIELDS:
            _handle_cur_field_input(text)
            return

    if intent is None:
        intent = route(text)

    if intent.intent == "help":
        _assistant(_help_text())
        return
    if intent.intent == "rescan":
        _handle_rescan()
        return
    if intent.intent == "retry":
        _handle_retry_intent()
        return
    if intent.intent == "update_athena":
        if not st.session_state.get("project_name"):
            _assistant("Select a project first with `/project <name>`.")
            st.session_state["stage"] = STAGE_AWAIT_PROJECT_SELECTION
            _prompt_project_selection()
            return
        _assistant(
            f"Editing Athena settings for `{st.session_state['project_name']}`. "
            "Type `-` to keep a field as-is."
        )
        st.session_state["cur_skipped"] = False
        _start_cur_collection(edit_mode=True)
        return
    if intent.intent == "set_project":
        if not st.session_state.get("profile"):
            st.session_state["stage"] = STAGE_AWAIT_PROFILE
            _assistant("Validate profile first. Example: `/analyze profile=my-sso-profile`.")
            return
        if intent.project_name:
            _select_project(intent.project_name)
            return
        _prompt_project_selection()
        return
    if intent.intent == "analyze":
        _handle_analyze_intent(
            intent.profile_name,
            intent.target_service,
            intent.account_scope,
            intent.target_account_id,
        )
        return

    _assistant("I can help run optimization scans. Ask to analyze an account with a profile, or use `/help`.")


def _chat_placeholder() -> str:
    stage = st.session_state.get("stage", STAGE_AWAIT_INTENT)
    if stage == STAGE_AWAIT_PROFILE:
        return "Enter AWS SSO profile name..."
    if stage == STAGE_AWAIT_IDC_FIELDS:
        return "Enter IAM Identity Center bootstrap value..."
    if stage == STAGE_AWAIT_SSO_LOGIN:
        wait_profile = st.session_state.get("sso_wait_profile")
        if wait_profile:
            return f"After running aws sso login --profile {wait_profile}, type retry..."
        return "After running aws sso login, type retry or any message..."
    if stage == STAGE_AWAIT_PROJECT_SELECTION:
        return "Type project number or project name..."
    if stage == STAGE_AWAIT_ACCOUNT_SELECTION:
        return "Type account number, account ID, or account name..."
    if stage == STAGE_AWAIT_CUR_FIELDS:
        return "Enter Athena detail..."
    if stage == STAGE_RUNNING_SCAN:
        return "Scan running..."
    return "Ask me to analyze optimization opportunities..."


def _status_value(value: str | None, fallback: str = "Not set") -> str:
    clean = (value or "").strip()
    return clean if clean else fallback


def _render_status() -> None:
    project = _status_value(st.session_state.get("project_name"), "Not selected")
    profile = _status_value(st.session_state.get("profile"), "Not validated")
    selected_targets = st.session_state.get("selected_targets") or []
    if len(selected_targets) > 1:
        account = f"Multiple ({len(selected_targets)})"
    else:
        account = _status_value(st.session_state.get("account_id"), "Unknown")

    if _needs_cur_questions():
        cur_status = "Needs Athena Inputs"
    else:
        cur_status = "Ready"

    st.markdown(
        f"""
        <div class="status-grid">
            <div class="status-card"><div class="status-label">Project</div><div class="status-value">{project}</div></div>
            <div class="status-card"><div class="status-label">AWS Profile</div><div class="status-value">{profile}</div></div>
            <div class="status-card"><div class="status-label">Account Target</div><div class="status-value">{account}</div></div>
            <div class="status-card"><div class="status-label">CUR Setup</div><div class="status-value">{cur_status}</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


st.set_page_config(page_title="CostOps Copilot", layout="wide", initial_sidebar_state="collapsed")
_apply_theme()
_init_state()

st.markdown(
    """
    <div class="title-wrap">
      <p class="title-main">CostOps Copilot</p>
    </div>
    """,
    unsafe_allow_html=True,
)

_render_status()

user_text = st.chat_input(_chat_placeholder())
if user_text is not None and user_text.strip():
    handle_user_input(user_text)
    st.rerun()

if st.session_state.get("stage") == STAGE_RUNNING_SCAN:
    with st.status("Running optimization scan...", expanded=True) as status:
        _run_scan(progress_writer=status.write)
        next_stage = st.session_state.get("stage")
        if next_stage == STAGE_READY_WITH_RESULTS:
            status.update(label="Optimization scan complete", state="complete")
        elif next_stage in {
            STAGE_AWAIT_CUR_FIELDS,
            STAGE_AWAIT_SSO_LOGIN,
            STAGE_AWAIT_IDC_FIELDS,
            STAGE_AWAIT_PROJECT_SELECTION,
            STAGE_AWAIT_ACCOUNT_SELECTION,
        }:
            status.update(label="Optimization scan paused - action required", state="running")
        else:
            status.update(label="Optimization scan failed", state="error")
    st.rerun()

st.markdown("### Chat")
for message in st.session_state["messages"]:
    st.chat_message(message["role"]).write(message["content"])

recommendations = st.session_state.get("recommendations") or []

if recommendations:
    st.markdown("### NAT Recommendations")
    st.dataframe(pd.DataFrame(recommendations_to_rows(recommendations)), use_container_width=True)

elif st.session_state.get("stage") == STAGE_READY_WITH_RESULTS:
    st.info("Scan completed with no NAT recommendations.")

if st.session_state.get("stage") == STAGE_READY_WITH_RESULTS:
    if st.button("Create Excel Recommendations"):
        selected_targets = st.session_state.get("selected_targets") or []
        if len(selected_targets) > 1:
            account_id = "multi"
        else:
            account_id = st.session_state.get("account_id") or "unknown"
        out_file = f"finops_report_{account_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
        out_path = write_excel(
            recommendations=recommendations,
            out_path=out_file,
            account_id=account_id,
            sql_used=st.session_state.get("run_sql"),
            cur_cost_lines=st.session_state.get("run_cur_cost_lines") or [],
            warnings=st.session_state.get("run_warnings") or [],
            diagnostics=st.session_state.get("run_diagnostics") or {},
        )
        st.session_state["latest_report_path"] = out_path
        st.session_state["latest_report_name"] = out_file
        _assistant(f"Prepared Excel with {len(recommendations)} recommendation row(s).")
        _persist_project_memory()
        st.rerun()

report_path = st.session_state.get("latest_report_path")
report_name = st.session_state.get("latest_report_name")
if report_path and report_name and Path(report_path).exists():
    with open(report_path, "rb") as f:
        st.download_button("Download Excel report", f, file_name=report_name)
