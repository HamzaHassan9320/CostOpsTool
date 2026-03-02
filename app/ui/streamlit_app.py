from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# Ensure `app.*` imports work even when Streamlit sets CWD/script path differently.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.auth.validation import validate_profile
from app.core.registry import run_action
from app.core.types import ActionRequest, Finding, RunContext
from app.outputs.excel_writer import write_excel
from app.outputs.report_builder import findings_to_rows

# IMPORTANT: ensure plugins register
import app.services.aws_config.plugin  # noqa: F401

STEP_VALIDATE_PROFILE = "validate_profile"
STEP_ASK_CUR_DATABASE = "ask_cur_database"
STEP_ASK_CUR_TABLE = "ask_cur_table"
STEP_ASK_CUR_WORKGROUP = "ask_cur_workgroup"
STEP_ASK_CUR_OUTPUT_S3 = "ask_cur_output_s3"
STEP_ASK_CUR_PROFILE = "ask_cur_profile"
STEP_ASK_CUR_REGION = "ask_cur_region"
STEP_ASK_EDP = "ask_edp"
STEP_RUN_SCAN = "run_scan"
STEP_SHOW_OPPORTUNITIES = "show_opportunities"
STEP_CHOOSE_FOR_EXCEL = "choose_for_excel"
STEP_DOWNLOAD = "download"

DEFAULT_ACTION = "aws_config.savings_scan"
ACTION_REQUIREMENTS = {
    DEFAULT_ACTION: {"needs_cur": True},
}


def _append_message(role: str, content: str) -> None:
    st.session_state["messages"].append({"role": role, "content": content})


def _init_state() -> None:
    defaults = {
        "messages": [],
        "wizard_step": STEP_VALIDATE_PROFILE,
        "account_id": None,
        "profile": "",
        "active_action": DEFAULT_ACTION,
        "edp_percent": None,
        "athena_database": os.getenv("ATHENA_DATABASE", ""),
        "athena_table": os.getenv("ATHENA_TABLE", ""),
        "athena_workgroup": os.getenv("ATHENA_WORKGROUP", "primary"),
        "athena_output_s3": os.getenv("ATHENA_OUTPUT_S3", ""),
        "athena_profile_name": os.getenv("ATHENA_PROFILE_NAME", ""),
        "athena_region": os.getenv("ATHENA_REGION", "us-east-1"),
        "cur_skipped": False,
        "scan_findings": [],
        "region_cost_rows": [],
        "cur_error": None,
        "cur_warning": None,
        "cur_query_profile": None,
        "cur_query_region": None,
        "selected_optimization_ids": [],
        "latest_report_path": None,
        "latest_report_name": None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _validate_edp(text: str) -> float | None:
    try:
        value = float(text.strip())
    except Exception:
        return None
    if value < 0 or value > 100:
        return None
    return value


def _active_action() -> str:
    return st.session_state.get("active_action", DEFAULT_ACTION)


def _action_needs_cur() -> bool:
    action = _active_action()
    req = ACTION_REQUIREMENTS.get(action, {})
    return bool(req.get("needs_cur", False))


def _missing_cur_fields() -> list[str]:
    missing = []
    if not st.session_state.get("athena_database", "").strip():
        missing.append("athena_database")
    if not st.session_state.get("athena_table", "").strip():
        missing.append("athena_table")
    if not st.session_state.get("athena_workgroup", "").strip():
        missing.append("athena_workgroup")
    if not st.session_state.get("athena_output_s3", "").strip():
        missing.append("athena_output_s3")
    return missing


def _needs_cur_questions() -> bool:
    if not _action_needs_cur():
        return False
    if st.session_state.get("cur_skipped"):
        return False
    return bool(_missing_cur_fields())


def _set_next_step_after_profile() -> None:
    if _needs_cur_questions():
        st.session_state["wizard_step"] = STEP_ASK_CUR_DATABASE
        _append_message(
            "assistant",
            "This optimization needs CUR pricing. Enter Athena CUR database name, or type 'skip'.",
        )
    else:
        st.session_state["wizard_step"] = STEP_ASK_EDP
        _append_message("assistant", "Enter your EDP percentage (0-100).")


def _summary_finding(findings: list[Finding]) -> Finding | None:
    for f in findings:
        if f.optimization_id == "aws_config.inventory_summary":
            return f
    return None


def _evidence_map(finding: Finding | None) -> dict:
    if finding is None:
        return {}
    return {e.key: e.value for e in finding.evidence}


def _build_context(account_id: str | None) -> RunContext:
    return RunContext(
        profile_name=st.session_state.get("profile", ""),
        account_id=account_id,
        days=30,
        regions=[],
        edp_percent=float(st.session_state.get("edp_percent") or 0.0),
        athena_database=st.session_state.get("athena_database", "").strip(),
        athena_table=st.session_state.get("athena_table", "").strip(),
        athena_workgroup=st.session_state.get("athena_workgroup", "").strip(),
        athena_output_s3=st.session_state.get("athena_output_s3", "").strip(),
        athena_profile_name=(st.session_state.get("athena_profile_name", "").strip() or None),
        athena_region=st.session_state.get("athena_region", "us-east-1").strip() or "us-east-1",
        requested_by=None,
    )


def _run_scan() -> None:
    account_id = st.session_state.get("account_id")
    profile_name = st.session_state.get("profile") or ""
    action = _active_action()
    if not profile_name:
        _append_message("assistant", "Please validate an AWS SSO profile first in the sidebar.")
        st.session_state["wizard_step"] = STEP_VALIDATE_PROFILE
        return

    req = ActionRequest(action=action, profile_name=profile_name, days=30, regions=None, output="excel")

    try:
        findings = run_action(req, lambda r: _build_context(account_id))
    except Exception as ex:
        _append_message("assistant", f"Run failed: {ex}")
        st.session_state["wizard_step"] = STEP_ASK_EDP
        return

    st.session_state["scan_findings"] = findings
    summary = _summary_finding(findings)
    summary_evidence = _evidence_map(summary)
    st.session_state["region_cost_rows"] = summary_evidence.get("region_cost_inputs") or []
    st.session_state["cur_error"] = summary_evidence.get("cur_error")
    st.session_state["cur_warning"] = summary_evidence.get("cur_warning")
    st.session_state["cur_query_profile"] = summary_evidence.get("cur_query_profile")
    st.session_state["cur_query_region"] = summary_evidence.get("cur_query_region")

    opportunities = [f for f in findings if f.optimization_id != "aws_config.inventory_summary"]
    unique_opt_ids = sorted({f.optimization_id for f in opportunities})
    st.session_state["selected_optimization_ids"] = unique_opt_ids
    if st.session_state["cur_error"]:
        _append_message(
            "assistant",
            "Scan completed, but CUR pricing data was unavailable for savings in some rows. "
            "You can still review all optimization opportunities.",
        )
    elif st.session_state["cur_warning"]:
        _append_message(
            "assistant",
            f"CUR warning: {st.session_state['cur_warning']} Pricing estimates may be partial.",
        )
    elif not st.session_state["region_cost_rows"]:
        _append_message(
            "assistant",
            "CUR returned zero region rows, so savings fields may be null. "
            "Verify Athena database/table/account and data freshness. "
            "If CUR lives in management account, set CUR profile to that payer profile and region to us-east-1.",
        )
    _append_message(
        "assistant",
        f"Scan complete. Found {len(opportunities)} optimization opportunities. "
        "Select optimization(s) below to create Excel.",
    )
    st.session_state["wizard_step"] = STEP_CHOOSE_FOR_EXCEL


st.set_page_config(page_title="FinOps Copilot", layout="wide")
st.title("FinOps Copilot")
_init_state()

if not st.session_state["messages"]:
    _append_message("assistant", "Validate an AWS SSO profile in the sidebar to begin.")

with st.sidebar:
    st.header("AWS Connection (SSO)")
    profile = st.text_input("AWS profile name", value=st.session_state.get("profile", ""))
    st.session_state["profile"] = profile
    if st.button("Validate"):
        try:
            ident = validate_profile(profile)
            st.success(f"Connected: {ident['account']} | {ident['arn']}")
            st.session_state["account_id"] = ident["account"]
            st.session_state["active_action"] = DEFAULT_ACTION
            st.session_state["scan_findings"] = []
            st.session_state["latest_report_path"] = None
            st.session_state["latest_report_name"] = None
            st.session_state["cur_skipped"] = False
            _append_message("assistant", "Profile validated for AWS Config savings scan.")
            _set_next_step_after_profile()
        except Exception as ex:
            st.error(str(ex))

st.markdown("### Chat")
for m in st.session_state["messages"]:
    st.chat_message(m["role"]).write(m["content"])

prompt_map = {
    STEP_VALIDATE_PROFILE: "Validate AWS profile in sidebar first.",
    STEP_ASK_CUR_DATABASE: "Enter Athena CUR database (or type 'skip').",
    STEP_ASK_CUR_TABLE: "Enter Athena CUR table name.",
    STEP_ASK_CUR_WORKGROUP: "Enter Athena workgroup (or press Enter for 'primary').",
    STEP_ASK_CUR_OUTPUT_S3: "Enter Athena results S3 location (s3://...).",
    STEP_ASK_CUR_PROFILE: "Enter CUR query profile name (or type 'same').",
    STEP_ASK_CUR_REGION: "Enter CUR Athena region (default us-east-1).",
    STEP_ASK_EDP: "Enter EDP percent (0-100). Example: 12",
    STEP_RUN_SCAN: "Running scan...",
    STEP_SHOW_OPPORTUNITIES: "Type 'rescan' to run again, or select optimizations for Excel below.",
    STEP_CHOOSE_FOR_EXCEL: "Select optimization(s) for Excel below, or type 'rescan' to rerun.",
    STEP_DOWNLOAD: "Download your Excel below, or type 'rescan' to rerun.",
}
user_text = st.chat_input(prompt_map.get(st.session_state["wizard_step"], "Enter command"))

if user_text:
    _append_message("user", user_text)
    text = user_text.strip()
    lower = text.lower()

    if lower in {"rescan", "rerun"}:
        if _needs_cur_questions():
            st.session_state["wizard_step"] = STEP_ASK_CUR_DATABASE
            _append_message("assistant", "This optimization needs CUR pricing. Enter Athena CUR database, or type 'skip'.")
        elif st.session_state.get("edp_percent") is None:
            _append_message("assistant", "Please enter EDP percentage first.")
            st.session_state["wizard_step"] = STEP_ASK_EDP
        else:
            _append_message("assistant", "Running AWS Config optimization scan...")
            st.session_state["wizard_step"] = STEP_RUN_SCAN
    elif st.session_state["wizard_step"] == STEP_VALIDATE_PROFILE:
        _append_message("assistant", "Please validate an AWS SSO profile first in the sidebar.")
    elif st.session_state["wizard_step"] == STEP_ASK_CUR_DATABASE:
        if lower == "skip":
            st.session_state["cur_skipped"] = True
            st.session_state["athena_database"] = ""
            st.session_state["athena_table"] = ""
            st.session_state["athena_workgroup"] = ""
            st.session_state["athena_output_s3"] = ""
            st.session_state["athena_profile_name"] = ""
            st.session_state["athena_region"] = "us-east-1"
            st.session_state["wizard_step"] = STEP_ASK_EDP
            _append_message("assistant", "CUR input skipped. Savings may be null. Enter EDP percentage (0-100).")
        elif not text:
            _append_message("assistant", "Athena database cannot be empty. Enter database, or type 'skip'.")
        else:
            st.session_state["cur_skipped"] = False
            st.session_state["athena_database"] = text
            st.session_state["wizard_step"] = STEP_ASK_CUR_TABLE
            _append_message("assistant", "Enter Athena CUR table name.")
    elif st.session_state["wizard_step"] == STEP_ASK_CUR_TABLE:
        if not text:
            _append_message("assistant", "Athena table cannot be empty.")
        else:
            st.session_state["athena_table"] = text
            st.session_state["wizard_step"] = STEP_ASK_CUR_WORKGROUP
            _append_message("assistant", "Enter Athena workgroup (or type 'primary').")
    elif st.session_state["wizard_step"] == STEP_ASK_CUR_WORKGROUP:
        st.session_state["athena_workgroup"] = text or "primary"
        st.session_state["wizard_step"] = STEP_ASK_CUR_OUTPUT_S3
        _append_message("assistant", "Enter Athena output S3 path (s3://bucket/prefix).")
    elif st.session_state["wizard_step"] == STEP_ASK_CUR_OUTPUT_S3:
        if not text.startswith("s3://"):
            _append_message("assistant", "Athena output must start with s3://")
        else:
            st.session_state["athena_output_s3"] = text
            st.session_state["wizard_step"] = STEP_ASK_CUR_PROFILE
            _append_message("assistant", "Enter CUR query profile name, or type 'same' to use current profile.")
    elif st.session_state["wizard_step"] == STEP_ASK_CUR_PROFILE:
        if lower in {"same", ""}:
            st.session_state["athena_profile_name"] = ""
        else:
            st.session_state["athena_profile_name"] = text
        st.session_state["wizard_step"] = STEP_ASK_CUR_REGION
        _append_message("assistant", "Enter CUR Athena region (default us-east-1).")
    elif st.session_state["wizard_step"] == STEP_ASK_CUR_REGION:
        st.session_state["athena_region"] = text or "us-east-1"
        st.session_state["wizard_step"] = STEP_ASK_EDP
        _append_message(
            "assistant",
            "CUR config saved (including profile/region). Enter your EDP percentage (0-100).",
        )
    elif st.session_state["wizard_step"] == STEP_ASK_EDP:
        edp = _validate_edp(text)
        if edp is None:
            _append_message("assistant", "Invalid EDP. Enter a number between 0 and 100.")
        else:
            st.session_state["edp_percent"] = edp
            st.session_state["wizard_step"] = STEP_RUN_SCAN
            _append_message("assistant", f"EDP set to {edp:.2f}%. Running AWS Config optimization scan...")
    else:
        _append_message("assistant", "Use 'rescan' to rerun, or select optimizations below for Excel export.")

if st.session_state["wizard_step"] == STEP_RUN_SCAN:
    with st.spinner("Running AWS Config optimization scan..."):
        _run_scan()
    st.rerun()

findings = st.session_state.get("scan_findings") or []
opportunities = [f for f in findings if f.optimization_id != "aws_config.inventory_summary"]
region_cost_rows = st.session_state.get("region_cost_rows") or []
if opportunities:
    st.markdown("### CUR Region Cost Inputs")
    cur_profile_label = st.session_state.get("cur_query_profile") or st.session_state.get("profile") or ""
    cur_region_label = st.session_state.get("cur_query_region") or st.session_state.get("athena_region") or "us-east-1"
    st.caption(f"CUR query profile: `{cur_profile_label}` | region: `{cur_region_label}`")
    if region_cost_rows:
        st.dataframe(pd.DataFrame(region_cost_rows), use_container_width=True)
    else:
        st.info("No CUR cost rows were returned for this scan.")

    st.markdown("### Optimization Opportunities")
    st.dataframe(pd.DataFrame(findings_to_rows(opportunities)), use_container_width=True)
    id_to_title = {}
    for f in opportunities:
        id_to_title.setdefault(f.optimization_id, f.title)
    optimization_ids = sorted(id_to_title.keys())
    selected_ids = st.multiselect(
        "Choose optimization(s) to include in Excel",
        options=optimization_ids,
        default=st.session_state.get("selected_optimization_ids", optimization_ids),
        format_func=lambda opt_id: f"{opt_id} | {id_to_title.get(opt_id, '')}",
    )
    st.session_state["selected_optimization_ids"] = selected_ids

    if st.button("Create Excel For Selected Optimizations"):
        if not selected_ids:
            st.warning("Select at least one optimization.")
        else:
            selected_findings = [f for f in opportunities if f.optimization_id in selected_ids]
            account_id = st.session_state.get("account_id")
            out_file = f"finops_report_{account_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
            out_path = write_excel(
                selected_findings,
                out_file,
                account_id=account_id,
                edp_percent=st.session_state.get("edp_percent"),
                region_cost_rows=st.session_state.get("region_cost_rows") or [],
                validation_sql=None,
            )
            st.session_state["latest_report_path"] = out_path
            st.session_state["latest_report_name"] = out_file
            st.session_state["wizard_step"] = STEP_DOWNLOAD
            _append_message(
                "assistant",
                f"Prepared Excel for {len(selected_findings)} findings across {len(selected_ids)} optimization types.",
            )

report_path = st.session_state.get("latest_report_path")
report_name = st.session_state.get("latest_report_name")
if report_path and report_name and Path(report_path).exists():
    with open(report_path, "rb") as f:
        st.download_button("Download Excel report", f, file_name=report_name)
