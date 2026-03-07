from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Callable

import pandas as pd
import streamlit as st

# Ensure `app.*` imports work even when Streamlit sets CWD/script path differently.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.auth.validation import validate_profile
from app.core.registry import run_action
from app.core.types import ActionRequest, RunContext
from app.llm.router import route
from app.memory.store import ProjectMemory, get_project, list_projects, upsert_project
from app.outputs.excel_writer import write_excel
from app.outputs.report_builder import recommendations_to_rows

# IMPORTANT: ensure plugin registers
import app.services.nat.plugin  # noqa: F401

ACTION_ID = "optimization.run_scan"

STAGE_AWAIT_INTENT = "await_intent"
STAGE_AWAIT_PROFILE = "await_profile"
STAGE_VALIDATING_PROFILE = "validating_profile"
STAGE_AWAIT_PROJECT_SELECTION = "await_project_selection"
STAGE_AWAIT_CUR_FIELDS = "await_cur_fields"
STAGE_RUNNING_SCAN = "running_scan"
STAGE_READY_WITH_RESULTS = "ready_with_results"

CUR_FIELDS_ALL = [
    "athena_database",
    "athena_table",
    "athena_workgroup",
    "athena_output_s3",
    "athena_profile_name",
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
    "athena_database": "Enter Athena CUR database name (or type 'skip' to skip CUR pricing).",
    "athena_table": "Enter Athena CUR table name.",
    "athena_workgroup": "Enter Athena workgroup (default: primary).",
    "athena_output_s3": "Enter Athena output S3 path (s3://bucket/prefix).",
    "athena_profile_name": (
        "Enter CUR query profile name for the account that has Athena/CUR "
        "(often management/payer), or type 'same' for the validated profile."
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
        "project_name": "",
        "project_options": [],
        "athena_database": os.getenv("ATHENA_DATABASE", ""),
        "athena_table": os.getenv("ATHENA_TABLE", ""),
        "athena_workgroup": os.getenv("ATHENA_WORKGROUP", "primary"),
        "athena_output_s3": os.getenv("ATHENA_OUTPUT_S3", ""),
        "athena_profile_name": os.getenv("ATHENA_PROFILE_NAME", ""),
        "athena_region": os.getenv("ATHENA_REGION", "us-east-1"),
        "cur_skipped": False,
        "cur_fields_queue": [],
        "cur_edit_mode": False,
        "recommendations": [],
        "run_diagnostics": {},
        "run_sql": None,
        "run_warnings": [],
        "run_cur_cost_lines": [],
        "latest_report_path": None,
        "latest_report_name": None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

    if not st.session_state["messages"]:
        _assistant(
            "Ask me to analyze NAT gateway optimization for an account. Example: "
            "`analyze idle nat gateway savings for this account with profile my-sso-profile`.\n\n"
            "Commands: `/analyze profile=<name>`, `/project <name>`, `/athena edit`, `/rescan`, `/help`."
        )


def _reset_scan_outputs() -> None:
    st.session_state["recommendations"] = []
    st.session_state["run_diagnostics"] = {}
    st.session_state["run_sql"] = None
    st.session_state["run_warnings"] = []
    st.session_state["run_cur_cost_lines"] = []
    st.session_state["latest_report_path"] = None
    st.session_state["latest_report_name"] = None


def _reset_project_settings() -> None:
    st.session_state["athena_database"] = os.getenv("ATHENA_DATABASE", "")
    st.session_state["athena_table"] = os.getenv("ATHENA_TABLE", "")
    st.session_state["athena_workgroup"] = os.getenv("ATHENA_WORKGROUP", "primary")
    st.session_state["athena_output_s3"] = os.getenv("ATHENA_OUTPUT_S3", "")
    st.session_state["athena_profile_name"] = os.getenv("ATHENA_PROFILE_NAME", "")
    st.session_state["athena_region"] = os.getenv("ATHENA_REGION", "us-east-1")
    st.session_state["cur_skipped"] = False


def _missing_cur_fields() -> list[str]:
    missing = []
    for field in CUR_FIELDS_REQUIRED:
        if not str(st.session_state.get(field, "")).strip():
            missing.append(field)
    return missing


def _needs_cur_questions() -> bool:
    if st.session_state.get("cur_skipped"):
        return False
    return bool(_missing_cur_fields())


def _build_context(
    account_id: str | None,
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
        cur_skipped=bool(st.session_state.get("cur_skipped")),
        athena_database=st.session_state.get("athena_database", "").strip(),
        athena_table=st.session_state.get("athena_table", "").strip(),
        athena_workgroup=st.session_state.get("athena_workgroup", "primary").strip() or "primary",
        athena_output_s3=st.session_state.get("athena_output_s3", "").strip(),
        athena_profile_name=st.session_state.get("athena_profile_name", "").strip(),
        athena_region=st.session_state.get("athena_region", "us-east-1").strip() or "us-east-1",
        created_at=existing.created_at if existing else "",
        last_used_at="",
    )
    upsert_project(memory)


def _run_scan(progress_writer: Callable[[str], None] | None = None) -> None:
    account_id = st.session_state.get("account_id")
    profile_name = st.session_state.get("profile") or ""
    if not profile_name:
        _assistant("Please start with an analysis request and validate an AWS profile in chat.")
        st.session_state["stage"] = STAGE_AWAIT_PROFILE
        return

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

    req = ActionRequest(action=ACTION_ID, profile_name=profile_name, days=30, regions=None, output="excel")
    try:
        result = run_action(req, lambda _: _build_context(account_id, progress_callback=_progress_callback))
    except Exception as ex:
        _assistant(f"Run failed: {ex}")
        st.session_state["stage"] = STAGE_AWAIT_INTENT
        return

    st.session_state["recommendations"] = result.recommendations
    st.session_state["run_diagnostics"] = result.diagnostics
    st.session_state["run_sql"] = result.sql_used
    st.session_state["run_warnings"] = result.warnings
    st.session_state["run_cur_cost_lines"] = result.cur_cost_lines

    _assistant(
        "Scan complete for project "
        f"`{st.session_state.get('project_name') or 'unspecified'}`. "
        f"Found {len(result.recommendations)} NAT recommendation(s)."
    )

    _persist_project_memory()
    st.session_state["stage"] = STAGE_READY_WITH_RESULTS


def _start_cur_collection(edit_mode: bool) -> None:
    if edit_mode:
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
        _persist_project_memory()
        _assistant("CUR config saved. Running optimization scan...")
        st.session_state["stage"] = STAGE_RUNNING_SCAN
        return

    field = queue[0]
    prompt = CUR_FIELD_PROMPTS[field]
    if st.session_state.get("cur_edit_mode"):
        current = str(st.session_state.get(field, "")).strip()
        if field == "athena_profile_name" and not current:
            current = "same as validated profile"
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
        st.session_state["athena_region"] = existing.athena_region or "us-east-1"
        st.session_state["cur_skipped"] = bool(existing.cur_skipped)
        _assistant(f"Loaded saved project `{existing.project_name}`.")
    else:
        st.session_state["project_name"] = clean_name
        _reset_project_settings()
        _assistant(f"Created new project `{clean_name}`.")

    _persist_project_memory()

    if _needs_cur_questions():
        _start_cur_collection(edit_mode=False)
        return

    _assistant("Running optimization scan...")
    st.session_state["stage"] = STAGE_RUNNING_SCAN


def _validate_and_set_profile(profile_name: str) -> None:
    clean = profile_name.strip()
    if not clean:
        st.session_state["stage"] = STAGE_AWAIT_PROFILE
        _assistant("Profile name cannot be empty. Enter the AWS SSO profile name to continue.")
        return

    st.session_state["stage"] = STAGE_VALIDATING_PROFILE
    try:
        ident = validate_profile(clean)
    except Exception as ex:
        st.session_state["stage"] = STAGE_AWAIT_PROFILE
        _assistant(f"Profile validation failed: {ex}")
        return

    st.session_state["profile"] = clean
    st.session_state["account_id"] = ident["account"]
    st.session_state["project_name"] = ""
    _reset_scan_outputs()

    _assistant(
        f"Profile validated. Account `{ident['account']}` | ARN `{ident['arn']}`.\n"
        "Now select an existing project or provide a new project name."
    )
    _prompt_project_selection()


def _handle_rescan() -> None:
    if not st.session_state.get("profile"):
        st.session_state["stage"] = STAGE_AWAIT_PROFILE
        _assistant("Start with an analysis request and profile validation first.")
        return
    if not st.session_state.get("project_name"):
        _prompt_project_selection()
        return
    if _needs_cur_questions():
        _start_cur_collection(edit_mode=False)
        return

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
    if value.lower() == "skip" and not edit_mode:
        st.session_state["cur_skipped"] = True
        st.session_state["athena_database"] = ""
        st.session_state["athena_table"] = ""
        st.session_state["athena_workgroup"] = ""
        st.session_state["athena_output_s3"] = ""
        st.session_state["athena_profile_name"] = ""
        st.session_state["athena_region"] = "us-east-1"
        st.session_state["cur_fields_queue"] = []
        st.session_state["cur_edit_mode"] = False
        _assistant("CUR input skipped for this project. Monthly costs may be empty.")
        _persist_project_memory()
        _assistant("Running optimization scan...")
        st.session_state["stage"] = STAGE_RUNNING_SCAN
        return

    if edit_mode and value == "-":
        st.session_state["cur_fields_queue"] = queue[1:]
        _ask_next_cur_prompt()
        return

    if field in {"athena_profile_name", "athena_region"} and value == "-":
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
        if value.lower() in {"", "same"}:
            cur_profile = st.session_state.get("profile", "").strip()
        else:
            cur_profile = value
        try:
            validate_profile(cur_profile)
        except Exception as ex:
            _assistant(
                f"CUR query profile `{cur_profile}` failed validation: {ex}. "
                f"Run `aws sso login --profile {cur_profile}` then re-enter this value."
            )
            return
        st.session_state[field] = cur_profile
    elif field == "athena_region":
        st.session_state[field] = value or "us-east-1"

    st.session_state["cur_fields_queue"] = queue[1:]
    _ask_next_cur_prompt()


def _help_text() -> str:
    return (
        "Available commands:\n"
        "- `/analyze profile=<aws-profile>`\n"
        "- `/project <project-name>`\n"
        "- `/athena edit`\n"
        "- `/rescan`\n"
        "- `/help`\n\n"
        "Natural chat also works, for example: "
        "`analyze idle nat gateway savings for this account with profile finops-sso`."
    )


def _handle_analyze_intent(profile_name: str | None) -> None:
    if profile_name:
        _validate_and_set_profile(profile_name)
        return

    if st.session_state.get("profile"):
        _assistant(f"Using validated profile `{st.session_state['profile']}`. Select a project for this analysis.")
        _prompt_project_selection()
        return

    st.session_state["stage"] = STAGE_AWAIT_PROFILE
    _assistant("Which AWS SSO profile should I use for this analysis?")


def handle_user_input(user_text: str) -> None:
    _append_message("user", user_text)
    text = user_text.strip()
    if not text:
        _assistant("Enter a message to continue.")
        return

    stage = st.session_state.get("stage", STAGE_AWAIT_INTENT)
    if not text.startswith("/"):
        if stage == STAGE_AWAIT_PROFILE:
            _validate_and_set_profile(text)
            return
        if stage == STAGE_AWAIT_PROJECT_SELECTION:
            _select_project(_parse_project_selection(text))
            return
        if stage == STAGE_AWAIT_CUR_FIELDS:
            _handle_cur_field_input(text)
            return

    intent = route(text)

    if intent.intent == "help":
        _assistant(_help_text())
        return
    if intent.intent == "rescan":
        _handle_rescan()
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
        _handle_analyze_intent(intent.profile_name)
        return

    _assistant("I can help run optimization scans. Ask to analyze an account with a profile, or use `/help`.")


def _chat_placeholder() -> str:
    stage = st.session_state.get("stage", STAGE_AWAIT_INTENT)
    if stage == STAGE_AWAIT_PROFILE:
        return "Enter AWS SSO profile name..."
    if stage == STAGE_AWAIT_PROJECT_SELECTION:
        return "Type project number or project name..."
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
    account = _status_value(st.session_state.get("account_id"), "Unknown")

    if st.session_state.get("cur_skipped"):
        cur_status = "Skipped"
    elif _needs_cur_questions():
        cur_status = "Needs Athena Inputs"
    else:
        cur_status = "Ready"

    st.markdown(
        f"""
        <div class="status-grid">
            <div class="status-card"><div class="status-label">Project</div><div class="status-value">{project}</div></div>
            <div class="status-card"><div class="status-label">AWS Profile</div><div class="status-value">{profile}</div></div>
            <div class="status-card"><div class="status-label">Account</div><div class="status-value">{account}</div></div>
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
      <p class="title-sub">Chat-first optimization analysis with reusable project memory</p>
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
        if st.session_state.get("stage") == STAGE_READY_WITH_RESULTS:
            status.update(label="Optimization scan complete", state="complete")
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
        account_id = st.session_state.get("account_id")
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
