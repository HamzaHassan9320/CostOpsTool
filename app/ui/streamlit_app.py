import streamlit as st
from datetime import datetime
from app.auth.validation import validate_profile
from app.core.types import ActionRequest, RunContext
from app.core.registry import run_action
from app.outputs.excel_writer import write_excel

# IMPORTANT: ensure plugins register
import app.services.aws_config.plugin  # noqa: F401

st.set_page_config(page_title="FinOps Copilot", layout="wide")
st.title("FinOps Copilot (MVP)")

with st.sidebar:
    st.header("AWS Connection (SSO)")
    profile = st.text_input("AWS profile name", value="")
    if st.button("Validate"):
        try:
            ident = validate_profile(profile)
            st.success(f"Connected: {ident['account']} | {ident['arn']}")
            st.session_state["account_id"] = ident["account"]
            st.session_state["profile"] = profile
        except Exception as e:
            st.error(str(e))

st.markdown("### Chat")
if "messages" not in st.session_state:
    st.session_state["messages"] = []

for m in st.session_state["messages"]:
    st.chat_message(m["role"]).write(m["content"])

user_text = st.chat_input("Ask: 'Check AWS Config savings for this account (30 days)'")
if user_text:
    st.session_state["messages"].append({"role": "user", "content": user_text})
    st.chat_message("user").write(user_text)

    # For MVP: skip LLM routing and just run AWS Config scan
    profile_name = st.session_state.get("profile") or ""
    account_id = st.session_state.get("account_id")

    if not profile_name:
        st.chat_message("assistant").write("Please validate an AWS SSO profile first in the sidebar.")
    else:
        req = ActionRequest(action="aws_config.savings_scan", profile_name=profile_name, days=30, regions=None, output="excel")

        def build_ctx(r: ActionRequest) -> RunContext:
            return RunContext(
                profile_name=r.profile_name,
                account_id=account_id,
                days=r.days,
                regions=r.regions or [],
                requested_by=None,
            )

        try:
            findings = run_action(req, build_ctx)
            st.chat_message("assistant").write(f"Found {len(findings)} items. Preparing report…")

            out_file = f"finops_report_{account_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
            path = write_excel(findings, out_file)

            with open(path, "rb") as f:
                st.download_button("Download Excel report", f, file_name=out_file)

            # show table inline
            import pandas as pd
            from app.outputs.report_builder import findings_to_rows
            st.dataframe(pd.DataFrame(findings_to_rows(findings)))

        except Exception as e:
            st.chat_message("assistant").write(f"Run failed: {e}")