from __future__ import annotations

from datetime import datetime
import json
import time
from typing import Any

from app.auth.session_factory import make_boto3_session
from app.core.types import AgentRunResult, RunContext
from app.llm.model import get_open_ai_model
from app.outputs.excel_writer import write_excel
from app.services.nat.costs.athena_nat_cur import (
    NatGatewayCurCostResult,
    get_last_full_month_nat_gateway_net_amortized_costs_by_ids,
)
from app.services.nat.optimization import (
    IdleNatCandidate,
    NatGatewayInfo,
    NatGatewayScanSummary,
    build_nat_recommendations,
    collect_nat_gateway_activity,
    identify_idle_nat_gateways,
    list_nat_gateways,
    list_regions,
)


class NatOptimizationToolset:
    def __init__(self, ctx: RunContext):
        self.ctx = ctx
        self.sess = make_boto3_session(ctx.profile_name)
        cur_profile = (ctx.athena_profile_name or "").strip() or ctx.profile_name
        self.cur_sess = make_boto3_session(cur_profile)

        self.regions: list[str] = []
        self.gateways: list[NatGatewayInfo] = []
        self.discovery_errors: list[str] = []
        self.activity_by_nat_id: dict[str, Any] = {}
        self.activity_errors: list[str] = []
        self.idle_candidates: list[IdleNatCandidate] = []
        self.scan_summary = NatGatewayScanSummary(0, 0, 0, 0, [])
        self.cur_result = NatGatewayCurCostResult(
            monthly_cost_by_nat_id={},
            eip_monthly_price_per_eip=None,
            lines=[],
            sql="",
            month_start=datetime.utcnow().date(),
            month_days=30,
            warning=None,
        )
        self.recommendations = []
        self.warnings: list[str] = []
        self.diagnostics: dict[str, Any] = {}
        self.step_durations_seconds: dict[str, float] = {}

    def _emit_progress(self, step: str, state: str, duration: float | None = None) -> None:
        callback = self.ctx.progress_callback
        if callback is None:
            return
        try:
            callback(step, state, duration)
        except Exception:
            pass

    def _start_step(self, step: str) -> float:
        self._emit_progress(step, "start", None)
        return time.perf_counter()

    def _finish_step(self, step: str, started: float) -> None:
        elapsed = max(0.0, time.perf_counter() - started)
        self.step_durations_seconds[step] = round(elapsed, 3)
        self._emit_progress(step, "done", elapsed)

    def resolve_context(self) -> dict[str, Any]:
        started = self._start_step("resolve_context")
        try:
            if not self.ctx.account_id:
                self.warnings.append("Account ID is missing from validated profile context.")
            cur_ready = bool(
                self.ctx.account_id
                and self.ctx.athena_database
                and self.ctx.athena_table
                and self.ctx.athena_workgroup
                and self.ctx.athena_output_s3
            )
            result = {
                "profile_name": self.ctx.profile_name,
                "account_id": self.ctx.account_id,
                "athena_region": self.ctx.athena_region,
                "cur_ready": cur_ready,
            }
            self.diagnostics["context"] = result
            return result
        finally:
            self._finish_step("resolve_context", started)

    def discover_nat_gateways(self) -> dict[str, Any]:
        started = self._start_step("discover_nat_gateways")
        try:
            self.regions = list_regions(self.sess)
            self.gateways, self.discovery_errors = list_nat_gateways(self.sess, self.regions)
            response = {
                "regions_scanned": len(self.regions),
                "nat_gateway_count": len(self.gateways),
                "discovery_error_count": len(self.discovery_errors),
            }
            self.diagnostics["discovery"] = response
            return response
        finally:
            self._finish_step("discover_nat_gateways", started)

    def collect_nat_activity(self) -> dict[str, Any]:
        started = self._start_step("collect_nat_activity")
        try:
            self.activity_by_nat_id, self.activity_errors = collect_nat_gateway_activity(self.sess, self.gateways)
            response = {
                "activity_gateway_count": len(self.activity_by_nat_id),
                "activity_error_count": len(self.activity_errors),
            }
            self.diagnostics["activity"] = response
            return response
        finally:
            self._finish_step("collect_nat_activity", started)

    def identify_idle_nat(self) -> dict[str, Any]:
        started = self._start_step("identify_idle_nat")
        try:
            self.idle_candidates, self.scan_summary = identify_idle_nat_gateways(
                gateways=self.gateways,
                activity_by_nat_id=self.activity_by_nat_id,
                activity_errors=self.discovery_errors + self.activity_errors,
            )
            response = {
                "idle_candidate_count": len(self.idle_candidates),
                "idle_6m_count": self.scan_summary.nat_gateway_idle_6m_count,
                "idle_2m_count": self.scan_summary.nat_gateway_idle_2m_count,
            }
            self.diagnostics["idle_detection"] = response
            return response
        finally:
            self._finish_step("identify_idle_nat", started)

    def query_nat_cur_net_amortized_by_ids(self) -> dict[str, Any]:
        started = self._start_step("query_nat_cur_net_amortized_by_ids")
        try:
            nat_ids = [c.gateway.nat_gateway_id for c in self.idle_candidates]
            if not nat_ids:
                self.cur_result = NatGatewayCurCostResult(
                    monthly_cost_by_nat_id={},
                    eip_monthly_price_per_eip=None,
                    lines=[],
                    sql="",
                    month_start=datetime.utcnow().date(),
                    month_days=30,
                    warning="No idle NAT gateways were found, so CUR lookup was skipped.",
                )
                self.warnings.append(self.cur_result.warning)
                response = {"cur_skipped": True, "reason": self.cur_result.warning}
                self.diagnostics["cur"] = response
                return response

            if not (
                self.ctx.account_id
                and self.ctx.athena_database
                and self.ctx.athena_table
                and self.ctx.athena_workgroup
                and self.ctx.athena_output_s3
            ):
                self.cur_result = NatGatewayCurCostResult(
                    monthly_cost_by_nat_id={},
                    eip_monthly_price_per_eip=None,
                    lines=[],
                    sql="",
                    month_start=datetime.utcnow().date(),
                    month_days=30,
                    warning="Athena CUR settings are incomplete; monthly costs were left empty.",
                )
                self.warnings.append(self.cur_result.warning)
                response = {"cur_skipped": True, "reason": self.cur_result.warning}
                self.diagnostics["cur"] = response
                return response

            self.cur_result = get_last_full_month_nat_gateway_net_amortized_costs_by_ids(
                sess=self.cur_sess,
                account_id=self.ctx.account_id,
                database=self.ctx.athena_database,
                table=self.ctx.athena_table,
                workgroup=self.ctx.athena_workgroup,
                output_s3=self.ctx.athena_output_s3,
                nat_gateway_ids=nat_ids,
                athena_region=self.ctx.athena_region,
            )
            if self.cur_result.warning:
                self.warnings.append(self.cur_result.warning)
            response = {
                "cur_line_count": len(self.cur_result.lines),
                "cur_gateway_count": len(self.cur_result.monthly_cost_by_nat_id),
                "eip_monthly_price_per_eip": self.cur_result.eip_monthly_price_per_eip,
                "warning": self.cur_result.warning,
            }
            self.diagnostics["cur"] = response
            return response
        finally:
            self._finish_step("query_nat_cur_net_amortized_by_ids", started)

    def build_nat_recommendations(self) -> dict[str, Any]:
        started = self._start_step("build_nat_recommendations")
        try:
            self.recommendations = build_nat_recommendations(
                account_id=self.ctx.account_id,
                candidates=self.idle_candidates,
                monthly_cost_by_nat_id=self.cur_result.monthly_cost_by_nat_id,
                eip_monthly_price_per_eip=self.cur_result.eip_monthly_price_per_eip,
            )
            response = {
                "recommendation_count": len(self.recommendations),
                "nat_scanned": self.scan_summary.nat_gateway_count_scanned,
                "nat_idle_6m_count": self.scan_summary.nat_gateway_idle_6m_count,
                "nat_idle_2m_count": self.scan_summary.nat_gateway_idle_2m_count,
                "nat_metric_error_count": self.scan_summary.nat_metric_error_count,
            }
            self.diagnostics["recommendations"] = response
            return response
        finally:
            self._finish_step("build_nat_recommendations", started)

    def export_recommendations_excel(self, out_path: str | None = None) -> dict[str, Any]:
        if not out_path:
            account = self.ctx.account_id or "unknown"
            out_path = f"finops_report_{account}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
        path = write_excel(
            recommendations=self.recommendations,
            out_path=out_path,
            account_id=self.ctx.account_id,
            sql_used=self.cur_result.sql,
            cur_cost_lines=self.cur_result.lines,
            warnings=self.warnings,
            diagnostics=self.diagnostics,
        )
        return {"out_path": path}

    def run_remaining_deterministic(self) -> None:
        if "context" not in self.diagnostics:
            self.resolve_context()
        if "discovery" not in self.diagnostics:
            self.discover_nat_gateways()
        if "activity" not in self.diagnostics:
            self.collect_nat_activity()
        if "idle_detection" not in self.diagnostics:
            self.identify_idle_nat()
        if "cur" not in self.diagnostics:
            self.query_nat_cur_net_amortized_by_ids()
        if "recommendations" not in self.diagnostics:
            self.build_nat_recommendations()

    def to_result(self) -> AgentRunResult:
        diagnostics = {
            "nat_gateway_count_scanned": self.scan_summary.nat_gateway_count_scanned,
            "nat_gateway_idle_6m_count": self.scan_summary.nat_gateway_idle_6m_count,
            "nat_gateway_idle_2m_count": self.scan_summary.nat_gateway_idle_2m_count,
            "nat_metric_error_count": self.scan_summary.nat_metric_error_count,
            "nat_metric_error_samples": self.scan_summary.nat_metric_error_samples,
            "discovery_errors": self.discovery_errors[:5],
            "step_durations_seconds": self.step_durations_seconds,
            "tool_diagnostics": self.diagnostics,
        }
        return AgentRunResult(
            recommendations=self.recommendations,
            diagnostics=diagnostics,
            sql_used=self.cur_result.sql or None,
            cur_cost_lines=self.cur_result.lines,
            warnings=[w for w in self.warnings if w],
        )


def _tools_schema() -> list[dict[str, Any]]:
    return [
        {
            "type": "function",
            "function": {
                "name": "resolve_context",
                "description": "Load account/profile/Athena context before scanning.",
                "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "discover_nat_gateways",
                "description": "Discover NAT gateways across enabled regions.",
                "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "collect_nat_activity",
                "description": "Collect CloudWatch NAT activity metrics for discovered gateways.",
                "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "identify_idle_nat",
                "description": "Identify idle NAT gateways from collected activity.",
                "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "query_nat_cur_net_amortized_by_ids",
                "description": "Query last full month NAT net amortized CUR costs for idle NAT IDs.",
                "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "build_nat_recommendations",
                "description": "Build recommendation rows from idle NAT findings and CUR costs.",
                "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "export_recommendations_excel",
                "description": "Export recommendation rows to Excel. Optional out_path.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "out_path": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
            },
        },
    ]


def _invoke_tool(toolset: NatOptimizationToolset, name: str, args: dict[str, Any]) -> dict[str, Any]:
    if name == "resolve_context":
        return toolset.resolve_context()
    if name == "discover_nat_gateways":
        return toolset.discover_nat_gateways()
    if name == "collect_nat_activity":
        return toolset.collect_nat_activity()
    if name == "identify_idle_nat":
        return toolset.identify_idle_nat()
    if name == "query_nat_cur_net_amortized_by_ids":
        return toolset.query_nat_cur_net_amortized_by_ids()
    if name == "build_nat_recommendations":
        return toolset.build_nat_recommendations()
    if name == "export_recommendations_excel":
        return toolset.export_recommendations_excel(out_path=args.get("out_path"))
    return {"error": f"Unknown tool: {name}"}


def _run_with_llm_tools(toolset: NatOptimizationToolset) -> tuple[bool, str | None]:
    try:
        client, config = get_open_ai_model()
    except Exception as ex:
        return False, f"LLM tool-calling unavailable: {ex}"

    messages: list[dict[str, Any]] = [
        {
            "role": "system",
            "content": (
                "You orchestrate a NAT idle optimization scan using tools. "
                "Use tools in logical order: resolve_context, discover_nat_gateways, collect_nat_activity, "
                "identify_idle_nat, query_nat_cur_net_amortized_by_ids, build_nat_recommendations. "
                "Do not fabricate results."
            ),
        },
        {
            "role": "user",
            "content": "Run the NAT optimization scan now and prepare recommendation rows.",
        },
    ]

    tools = _tools_schema()
    used_any_tool = False

    try:
        for _ in range(10):
            resp = client.chat.completions.create(
                model=config.model,
                temperature=0,
                messages=messages,
                tools=tools,
                tool_choice="auto",
            )
            msg = resp.choices[0].message

            assistant_payload: dict[str, Any] = {"role": "assistant"}
            if msg.content is not None:
                assistant_payload["content"] = msg.content
            if msg.tool_calls:
                assistant_payload["tool_calls"] = [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        },
                    }
                    for tc in msg.tool_calls
                ]
            messages.append(assistant_payload)

            if not msg.tool_calls:
                break

            used_any_tool = True
            for tc in msg.tool_calls:
                tool_name = tc.function.name
                try:
                    args = json.loads(tc.function.arguments or "{}")
                except Exception:
                    args = {}
                output = _invoke_tool(toolset, tool_name, args)
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": json.dumps(output),
                    }
                )
    except Exception as ex:
        return used_any_tool, f"LLM tool-calling failed: {ex}"

    return used_any_tool, None


def run_nat_optimization_agent(ctx: RunContext) -> AgentRunResult:
    toolset = NatOptimizationToolset(ctx)
    llm_ran, llm_error = _run_with_llm_tools(toolset)

    # Ensure completion even when the model is unavailable or stops early.
    if "recommendations" not in toolset.diagnostics:
        toolset.run_remaining_deterministic()

    if llm_error:
        if "recommendations" in toolset.diagnostics:
            toolset.diagnostics["llm_tool_error_recovered"] = llm_error
        else:
            toolset.warnings.append(llm_error)
    elif llm_ran and "recommendations" not in toolset.diagnostics:
        toolset.warnings.append("LLM run was incomplete and deterministic fallback also failed.")

    return toolset.to_result()
