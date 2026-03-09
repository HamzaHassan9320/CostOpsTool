from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal

from app.core.registry import run_action
from app.core.types import ActionRequest, AgentRunResult, RunContext


@dataclass
class AccountExecutionTarget:
    account_id: str
    account_name: str
    role_name: str
    aws_session: Any


def run_scan_for_targets(
    *,
    action_id: str,
    profile_name: str,
    days: int,
    output: Literal["excel", "json", "markdown"],
    targets: list[AccountExecutionTarget],
    build_context_for_target: Callable[[AccountExecutionTarget], RunContext],
    run_action_fn: Callable[[ActionRequest, Callable[[ActionRequest], RunContext]], AgentRunResult] = run_action,
) -> AgentRunResult:
    if not targets:
        raise ValueError("No account targets were provided.")

    aggregated_recommendations = []
    aggregated_warnings: list[str] = []
    aggregated_cur_lines = []
    sql_used: str | None = None
    success_count = 0
    failures: list[dict[str, str]] = []
    account_runs: list[dict[str, Any]] = []
    first_success: AgentRunResult | None = None

    for target in targets:
        req = ActionRequest(
            action=action_id,
            profile_name=profile_name,
            days=days,
            regions=None,
            output=output,
        )
        try:
            result = run_action_fn(req, lambda _: build_context_for_target(target))
        except Exception as ex:
            failures.append(
                {
                    "account_id": target.account_id,
                    "account_name": target.account_name,
                    "role_name": target.role_name,
                    "error": str(ex),
                }
            )
            continue

        success_count += 1
        if first_success is None:
            first_success = result
        aggregated_recommendations.extend(result.recommendations)
        aggregated_warnings.extend(result.warnings)
        aggregated_cur_lines.extend(result.cur_cost_lines)
        if sql_used is None and result.sql_used:
            sql_used = result.sql_used
        account_runs.append(
            {
                "account_id": target.account_id,
                "account_name": target.account_name,
                "role_name": target.role_name,
                "recommendation_count": len(result.recommendations),
                "diagnostics": result.diagnostics,
                "warnings": result.warnings,
            }
        )

    diagnostics = {
        "multi_account": {
            "target_count": len(targets),
            "success_count": success_count,
            "failure_count": len(failures),
            "failures": failures,
            "accounts": [
                {
                    "account_id": target.account_id,
                    "account_name": target.account_name,
                    "role_name": target.role_name,
                }
                for target in targets
            ],
        },
        "per_account_runs": account_runs,
    }

    if success_count == 0:
        if len(failures) == 1:
            failed = failures[0]
            raise RuntimeError(
                "Account scan failed for "
                f"{failed.get('account_name') or failed.get('account_id')} "
                f"({failed.get('account_id')}): {failed.get('error')}"
            )
        sample = "; ".join(
            f"{item.get('account_id')}: {item.get('error')}"
            for item in failures[:3]
        )
        raise RuntimeError(
            "All account scans failed. "
            f"Sample errors: {sample}"
        )

    if failures:
        aggregated_warnings.append(
            f"{len(failures)} account scan(s) failed while {success_count} succeeded. Check diagnostics for details."
        )

    if len(targets) == 1 and first_success is not None and not failures:
        diagnostics["single_run_diagnostics"] = first_success.diagnostics

    return AgentRunResult(
        recommendations=aggregated_recommendations,
        diagnostics=diagnostics,
        sql_used=sql_used,
        cur_cost_lines=aggregated_cur_lines,
        warnings=aggregated_warnings,
    )
