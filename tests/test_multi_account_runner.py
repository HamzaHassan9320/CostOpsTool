from __future__ import annotations

from app.agent.multi_account import AccountExecutionTarget, run_scan_for_targets
from app.core.types import AgentRunResult, NatRecommendationRow, RunContext


def _ctx_for_target(target: AccountExecutionTarget) -> RunContext:
    return RunContext(
        profile_name="dev-sso",
        account_id=target.account_id,
        days=30,
        regions=[],
        athena_database="cost",
        athena_table="cudos01",
        athena_workgroup="primary",
        athena_output_s3="s3://bucket/prefix",
        athena_profile_name="payer",
        athena_region="us-east-1",
        requested_by=None,
        aws_session=target.aws_session,
        account_name=target.account_name,
        role_name=target.role_name,
    )


def test_run_scan_for_targets_uses_injected_session():
    session_obj = object()
    targets = [
        AccountExecutionTarget(
            account_id="111111111111",
            account_name="Dev",
            role_name="ReadOnly",
            aws_session=session_obj,
        )
    ]
    seen_sessions = []

    def fake_run_action(req, build_ctx):
        ctx = build_ctx(req)
        seen_sessions.append(ctx.aws_session)
        return AgentRunResult(
            recommendations=[
                NatRecommendationRow(
                    account_id=ctx.account_id,
                    region="eu-west-1",
                    gateway_name="gw",
                    gateway_id="nat-1",
                    lookback_duration="6 months",
                    bytes_out_to_destination=0.0,
                    bytes_out_to_source=0.0,
                    active_connections=0.0,
                    monthly_cost=10.0,
                )
            ],
            diagnostics={"ok": True},
            sql_used="select 1",
            cur_cost_lines=[],
            warnings=[],
        )

    result = run_scan_for_targets(
        action_id="optimization.run_scan",
        profile_name="dev-sso",
        days=30,
        output="excel",
        targets=targets,
        build_context_for_target=_ctx_for_target,
        run_action_fn=fake_run_action,
    )

    assert seen_sessions == [session_obj]
    assert len(result.recommendations) == 1
    assert result.recommendations[0].account_id == "111111111111"


def test_run_scan_for_targets_continues_when_one_account_fails():
    targets = [
        AccountExecutionTarget(account_id="111111111111", account_name="Dev", role_name="ReadOnly", aws_session=object()),
        AccountExecutionTarget(account_id="222222222222", account_name="Prod", role_name="ReadOnly", aws_session=object()),
    ]

    def fake_run_action(req, build_ctx):
        ctx = build_ctx(req)
        if ctx.account_id == "222222222222":
            raise RuntimeError("AccessDenied")
        return AgentRunResult(
            recommendations=[],
            diagnostics={"account": ctx.account_id},
            sql_used=None,
            cur_cost_lines=[],
            warnings=[],
        )

    result = run_scan_for_targets(
        action_id="optimization.run_scan",
        profile_name="dev-sso",
        days=30,
        output="excel",
        targets=targets,
        build_context_for_target=_ctx_for_target,
        run_action_fn=fake_run_action,
    )

    diagnostics = result.diagnostics.get("multi_account") or {}
    assert diagnostics.get("target_count") == 2
    assert diagnostics.get("success_count") == 1
    assert diagnostics.get("failure_count") == 1
    assert result.warnings


def test_run_scan_for_targets_single_failure_includes_account_error():
    targets = [
        AccountExecutionTarget(account_id="546377338878", account_name="sc-awslogging1", role_name="ReadOnly", aws_session=object()),
    ]

    def fake_run_action(req, build_ctx):
        _ = build_ctx(req)
        raise RuntimeError("AccessDenied: not authorized to perform ec2:DescribeRegions")

    try:
        run_scan_for_targets(
            action_id="optimization.run_scan",
            profile_name="dev-sso",
            days=30,
            output="excel",
            targets=targets,
            build_context_for_target=_ctx_for_target,
            run_action_fn=fake_run_action,
        )
        assert False, "Expected run_scan_for_targets to raise"
    except RuntimeError as ex:
        text = str(ex)
        assert "546377338878" in text
        assert "AccessDenied" in text
