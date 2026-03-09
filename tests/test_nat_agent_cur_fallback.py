from __future__ import annotations

from app.agent import nat_agent as mod
from app.core.types import RunContext
from app.services.nat.optimization import IdleNatCandidate, NatGatewayActivity, NatGatewayInfo


def _idle_candidate() -> IdleNatCandidate:
    gateway = NatGatewayInfo(
        nat_gateway_id="nat-abc123",
        gateway_name="egress",
        region="eu-west-1",
        state="available",
        connectivity_type="public",
        vpc_id="vpc-1",
        subnet_id="subnet-1",
        public_ips=[],
        allocation_ids=[],
    )
    activity = NatGatewayActivity(
        bytes_out_to_destination_sum_6m=0.0,
        bytes_out_to_source_sum_6m=0.0,
        active_connection_max_6m=0.0,
        bytes_out_to_destination_sum_2m=0.0,
        bytes_out_to_source_sum_2m=0.0,
        active_connection_max_2m=0.0,
        datapoint_count_6m=1,
        datapoint_count_2m=1,
    )
    return IdleNatCandidate(gateway=gateway, activity=activity, lookback_duration="6 months")


def test_cur_query_failure_is_fatal(monkeypatch):
    monkeypatch.setattr(mod, "make_boto3_session", lambda profile_name: object())
    monkeypatch.setattr(
        mod,
        "get_last_full_month_nat_gateway_net_amortized_costs_by_ids",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("Error loading SSO Token: token missing")),
    )

    toolset = mod.NatOptimizationToolset(
        RunContext(
            profile_name="demo",
            account_id="123456789012",
            days=30,
            regions=[],
            athena_database="cost",
            athena_table="cudos01",
            athena_workgroup="primary",
            athena_output_s3="s3://bucket/prefix",
            athena_profile_name="demo",
            athena_region="us-east-1",
            requested_by=None,
        )
    )
    toolset.idle_candidates = [_idle_candidate()]

    try:
        toolset.query_nat_cur_net_amortized_by_ids()
        assert False, "Expected CUR query failure to raise"
    except RuntimeError as ex:
        assert "CUR pricing query failed" in str(ex)


def test_toolset_uses_injected_cur_session(monkeypatch):
    scan_session = object()
    cur_session = object()
    calls: list[str] = []

    def fake_make_session(profile_name: str):
        calls.append(profile_name)
        return scan_session

    monkeypatch.setattr(mod, "make_boto3_session", fake_make_session)

    toolset = mod.NatOptimizationToolset(
        RunContext(
            profile_name="demo",
            account_id="123456789012",
            days=30,
            regions=[],
            athena_database="cost",
            athena_table="cudos01",
            athena_workgroup="primary",
            athena_output_s3="s3://bucket/prefix",
            athena_profile_name=None,
            athena_region="us-east-1",
            requested_by=None,
            cur_session=cur_session,
        )
    )
    assert toolset.sess is scan_session
    assert toolset.cur_sess is cur_session
    assert calls == ["demo"]
